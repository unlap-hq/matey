from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Annotated

import typer

from matey.cli.common import (
    build_execution_context,
    read_schema_sql,
    resolve_lock_engine_for_sync,
    resolve_target_execution,
)
from matey.cli.help import command_help
from matey.cli.options import get_options
from matey.cli.output import OutputOptions, RichDbmateRenderer
from matey.core import (
    ConfigError,
    LockfileError,
    PathResolutionError,
    ResolvedPaths,
    SchemaValidationError,
    TargetSelectionError,
    URLResolutionError,
)
from matey.core.lock import SchemaLock, build_schema_lock, lockfile_path, write_schema_lock
from matey.core.replay import (
    SchemaEvaluationResult,
    SchemaReplayResult,
    collect_replay_checkpoint_updates,
    evaluate_schema_lock_target,
    evaluate_schema_lock_target_with_down,
)


def _write_temp_text(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return tmp_path


def _write_temp_lock(path: Path, lock: SchemaLock) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        write_schema_lock(tmp_path, lock)
        with tmp_path.open("rb") as handle:
            os.fsync(handle.fileno())
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return tmp_path


def _replace_file(path: Path, content: str) -> None:
    tmp_path = _write_temp_text(path, content)
    tmp_path.replace(path)
    _fsync_parent(path)


def _fsync_parent(path: Path) -> None:
    dir_fd = os.open(str(path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def _restore_previous_file(path: Path, previous: str | None) -> None:
    if previous is None:
        path.unlink(missing_ok=True)
        return
    _replace_file(path, previous)


def _resolve_checkpoint_path(*, paths: ResolvedPaths, relative_path: str) -> Path:
    candidate_rel = Path(relative_path)
    if candidate_rel.is_absolute():
        raise LockfileError(f"Checkpoint path must be relative: {relative_path}")
    db_root = paths.db_dir.resolve()
    candidate = (paths.db_dir / candidate_rel).resolve()
    if not candidate.is_relative_to(db_root):
        raise LockfileError(f"Checkpoint path escapes target db dir: {relative_path}")
    return candidate


def _atomic_write_schema_and_lock(
    *,
    paths: ResolvedPaths,
    schema_sql: str,
    checkpoint_updates: dict[str, str],
    real_url: str | None,
    test_url: str | None,
    target_name: str,
) -> tuple[bool, SchemaLock]:
    lock_path = lockfile_path(paths)
    previous_lock = lock_path.read_text(encoding="utf-8") if lock_path.exists() else None

    target_updates: dict[Path, str] = {paths.schema_file: schema_sql}
    for checkpoint_rel, checkpoint_sql in checkpoint_updates.items():
        checkpoint_path = _resolve_checkpoint_path(paths=paths, relative_path=checkpoint_rel)
        target_updates[checkpoint_path] = checkpoint_sql

    previous_contents: dict[Path, str | None] = {
        path: (read_schema_sql(path) if path.exists() else None)
        for path in target_updates
    }
    changed = any(previous_contents[path] != content for path, content in target_updates.items())

    replaced: list[Path] = []
    lock_tmp: Path | None = None
    try:
        for path, content in target_updates.items():
            _replace_file(path, content)
            replaced.append(path)

        engine = resolve_lock_engine_for_sync(
            paths=paths,
            real_url=real_url,
            test_url=test_url,
        )
        lock = build_schema_lock(
            paths=paths,
            engine=engine,
            target=target_name,
            schema_sql_override=schema_sql,
        )
        lock_tmp = _write_temp_lock(lock_path, lock)
        lock_tmp.replace(lock_path)
        _fsync_parent(lock_path)

    except Exception:
        if lock_tmp is not None:
            lock_tmp.unlink(missing_ok=True)
        for path in reversed(replaced):
            _restore_previous_file(path, previous_contents[path])
        _restore_previous_file(lock_path, previous_lock)
        raise

    lock_changed = previous_lock != lock_path.read_text(encoding="utf-8")
    return changed or lock_changed, lock


def _run_schema_replay(
    *,
    target_name: str,
    dbmate_binary,
    paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    clean: bool,
    renderer: RichDbmateRenderer,
) -> SchemaReplayResult:
    return evaluate_schema_lock_target(
        target_name=target_name,
        dbmate_binary=dbmate_binary,
        paths=paths,
        test_url=test_url,
        keep_scratch=keep_scratch,
        base_branch=base_branch,
        clean=clean,
        on_dbmate_result=renderer.handle,
    )


def _run_schema_evaluation(
    *,
    target_name: str,
    dbmate_binary,
    paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    clean: bool,
    renderer: RichDbmateRenderer,
) -> SchemaEvaluationResult:
    return evaluate_schema_lock_target_with_down(
        target_name=target_name,
        dbmate_binary=dbmate_binary,
        paths=paths,
        test_url=test_url,
        keep_scratch=keep_scratch,
        base_branch=base_branch,
        clean=clean,
        on_dbmate_result=renderer.handle,
    )


def _emit_schema_evaluation_output(
    *,
    target_name: str,
    evaluation: SchemaEvaluationResult,
    keep_scratch: bool,
    require_replay_match: bool,
) -> bool:
    replay = evaluation.replay
    down = evaluation.down

    if replay.diff_text:
        typer.echo(f"=== schema replay diff [target={target_name}] ===")
        typer.echo(replay.diff_text.rstrip())
    if replay.error:
        typer.secho(f"[matey] target={target_name}: {replay.error}", fg="red")
    if keep_scratch and replay.scratch_url:
        typer.echo(f"[matey] target={target_name}: keeping scratch at {replay.scratch_url}")

    if down.diff_text:
        typer.echo(f"=== down round-trip diff [target={target_name}] ===")
        typer.echo(down.diff_text.rstrip())
    if down.error:
        typer.secho(f"[matey] target={target_name}: {down.error}", fg="red")
    if keep_scratch and down.scratch_url:
        typer.echo(f"[matey] target={target_name}: keeping scratch at {down.scratch_url}")

    if replay.error or down.error or not down.success:
        return False
    return not (require_replay_match and not replay.success)


def register(schema_app: typer.Typer) -> None:
    @schema_app.command("validate", help=command_help("schema", "validate"))
    def schema_validate(
        ctx: typer.Context,
        clean: Annotated[
            bool, typer.Option("--clean", help="Force full replay from empty scratch.")
        ] = False,
    ) -> None:
        options = get_options(ctx)
        renderer = RichDbmateRenderer(
            options=OutputOptions(verbose=options.verbose, quiet=options.quiet),
        )

        try:
            context = build_execution_context(options)
        except (ConfigError, TargetSelectionError) as error:
            raise typer.BadParameter(str(error)) from error

        failures = 0
        for selected_target in context.selected_targets:
            try:
                paths, _real_url, test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=selected_target,
                    require_real_url=False,
                )
                evaluation = _run_schema_evaluation(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    test_url=test_url,
                    keep_scratch=options.keep_scratch,
                    base_branch=options.base_branch,
                    clean=clean,
                    renderer=renderer,
                )
            except (
                PathResolutionError,
                URLResolutionError,
                ConfigError,
                SchemaValidationError,
                LockfileError,
            ) as error:
                typer.secho(f"[matey] target={selected_target.name} validation error: {error}", fg="red")
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name} unexpected validation error: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if not _emit_schema_evaluation_output(
                target_name=selected_target.name,
                evaluation=evaluation,
                keep_scratch=options.keep_scratch,
                require_replay_match=True,
            ):
                failures += 1
                continue

            if not options.quiet:
                typer.secho(f"[matey] target={selected_target.name}: schema validation passed.", fg="green")

        if failures:
            raise typer.Exit(1)

    @schema_app.command("regen", help=command_help("schema", "regen"))
    def schema_regen(
        ctx: typer.Context,
        clean: Annotated[
            bool, typer.Option("--clean", help="Force full replay from empty scratch.")
        ] = False,
    ) -> None:
        options = get_options(ctx)
        renderer = RichDbmateRenderer(
            options=OutputOptions(verbose=options.verbose, quiet=options.quiet),
        )

        try:
            context = build_execution_context(options)
        except (ConfigError, TargetSelectionError) as error:
            raise typer.BadParameter(str(error)) from error

        failures = 0
        for selected_target in context.selected_targets:
            try:
                paths, real_url, test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=selected_target,
                    require_real_url=False,
                )
                evaluation = _run_schema_evaluation(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    test_url=test_url,
                    keep_scratch=options.keep_scratch,
                    base_branch=options.base_branch,
                    clean=clean,
                    renderer=renderer,
                )
            except (
                PathResolutionError,
                URLResolutionError,
                ConfigError,
                SchemaValidationError,
                LockfileError,
            ) as error:
                typer.secho(f"[matey] target={selected_target.name} regen error: {error}", fg="red")
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name} unexpected regen error: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if not _emit_schema_evaluation_output(
                target_name=selected_target.name,
                evaluation=evaluation,
                keep_scratch=options.keep_scratch,
                require_replay_match=False,
            ):
                failures += 1
                continue

            try:
                checkpoint_updates = collect_replay_checkpoint_updates(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    test_url=test_url,
                    keep_scratch=options.keep_scratch,
                    base_branch=options.base_branch,
                    clean=clean,
                    on_dbmate_result=renderer.handle,
                )
                changed, lock = _atomic_write_schema_and_lock(
                    paths=paths,
                    schema_sql=evaluation.replay.expected_schema_sql,
                    checkpoint_updates=checkpoint_updates,
                    real_url=real_url,
                    test_url=test_url,
                    target_name=selected_target.name,
                )
            except LockfileError as error:
                typer.secho(
                    f"[matey] target={selected_target.name}: unable to sync lockfile: {error}",
                    fg="red",
                )
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name}: unable to update schema artifacts: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if not options.quiet:
                if changed:
                    typer.secho(
                        f"[matey] target={selected_target.name}: wrote {paths.schema_file}",
                        fg="green",
                    )
                else:
                    typer.secho(
                        f"[matey] target={selected_target.name}: schema artifacts already up to date.",
                        fg="green",
                    )
                typer.secho(
                    f"[matey] target={selected_target.name}: synced {lockfile_path(paths)} "
                    f"(engine={lock.engine}, steps={lock.head_index}).",
                    fg="green",
                )

        if failures:
            raise typer.Exit(1)

    @schema_app.command("diff", help=command_help("schema", "diff"))
    def schema_diff(
        ctx: typer.Context,
    ) -> None:
        options = get_options(ctx)
        renderer = RichDbmateRenderer(
            options=OutputOptions(verbose=options.verbose, quiet=options.quiet),
        )

        try:
            context = build_execution_context(options)
        except (ConfigError, TargetSelectionError) as error:
            raise typer.BadParameter(str(error)) from error

        failures = 0
        for selected_target in context.selected_targets:
            try:
                paths, _real_url, test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=selected_target,
                    require_real_url=False,
                )
                result = _run_schema_replay(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    test_url=test_url,
                    keep_scratch=options.keep_scratch,
                    base_branch=options.base_branch,
                    clean=False,
                    renderer=renderer,
                )
            except (
                PathResolutionError,
                URLResolutionError,
                ConfigError,
                SchemaValidationError,
                LockfileError,
            ) as error:
                typer.secho(f"[matey] target={selected_target.name} diff error: {error}", fg="red")
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name} unexpected diff error: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if result.diff_text:
                typer.echo(f"=== schema replay diff [target={selected_target.name}] ===")
                typer.echo(result.diff_text.rstrip())
            if result.error:
                typer.secho(f"[matey] target={selected_target.name}: {result.error}", fg="red")
                failures += 1
                continue
            if options.keep_scratch and result.scratch_url:
                typer.echo(f"[matey] target={selected_target.name}: keeping scratch at {result.scratch_url}")
            if not result.success:
                failures += 1
            elif not options.quiet:
                typer.secho(
                    f"[matey] target={selected_target.name}: no schema differences found.",
                    fg="green",
                )

        if failures:
            raise typer.Exit(1)
