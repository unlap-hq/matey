from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Annotated

import typer

from matey.cli.common import (
    build_execution_context,
    get_options,
    read_schema_sql,
    resolve_lock_engine_for_sync,
    resolve_target_execution,
)
from matey.cli.output import OutputOptions, RichDbmateRenderer
from matey.domain import (
    ConfigError,
    LockfileError,
    PathResolutionError,
    ResolvedPaths,
    SchemaValidationError,
    TargetSelectionError,
    URLResolutionError,
)
from matey.workflows.lockfile import (
    build_schema_lock,
    lockfile_path,
    write_schema_lock,
)
from matey.workflows.schema_lock import (
    SchemaReplayResult,
    evaluate_schema_lock_target,
    validate_schema_lock_down_target,
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


def _write_temp_lock(path: Path, lock) -> Path:
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
    tmp_path = _write_temp_text(path, previous)
    tmp_path.replace(path)
    _fsync_parent(path)


def _atomic_write_schema_and_lock(
    *,
    paths: ResolvedPaths,
    schema_sql: str,
    real_url: str | None,
    test_url: str | None,
    target_name: str,
) -> tuple[bool, object]:
    lock_path = lockfile_path(paths)
    previous_schema = read_schema_sql(paths.schema_file) if paths.schema_file.exists() else None
    previous_lock = lock_path.read_text(encoding="utf-8") if lock_path.exists() else None

    schema_tmp: Path | None = None
    lock_tmp: Path | None = None
    schema_replaced = False
    lock_replaced = False
    try:
        changed = previous_schema != schema_sql
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
        schema_tmp = _write_temp_text(paths.schema_file, schema_sql)
        lock_tmp = _write_temp_lock(lock_path, lock)

        schema_tmp.replace(paths.schema_file)
        schema_replaced = True
        lock_tmp.replace(lock_path)
        lock_replaced = True
        _fsync_parent(paths.schema_file)
        _fsync_parent(lock_path)
    except Exception:
        if schema_tmp is not None:
            schema_tmp.unlink(missing_ok=True)
        if lock_tmp is not None:
            lock_tmp.unlink(missing_ok=True)
        if schema_replaced or lock_replaced:
            _restore_previous_file(paths.schema_file, previous_schema)
            _restore_previous_file(lock_path, previous_lock)
        raise
    return changed, lock


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


def register(schema_app: typer.Typer) -> None:
    @schema_app.command("validate", help="Validate schema.sql against lockfile replay.")
    def schema_validate(
        ctx: typer.Context,
        clean: Annotated[
            bool, typer.Option("--clean", help="Force full replay from empty scratch.")
        ] = False,
        down: Annotated[
            bool, typer.Option("--down", help="Validate changed-tail downgrade round-trip.")
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
                result = _run_schema_replay(
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
                continue

            if down:
                down_result = validate_schema_lock_down_target(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    test_url=test_url,
                    keep_scratch=options.keep_scratch,
                    base_branch=options.base_branch,
                    on_dbmate_result=renderer.handle,
                )
                if down_result.diff_text:
                    typer.echo(f"=== down round-trip diff [target={selected_target.name}] ===")
                    typer.echo(down_result.diff_text.rstrip())
                if down_result.error:
                    typer.secho(f"[matey] target={selected_target.name}: {down_result.error}", fg="red")
                if options.keep_scratch and down_result.scratch_url:
                    typer.echo(
                        f"[matey] target={selected_target.name}: keeping scratch at {down_result.scratch_url}"
                    )
                if not down_result.success:
                    failures += 1
                    continue

            if not options.quiet:
                typer.secho(f"[matey] target={selected_target.name}: schema validation passed.", fg="green")

        if failures:
            raise typer.Exit(1)

    @schema_app.command("regen", help="Regenerate schema.sql and schema.lock.toml from replay.")
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
                result = _run_schema_replay(
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

            if result.diff_text:
                typer.echo(f"=== schema replay diff [target={selected_target.name}] ===")
                typer.echo(result.diff_text.rstrip())
            if result.error:
                typer.secho(f"[matey] target={selected_target.name}: {result.error}", fg="red")
                failures += 1
                continue

            try:
                changed, lock = _atomic_write_schema_and_lock(
                    paths=paths,
                    schema_sql=result.expected_schema_sql,
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

            if options.keep_scratch and result.scratch_url:
                typer.echo(f"[matey] target={selected_target.name}: keeping scratch at {result.scratch_url}")

            if not options.quiet:
                if changed:
                    typer.secho(
                        f"[matey] target={selected_target.name}: wrote {paths.schema_file}",
                        fg="green",
                    )
                else:
                    typer.secho(
                        f"[matey] target={selected_target.name}: schema already up to date.",
                        fg="green",
                    )
                typer.secho(
                    f"[matey] target={selected_target.name}: synced {lockfile_path(paths)} "
                    f"(engine={lock.engine}, steps={lock.head_index}).",
                    fg="green",
                )

        if failures:
            raise typer.Exit(1)

    @schema_app.command("diff", help="Show schema.sql vs lockfile replay diff.")
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
