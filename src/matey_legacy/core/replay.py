from __future__ import annotations

import hashlib
import re
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from matey.adapters.dbmate import (
    DbmateLogContext,
    DbmateResultCallback,
    run_dbmate,
    run_dbmate_capture,
)
from matey.adapters.fs import _extract_dump_schema, _run_capture_with_retry, _run_step_with_retry
from matey.adapters.git import (
    _detect_base_ref,
    _map_to_worktree,
    _resolve_merge_base,
    _resolve_repo_root,
    _temporary_worktree,
)
from matey.adapters.scratch import detect_engine, plan_scratch_target
from matey.core import LockfileError, ResolvedPaths, SchemaValidationError
from matey.core.diff import normalize_sql_text, read_schema_sql, schema_diff_text
from matey.core.lock import SchemaLock, load_schema_lock, lockfile_path, verify_schema_lock
from matey.core.scratch import cleanup_scratch, ensure_scratch_ready, scratch_retry_count
from matey.settings.env import RuntimeEnv, load_runtime_env

__all__ = [
    "SchemaDownResult",
    "SchemaEvaluationResult",
    "SchemaReplayResult",
    "collect_replay_checkpoint_updates",
    "evaluate_schema_lock_target",
    "evaluate_schema_lock_target_with_down",
    "expected_schema_from_head_lock",
    "validate_schema_lock_down_target",
]


@dataclass(frozen=True)
class SchemaReplayResult:
    target_name: str
    success: bool
    head_schema_sql: str
    expected_schema_sql: str
    diff_text: str | None
    scratch_url: str
    error: str | None = None


@dataclass(frozen=True)
class SchemaDownResult:
    target_name: str
    success: bool
    scratch_url: str
    diff_text: str | None = None
    error: str | None = None


@dataclass(frozen=True)
class SchemaEvaluationResult:
    replay: SchemaReplayResult
    down: SchemaDownResult


@dataclass(frozen=True)
class _DownTailStep:
    migration_ref: str
    migration_file_name: str
    has_down_sql: bool


@dataclass(frozen=True)
class _HeadMigrationStep:
    migration_ref: str
    migration_file_name: str
    migration_file_path: Path
    migration_digest: str
    checkpoint_file: str


@dataclass(frozen=True)
class _ReplayPlan:
    engine: str
    wait_timeout: str
    anchor_schema_sql: str | None
    tail_steps: tuple[_HeadMigrationStep, ...]


def _dbmate_wait_timeout(runtime_env: RuntimeEnv) -> str:
    return runtime_env.dbmate_wait_timeout


def _build_base_paths_in_worktree(*, head_paths: ResolvedPaths, repo_root: Path, worktree: Path) -> ResolvedPaths:
    base_migrations = _map_to_worktree(head_paths.migrations_dir, repo_root, worktree)
    base_schema = _map_to_worktree(head_paths.schema_file, repo_root, worktree)
    base_db_dir = base_migrations.parent
    return ResolvedPaths(
        db_dir=base_db_dir,
        migrations_dir=base_migrations,
        schema_file=base_schema,
    )


def _load_and_verify_lock(paths: ResolvedPaths) -> SchemaLock:
    lock = load_schema_lock(lockfile_path(paths))
    verify_schema_lock(lock, db_dir=paths.db_dir)
    return lock


def _load_and_verify_lock_optional(paths: ResolvedPaths) -> SchemaLock | None:
    if not lockfile_path(paths).exists():
        return None
    return _load_and_verify_lock(paths)


def _digest_file(path: Path) -> str:
    return hashlib.blake2b(path.read_bytes(), digest_size=32).hexdigest()


def _relative_to_db(path: Path, *, db_dir: Path) -> str:
    try:
        return path.relative_to(db_dir).as_posix()
    except ValueError as error:
        raise LockfileError(f"Path {path} is outside target db dir {db_dir}.") from error


def _sorted_migrations(migrations_dir: Path) -> list[Path]:
    if not migrations_dir.exists():
        return []
    migrations = [path for path in migrations_dir.iterdir() if path.is_file() and path.suffix == ".sql"]
    return sorted(migrations, key=lambda item: item.name)


def _default_checkpoint_for_migration(migration_path: Path) -> str:
    return f"checkpoints/{migration_path.stem}.sql"


def _existing_checkpoint_map(lock: SchemaLock) -> dict[str, str]:
    return {step.migration_file: step.checkpoint_file for step in lock.steps}


def _collect_head_migration_steps(*, paths: ResolvedPaths, lock: SchemaLock) -> tuple[_HeadMigrationStep, ...]:
    checkpoint_map = _existing_checkpoint_map(lock)
    steps: list[_HeadMigrationStep] = []
    for migration_path in _sorted_migrations(paths.migrations_dir):
        migration_ref = _relative_to_db(migration_path, db_dir=paths.db_dir)
        steps.append(
            _HeadMigrationStep(
                migration_ref=migration_ref,
                migration_file_name=migration_path.name,
                migration_file_path=migration_path,
                migration_digest=_digest_file(migration_path),
                checkpoint_file=checkpoint_map.get(
                    migration_ref,
                    _default_checkpoint_for_migration(migration_path),
                ),
            )
        )
    return tuple(steps)


def _first_divergence_index(*, base_lock: SchemaLock | None, head_steps: tuple[_HeadMigrationStep, ...]) -> int:
    if base_lock is None:
        return 1

    shared = min(len(base_lock.steps), len(head_steps))
    for index in range(shared):
        base_step = base_lock.steps[index]
        head_step = head_steps[index]
        if (
            base_step.migration_file != head_step.migration_ref
            or base_step.migration_digest != head_step.migration_digest
        ):
            return index + 1

    if len(base_lock.steps) == len(head_steps):
        return len(head_steps) + 1
    return shared + 1


def _read_anchor_schema_sql(
    *,
    base_paths: ResolvedPaths,
    base_lock: SchemaLock | None,
    divergence: int,
    context: str,
) -> str | None:
    if divergence <= 1:
        return None
    if base_lock is None:
        raise LockfileError(f"Unable to resolve {context} anchor checkpoint without a base lockfile.")

    anchor_step = base_lock.steps[divergence - 2]
    anchor_checkpoint = base_paths.db_dir / anchor_step.checkpoint_file
    if not anchor_checkpoint.exists():
        raise LockfileError(
            f"Unable to resolve {context} anchor checkpoint for divergence replay: {anchor_checkpoint}"
        )
    return anchor_checkpoint.read_text(encoding="utf-8")


@contextmanager
def _temporary_tail_migrations_dir(*, source_migrations_dir: Path, file_names: list[str]) -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="matey-tail-migrations-") as tmp_name:
        tmp_dir = Path(tmp_name)
        for name in file_names:
            source_path = source_migrations_dir / name
            if not source_path.exists():
                raise LockfileError(f"Tail migration file is missing: {source_path}")
            tmp_path = tmp_dir / name
            tmp_path.write_bytes(source_path.read_bytes())
        yield tmp_dir


_SECTION_MARKER_PATTERN = re.compile(
    r"^\s*--\s*migrate:(up|down)\b.*$",
    re.IGNORECASE | re.MULTILINE,
)
_DOWN_MARKER_PATTERN = re.compile(r"^\s*--\s*migrate:down\b.*$", re.IGNORECASE | re.MULTILINE)
_LINE_COMMENT_ONLY_PATTERN = re.compile(r"^\s*--.*$", re.MULTILINE)
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)


def _extract_down_section(migration_text: str) -> str:
    down_match = _DOWN_MARKER_PATTERN.search(migration_text)
    if down_match is None:
        return ""
    section_start = down_match.end()
    next_section = _SECTION_MARKER_PATTERN.search(migration_text, section_start)
    section_end = next_section.start() if next_section is not None else len(migration_text)
    return migration_text[section_start:section_end]


def _contains_executable_sql(section_text: str) -> bool:
    without_block_comments = _BLOCK_COMMENT_PATTERN.sub("", section_text)
    without_line_comments = _LINE_COMMENT_ONLY_PATTERN.sub("", without_block_comments)
    stripped = re.sub(r"[;\s]+", "", without_line_comments)
    return bool(stripped)


def _migration_has_down_sql(migration_file: Path) -> bool:
    try:
        text = migration_file.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise LockfileError(f"Missing migration file: {migration_file}") from error
    return _contains_executable_sql(_extract_down_section(text))


def _resolve_replay_engine(*, head_lock: SchemaLock, test_url: str | None) -> str:
    if test_url:
        detected = detect_engine(test_url)
        if detected != head_lock.engine:
            raise SchemaValidationError(
                "Scratch URL engine does not match lockfile engine "
                f"({detected} != {head_lock.engine})."
            )
        return detected
    return head_lock.engine


def _finalize_error(*, error: str | None, cleanup_error: str | None) -> str | None:
    if error and cleanup_error:
        return f"{error} Cleanup error: {cleanup_error}"
    return error or cleanup_error


def _scratch_schema_from_replay(
    *,
    target_name: str,
    dbmate_binary: Path,
    engine: str,
    head_paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    wait_timeout: str,
    anchor_schema_sql: str | None,
    tail_migration_file_names: list[str],
    on_dbmate_result: DbmateResultCallback | None,
) -> tuple[str, str]:
    scratch_plan = plan_scratch_target(
        engine=engine,
        scratch_name=f"matey_{target_name}_schema_replay",
        test_url=test_url,
    )
    scratch_url = scratch_plan.target.scratch_url
    retries = scratch_retry_count(
        engine=engine,
        auto_provisioned=scratch_plan.target.auto_provisioned,
    )

    runtime_schema_dir = Path(tempfile.mkdtemp(prefix="matey-replay-schema-file-"))
    runtime_schema_file = runtime_schema_dir / f"{target_name}.replay.sql"
    runtime_schema_file.write_text("", encoding="utf-8")
    load_schema_file = runtime_schema_dir / f"{target_name}.replay.anchor.sql"

    schema_sql: str | None = None
    error: str | None = None
    try:
        ready_error = ensure_scratch_ready(
            target_name=target_name,
            phase="replay",
            dbmate_binary=dbmate_binary,
            scratch_url=scratch_url,
            migrations_dir=head_paths.migrations_dir,
            runtime_schema_file=runtime_schema_file,
            engine=engine,
            retries=retries,
            wait_timeout=wait_timeout,
            wait_error="dbmate wait failed on replay scratch target.",
            create_error="dbmate create failed on replay scratch target.",
            on_dbmate_result=on_dbmate_result,
            run_dbmate_fn=run_dbmate,
        )
        if ready_error is not None:
            error = ready_error
            return "", scratch_url

        if anchor_schema_sql is not None:
            load_schema_file.write_text(anchor_schema_sql, encoding="utf-8")
            load_code = _run_step_with_retry(
                retries=retries,
                delay_seconds=1.0,
                run_step=lambda: run_dbmate(
                    dbmate_binary=dbmate_binary,
                    url=scratch_url,
                    migrations_dir=head_paths.migrations_dir,
                    schema_file=load_schema_file,
                    verb="load",
                    global_args=["--no-dump-schema"],
                    log_context=DbmateLogContext(
                        target=target_name,
                        phase="replay",
                        step="load-anchor",
                    ),
                    on_result=on_dbmate_result,
                ),
            )
            if load_code != 0:
                error = "dbmate load failed while loading replay anchor checkpoint."
                return "", scratch_url

        if tail_migration_file_names:
            with _temporary_tail_migrations_dir(
                source_migrations_dir=head_paths.migrations_dir,
                file_names=tail_migration_file_names,
            ) as tail_dir:
                up_code = _run_step_with_retry(
                    retries=retries,
                    delay_seconds=1.0,
                    run_step=lambda: run_dbmate(
                        dbmate_binary=dbmate_binary,
                        url=scratch_url,
                        migrations_dir=tail_dir,
                        schema_file=runtime_schema_file,
                        verb="up",
                        global_args=["--no-dump-schema"],
                        log_context=DbmateLogContext(target=target_name, phase="replay", step="up-tail"),
                        on_result=on_dbmate_result,
                    ),
                )
            if up_code != 0:
                error = "dbmate up failed while replaying tail migrations."
                return "", scratch_url

        dump_result = _run_capture_with_retry(
            retries=retries,
            delay_seconds=1.0,
            run_step=lambda: run_dbmate_capture(
                dbmate_binary=dbmate_binary,
                url=scratch_url,
                migrations_dir=head_paths.migrations_dir,
                schema_file=runtime_schema_file,
                verb="dump",
                log_context=DbmateLogContext(target=target_name, phase="replay", step="dump"),
                on_result=on_dbmate_result,
            ),
        )
        if dump_result.returncode != 0:
            details = (dump_result.stderr or dump_result.stdout or "").strip()
            error = f"dbmate dump failed on replay scratch target. {details}".strip()
            return "", scratch_url
        schema_sql = normalize_sql_text(_extract_dump_schema(dump_result=dump_result, schema_file=runtime_schema_file))
        return schema_sql, scratch_url
    finally:
        cleanup_error = cleanup_scratch(
            target_name=target_name,
            phase="replay",
            dbmate_binary=dbmate_binary,
            scratch_url=scratch_url,
            migrations_dir=head_paths.migrations_dir,
            runtime_schema_file=runtime_schema_file,
            runtime_schema_dir=runtime_schema_dir,
            keep_scratch=keep_scratch,
            cleanup_required=scratch_plan.target.cleanup_required,
            cleanup=scratch_plan.cleanup,
            on_dbmate_result=on_dbmate_result,
            drop_error="dbmate drop failed while cleaning replay scratch target.",
            run_dbmate_fn=run_dbmate,
        )
        final_error = _finalize_error(error=error, cleanup_error=cleanup_error)
        if final_error is not None:
            raise SchemaValidationError(final_error)



def expected_schema_from_head_lock(*, paths: ResolvedPaths) -> str:
    _load_and_verify_lock(paths)
    return read_schema_sql(paths.schema_file)


def _dump_schema(
    *,
    target_name: str,
    phase: str,
    dbmate_binary: Path,
    scratch_url: str,
    migrations_dir: Path,
    runtime_schema_file: Path,
    retries: int,
    step: str,
    error_prefix: str,
    on_dbmate_result: DbmateResultCallback | None,
) -> tuple[str | None, str | None]:
    dump_result = _run_capture_with_retry(
        retries=retries,
        delay_seconds=1.0,
        run_step=lambda: run_dbmate_capture(
            dbmate_binary=dbmate_binary,
            url=scratch_url,
            migrations_dir=migrations_dir,
            schema_file=runtime_schema_file,
            verb="dump",
            log_context=DbmateLogContext(target=target_name, phase=phase, step=step),
            on_result=on_dbmate_result,
        ),
    )
    if dump_result.returncode != 0:
        details = (dump_result.stderr or dump_result.stdout or "").strip()
        return None, f"{error_prefix}. {details}".strip()
    return (
        normalize_sql_text(_extract_dump_schema(dump_result=dump_result, schema_file=runtime_schema_file)),
        None,
    )


def _run_down_roundtrip(
    *,
    target_name: str,
    dbmate_binary: Path,
    head_paths: ResolvedPaths,
    engine: str,
    anchor_schema_sql: str | None,
    tail_steps: list[_DownTailStep],
    test_url: str | None,
    keep_scratch: bool,
    wait_timeout: str,
    on_dbmate_result: DbmateResultCallback | None,
) -> SchemaDownResult:
    scratch_plan = plan_scratch_target(
        engine=engine,
        scratch_name=f"matey_{target_name}_schema_down",
        test_url=test_url,
    )
    scratch_url = scratch_plan.target.scratch_url
    retries = scratch_retry_count(
        engine=engine,
        auto_provisioned=scratch_plan.target.auto_provisioned,
    )

    runtime_schema_dir = Path(tempfile.mkdtemp(prefix="matey-down-schema-file-"))
    runtime_schema_file = runtime_schema_dir / f"{target_name}.down.sql"
    runtime_schema_file.write_text("", encoding="utf-8")
    load_schema_file = runtime_schema_dir / f"{target_name}.down.anchor.sql"

    result = SchemaDownResult(target_name=target_name, success=True, scratch_url=scratch_url)
    try:
        ready_error = ensure_scratch_ready(
            target_name=target_name,
            phase="down",
            dbmate_binary=dbmate_binary,
            scratch_url=scratch_url,
            migrations_dir=head_paths.migrations_dir,
            runtime_schema_file=runtime_schema_file,
            engine=engine,
            retries=retries,
            wait_timeout=wait_timeout,
            wait_error="dbmate wait failed for down-validation scratch target.",
            create_error="dbmate create failed for down-validation scratch target.",
            on_dbmate_result=on_dbmate_result,
            run_dbmate_fn=run_dbmate,
        )
        if ready_error is not None:
            result = SchemaDownResult(
                target_name=target_name,
                success=False,
                scratch_url=scratch_url,
                error=ready_error,
            )

        if result.success and anchor_schema_sql is not None:
            load_schema_file.write_text(anchor_schema_sql, encoding="utf-8")
            load_code = _run_step_with_retry(
                retries=retries,
                delay_seconds=1.0,
                run_step=lambda: run_dbmate(
                    dbmate_binary=dbmate_binary,
                    url=scratch_url,
                    migrations_dir=head_paths.migrations_dir,
                    schema_file=load_schema_file,
                    verb="load",
                    global_args=["--no-dump-schema"],
                    log_context=DbmateLogContext(target=target_name, phase="down", step="load-anchor"),
                    on_result=on_dbmate_result,
                ),
            )
            if load_code != 0:
                result = SchemaDownResult(
                    target_name=target_name,
                    success=False,
                    scratch_url=scratch_url,
                    error="dbmate load failed while loading anchor for down-validation.",
                )

        for step_index, tail_step in enumerate(tail_steps, start=1):
            if not result.success:
                break
            baseline_schema_sql: str | None = None
            if tail_step.has_down_sql:
                baseline_schema_sql, dump_error = _dump_schema(
                    target_name=target_name,
                    phase="down",
                    dbmate_binary=dbmate_binary,
                    scratch_url=scratch_url,
                    migrations_dir=head_paths.migrations_dir,
                    runtime_schema_file=runtime_schema_file,
                    retries=retries,
                    step=f"dump-base-{step_index}",
                    error_prefix="dbmate dump failed before down-check step",
                    on_dbmate_result=on_dbmate_result,
                )
                if dump_error is not None:
                    result = SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        error=f"{dump_error} ({tail_step.migration_ref})",
                    )
                    break

            with _temporary_tail_migrations_dir(
                source_migrations_dir=head_paths.migrations_dir,
                file_names=[tail_step.migration_file_name],
            ) as step_dir:
                up_step = f"up-{step_index}"
                up_code = _run_step_with_retry(
                    retries=retries,
                    delay_seconds=1.0,
                    run_step=lambda step_dir=step_dir, up_step=up_step: run_dbmate(
                        dbmate_binary=dbmate_binary,
                        url=scratch_url,
                        migrations_dir=step_dir,
                        schema_file=runtime_schema_file,
                        verb="up",
                        global_args=["--no-dump-schema"],
                        log_context=DbmateLogContext(target=target_name, phase="down", step=up_step),
                        on_result=on_dbmate_result,
                    ),
                )
                if up_code != 0:
                    result = SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        error=(
                            "dbmate up failed while applying tail migration during down-validation "
                            f"({tail_step.migration_ref})."
                        ),
                    )
                    break

                if not tail_step.has_down_sql:
                    continue

                rollback_step = f"rollback-{step_index}"
                rollback_code = _run_step_with_retry(
                    retries=retries,
                    delay_seconds=1.0,
                    run_step=lambda step_dir=step_dir, rollback_step=rollback_step: run_dbmate(
                        dbmate_binary=dbmate_binary,
                        url=scratch_url,
                        migrations_dir=step_dir,
                        schema_file=runtime_schema_file,
                        verb="rollback",
                        global_args=["--no-dump-schema"],
                        extra_args=["1"],
                        log_context=DbmateLogContext(target=target_name, phase="down", step=rollback_step),
                        on_result=on_dbmate_result,
                    ),
                )
                if rollback_code != 0:
                    result = SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        error=(
                            "dbmate rollback failed during down-validation "
                            f"({tail_step.migration_ref})."
                        ),
                    )
                    break

                rolled_back_schema_sql, dump_error = _dump_schema(
                    target_name=target_name,
                    phase="down",
                    dbmate_binary=dbmate_binary,
                    scratch_url=scratch_url,
                    migrations_dir=head_paths.migrations_dir,
                    runtime_schema_file=runtime_schema_file,
                    retries=retries,
                    step=f"dump-rollback-{step_index}",
                    error_prefix="dbmate dump failed after rollback in down-check step",
                    on_dbmate_result=on_dbmate_result,
                )
                if dump_error is not None:
                    result = SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        error=f"{dump_error} ({tail_step.migration_ref})",
                    )
                    break

                diff_text = schema_diff_text(
                    baseline_schema_sql or "",
                    rolled_back_schema_sql or "",
                    expected_name=f"{tail_step.migration_ref}:before",
                    actual_name=f"{tail_step.migration_ref}:after-down",
                )
                if diff_text:
                    result = SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        diff_text=diff_text,
                        error=(
                            "Down validation failed: schema did not round-trip after rollback "
                            f"({tail_step.migration_ref})."
                        ),
                    )
                    break

                reapply_step = f"reapply-{step_index}"
                reapply_code = _run_step_with_retry(
                    retries=retries,
                    delay_seconds=1.0,
                    run_step=lambda step_dir=step_dir, reapply_step=reapply_step: run_dbmate(
                        dbmate_binary=dbmate_binary,
                        url=scratch_url,
                        migrations_dir=step_dir,
                        schema_file=runtime_schema_file,
                        verb="up",
                        global_args=["--no-dump-schema"],
                        log_context=DbmateLogContext(target=target_name, phase="down", step=reapply_step),
                        on_result=on_dbmate_result,
                    ),
                )
                if reapply_code != 0:
                    result = SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        error=(
                            "dbmate up failed while re-applying migration after down-check "
                            f"({tail_step.migration_ref})."
                        ),
                    )
                    break
    finally:
        cleanup_error = cleanup_scratch(
            target_name=target_name,
            phase="down",
            dbmate_binary=dbmate_binary,
            scratch_url=scratch_url,
            migrations_dir=head_paths.migrations_dir,
            runtime_schema_file=runtime_schema_file,
            runtime_schema_dir=runtime_schema_dir,
            keep_scratch=keep_scratch,
            cleanup_required=scratch_plan.target.cleanup_required,
            cleanup=scratch_plan.cleanup,
            on_dbmate_result=on_dbmate_result,
            drop_error="dbmate drop failed while cleaning down-validation scratch target.",
            run_dbmate_fn=run_dbmate,
        )
        if cleanup_error is not None:
            result = SchemaDownResult(
                target_name=target_name,
                success=False,
                scratch_url=scratch_url,
                diff_text=result.diff_text,
                error=_finalize_error(error=result.error, cleanup_error=cleanup_error),
            )
    return result



def _prepare_replay_plan(
    *,
    head_paths: ResolvedPaths,
    source_repo_root: Path,
    head_repo_root: Path,
    base_branch: str | None,
    test_url: str | None,
    clean: bool,
    environ: Mapping[str, str] | None,
) -> _ReplayPlan:
    runtime_env = load_runtime_env(environ=environ)
    wait_timeout = _dbmate_wait_timeout(runtime_env)

    head_lock = _load_and_verify_lock(head_paths)
    engine = _resolve_replay_engine(head_lock=head_lock, test_url=test_url)
    head_steps = _collect_head_migration_steps(paths=head_paths, lock=head_lock)

    if clean:
        divergence = 1
        anchor_schema_sql: str | None = None
    else:
        base_ref = _detect_base_ref(
            explicit_base_branch=base_branch,
            repo_root=source_repo_root,
            runtime_env=runtime_env,
        )
        merge_base = _resolve_merge_base(source_repo_root, base_ref)
        with _temporary_worktree(source_repo_root, merge_base) as base_worktree:
            base_paths = _build_base_paths_in_worktree(
                head_paths=head_paths,
                repo_root=head_repo_root,
                worktree=base_worktree,
            )
            base_lock = _load_and_verify_lock_optional(base_paths)
            divergence = _first_divergence_index(base_lock=base_lock, head_steps=head_steps)
            anchor_schema_sql = _read_anchor_schema_sql(
                base_paths=base_paths,
                base_lock=base_lock,
                divergence=divergence,
                context="base",
            )

    tail_steps = tuple(head_steps[divergence - 1 :]) if divergence <= len(head_steps) else ()
    return _ReplayPlan(
        engine=engine,
        wait_timeout=wait_timeout,
        anchor_schema_sql=anchor_schema_sql,
        tail_steps=tail_steps,
    )


def _build_down_tail_steps(tail_steps: tuple[_HeadMigrationStep, ...]) -> list[_DownTailStep]:
    return [
        _DownTailStep(
            migration_ref=step.migration_ref,
            migration_file_name=step.migration_file_name,
            has_down_sql=_migration_has_down_sql(step.migration_file_path),
        )
        for step in tail_steps
    ]


def _evaluate_schema_lock_target_impl(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    clean: bool,
    validate_down: bool,
    on_dbmate_result: DbmateResultCallback | None,
    cwd: Path | None,
    environ: Mapping[str, str] | None,
) -> tuple[SchemaReplayResult, SchemaDownResult, _ReplayPlan]:
    source_repo_root = _resolve_repo_root(cwd)
    head_paths = paths
    head_repo_root = source_repo_root

    head_schema_sql = read_schema_sql(head_paths.schema_file)
    replay_plan = _prepare_replay_plan(
        head_paths=head_paths,
        source_repo_root=source_repo_root,
        head_repo_root=head_repo_root,
        base_branch=base_branch,
        test_url=test_url,
        clean=clean,
        environ=environ,
    )

    expected_schema_sql, scratch_url = _scratch_schema_from_replay(
        target_name=target_name,
        engine=replay_plan.engine,
        dbmate_binary=dbmate_binary,
        head_paths=head_paths,
        test_url=test_url,
        keep_scratch=keep_scratch,
        wait_timeout=replay_plan.wait_timeout,
        anchor_schema_sql=replay_plan.anchor_schema_sql,
        tail_migration_file_names=[step.migration_file_name for step in replay_plan.tail_steps],
        on_dbmate_result=on_dbmate_result,
    )

    diff_text = schema_diff_text(
        head_schema_sql,
        expected_schema_sql,
        expected_name=str(head_paths.schema_file),
        actual_name=f"{target_name}.replay.sql",
    )
    replay_result = SchemaReplayResult(
        target_name=target_name,
        success=not diff_text,
        head_schema_sql=head_schema_sql,
        expected_schema_sql=expected_schema_sql,
        diff_text=diff_text or None,
        scratch_url=scratch_url,
        error=None,
    )

    if not validate_down:
        return replay_result, SchemaDownResult(target_name=target_name, success=True, scratch_url=""), replay_plan

    if not replay_result.success:
        return (
            replay_result,
            SchemaDownResult(
                target_name=target_name,
                success=False,
                scratch_url="",
                error="Skipped down-validation because forward replay diff failed.",
            ),
            replay_plan,
        )

    down_tail_steps = _build_down_tail_steps(replay_plan.tail_steps)
    if not down_tail_steps:
        return (
            replay_result,
            SchemaDownResult(target_name=target_name, success=True, scratch_url=""),
            replay_plan,
        )

    down_result = _run_down_roundtrip(
        target_name=target_name,
        dbmate_binary=dbmate_binary,
        head_paths=head_paths,
        engine=replay_plan.engine,
        anchor_schema_sql=replay_plan.anchor_schema_sql,
        tail_steps=down_tail_steps,
        test_url=test_url,
        keep_scratch=keep_scratch,
        wait_timeout=replay_plan.wait_timeout,
        on_dbmate_result=on_dbmate_result,
    )
    return replay_result, down_result, replay_plan


def evaluate_schema_lock_target(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    clean: bool,
    on_dbmate_result: DbmateResultCallback | None,
    cwd: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> SchemaReplayResult:
    replay_result, _down_result, _plan = _evaluate_schema_lock_target_impl(
        target_name=target_name,
        dbmate_binary=dbmate_binary,
        paths=paths,
        test_url=test_url,
        keep_scratch=keep_scratch,
        base_branch=base_branch,
        clean=clean,
        validate_down=False,
        on_dbmate_result=on_dbmate_result,
        cwd=cwd,
        environ=environ,
    )
    return replay_result


def evaluate_schema_lock_target_with_down(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    clean: bool,
    on_dbmate_result: DbmateResultCallback | None,
    cwd: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> SchemaEvaluationResult:
    replay_result, down_result, _plan = _evaluate_schema_lock_target_impl(
        target_name=target_name,
        dbmate_binary=dbmate_binary,
        paths=paths,
        test_url=test_url,
        keep_scratch=keep_scratch,
        base_branch=base_branch,
        clean=clean,
        validate_down=True,
        on_dbmate_result=on_dbmate_result,
        cwd=cwd,
        environ=environ,
    )
    return SchemaEvaluationResult(replay=replay_result, down=down_result)


def validate_schema_lock_down_target(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    on_dbmate_result: DbmateResultCallback | None,
    cwd: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> SchemaDownResult:
    source_repo_root = _resolve_repo_root(cwd)
    replay_plan = _prepare_replay_plan(
        head_paths=paths,
        source_repo_root=source_repo_root,
        head_repo_root=source_repo_root,
        base_branch=base_branch,
        test_url=test_url,
        clean=False,
        environ=environ,
    )

    down_tail_steps = _build_down_tail_steps(replay_plan.tail_steps)
    if not down_tail_steps:
        return SchemaDownResult(target_name=target_name, success=True, scratch_url="")

    return _run_down_roundtrip(
        target_name=target_name,
        dbmate_binary=dbmate_binary,
        head_paths=paths,
        engine=replay_plan.engine,
        anchor_schema_sql=replay_plan.anchor_schema_sql,
        tail_steps=down_tail_steps,
        test_url=test_url,
        keep_scratch=keep_scratch,
        wait_timeout=replay_plan.wait_timeout,
        on_dbmate_result=on_dbmate_result,
    )


def _collect_tail_checkpoint_updates(
    *,
    target_name: str,
    dbmate_binary: Path,
    head_paths: ResolvedPaths,
    replay_plan: _ReplayPlan,
    test_url: str | None,
    keep_scratch: bool,
    on_dbmate_result: DbmateResultCallback | None,
) -> dict[str, str]:
    if not replay_plan.tail_steps:
        return {}

    scratch_plan = plan_scratch_target(
        engine=replay_plan.engine,
        scratch_name=f"matey_{target_name}_schema_checkpoint",
        test_url=test_url,
    )
    scratch_url = scratch_plan.target.scratch_url
    retries = scratch_retry_count(
        engine=replay_plan.engine,
        auto_provisioned=scratch_plan.target.auto_provisioned,
    )

    runtime_schema_dir = Path(tempfile.mkdtemp(prefix="matey-checkpoint-schema-file-"))
    runtime_schema_file = runtime_schema_dir / f"{target_name}.checkpoint.sql"
    runtime_schema_file.write_text("", encoding="utf-8")
    load_schema_file = runtime_schema_dir / f"{target_name}.checkpoint.anchor.sql"

    error: str | None = None
    checkpoint_updates: dict[str, str] = {}
    try:
        ready_error = ensure_scratch_ready(
            target_name=target_name,
            phase="checkpoint",
            dbmate_binary=dbmate_binary,
            scratch_url=scratch_url,
            migrations_dir=head_paths.migrations_dir,
            runtime_schema_file=runtime_schema_file,
            engine=replay_plan.engine,
            retries=retries,
            wait_timeout=replay_plan.wait_timeout,
            wait_error="dbmate wait failed for checkpoint scratch target.",
            create_error="dbmate create failed for checkpoint scratch target.",
            on_dbmate_result=on_dbmate_result,
            run_dbmate_fn=run_dbmate,
        )
        if ready_error is not None:
            error = ready_error
            return {}

        if replay_plan.anchor_schema_sql is not None:
            load_schema_file.write_text(replay_plan.anchor_schema_sql, encoding="utf-8")
            load_code = _run_step_with_retry(
                retries=retries,
                delay_seconds=1.0,
                run_step=lambda: run_dbmate(
                    dbmate_binary=dbmate_binary,
                    url=scratch_url,
                    migrations_dir=head_paths.migrations_dir,
                    schema_file=load_schema_file,
                    verb="load",
                    global_args=["--no-dump-schema"],
                    log_context=DbmateLogContext(target=target_name, phase="checkpoint", step="load-anchor"),
                    on_result=on_dbmate_result,
                ),
            )
            if load_code != 0:
                error = "dbmate load failed while loading anchor for checkpoint capture."
                return {}

        for step_index, tail_step in enumerate(replay_plan.tail_steps, start=1):
            with _temporary_tail_migrations_dir(
                source_migrations_dir=head_paths.migrations_dir,
                file_names=[tail_step.migration_file_name],
            ) as step_dir:
                up_code = _run_step_with_retry(
                    retries=retries,
                    delay_seconds=1.0,
                    run_step=lambda step_dir=step_dir, step_index=step_index: run_dbmate(
                        dbmate_binary=dbmate_binary,
                        url=scratch_url,
                        migrations_dir=step_dir,
                        schema_file=runtime_schema_file,
                        verb="up",
                        global_args=["--no-dump-schema"],
                        log_context=DbmateLogContext(
                            target=target_name,
                            phase="checkpoint",
                            step=f"up-{step_index}",
                        ),
                        on_result=on_dbmate_result,
                    ),
                )
                if up_code != 0:
                    error = (
                        "dbmate up failed while applying migration for checkpoint capture "
                        f"({tail_step.migration_ref})."
                    )
                    return {}

            schema_sql, dump_error = _dump_schema(
                target_name=target_name,
                phase="checkpoint",
                dbmate_binary=dbmate_binary,
                scratch_url=scratch_url,
                migrations_dir=head_paths.migrations_dir,
                runtime_schema_file=runtime_schema_file,
                retries=retries,
                step=f"dump-{step_index}",
                error_prefix="dbmate dump failed while capturing checkpoint",
                on_dbmate_result=on_dbmate_result,
            )
            if dump_error is not None:
                error = f"{dump_error} ({tail_step.migration_ref})"
                return {}
            checkpoint_updates[tail_step.checkpoint_file] = schema_sql or ""

        return checkpoint_updates
    finally:
        cleanup_error = cleanup_scratch(
            target_name=target_name,
            phase="checkpoint",
            dbmate_binary=dbmate_binary,
            scratch_url=scratch_url,
            migrations_dir=head_paths.migrations_dir,
            runtime_schema_file=runtime_schema_file,
            runtime_schema_dir=runtime_schema_dir,
            keep_scratch=keep_scratch,
            cleanup_required=scratch_plan.target.cleanup_required,
            cleanup=scratch_plan.cleanup,
            on_dbmate_result=on_dbmate_result,
            drop_error="dbmate drop failed while cleaning checkpoint scratch target.",
            run_dbmate_fn=run_dbmate,
        )
        final_error = _finalize_error(error=error, cleanup_error=cleanup_error)
        if final_error is not None:
            raise SchemaValidationError(final_error)


def collect_replay_checkpoint_updates(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    clean: bool,
    on_dbmate_result: DbmateResultCallback | None,
    cwd: Path | None = None,
    environ: Mapping[str, str] | None = None,
) -> dict[str, str]:
    source_repo_root = _resolve_repo_root(cwd)
    replay_plan = _prepare_replay_plan(
        head_paths=paths,
        source_repo_root=source_repo_root,
        head_repo_root=source_repo_root,
        base_branch=base_branch,
        test_url=test_url,
        clean=clean,
        environ=environ,
    )
    return _collect_tail_checkpoint_updates(
        target_name=target_name,
        dbmate_binary=dbmate_binary,
        head_paths=paths,
        replay_plan=replay_plan,
        test_url=test_url,
        keep_scratch=keep_scratch,
        on_dbmate_result=on_dbmate_result,
    )
