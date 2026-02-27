from __future__ import annotations

import re
import shutil
import tempfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from matey.domain import LockfileError, ResolvedPaths, SchemaValidationError
from matey.drivers.dbmate import (
    DbmateLogContext,
    DbmateResultCallback,
    run_dbmate,
    run_dbmate_capture,
)
from matey.drivers.scratch import detect_engine, plan_scratch_target
from matey.env import RuntimeEnv, load_runtime_env
from matey.workflows.lockfile import (
    SchemaLock,
    first_divergence_index,
    load_schema_lock,
    lockfile_path,
    migration_file_names_for_steps,
    verify_schema_lock,
)
from matey.workflows.schema import run_clean_check
from matey.workflows.schema_diff import normalize_sql_text, read_schema_sql, schema_diff_text
from matey.workflows.schema_exec import (
    _extract_dump_schema,
    _run_capture_with_retry,
    _run_step_with_retry,
)
from matey.workflows.schema_git import (
    _detect_base_ref,
    _map_to_worktree,
    _resolve_merge_base,
    _resolve_repo_root,
    _temporary_worktree,
)

__all__ = [
    "SchemaDownResult",
    "SchemaReplayResult",
    "evaluate_schema_lock_target",
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


def _dbmate_wait_timeout(runtime_env: RuntimeEnv) -> str:
    return runtime_env.dbmate_wait_timeout


def _scratch_retry_count(*, engine: str, auto_provisioned: bool) -> int:
    return 12 if auto_provisioned and engine != "sqlite" else 1


def _ensure_scratch_ready(
    *,
    target_name: str,
    phase: str,
    dbmate_binary: Path,
    scratch_url: str,
    migrations_dir: Path,
    runtime_schema_file: Path,
    engine: str,
    retries: int,
    wait_timeout: str,
    wait_error: str,
    create_error: str,
    on_dbmate_result: DbmateResultCallback | None,
) -> str | None:
    if engine != "bigquery":
        wait_code = _run_step_with_retry(
            retries=retries,
            delay_seconds=1.0,
            run_step=lambda: run_dbmate(
                dbmate_binary=dbmate_binary,
                url=scratch_url,
                migrations_dir=migrations_dir,
                schema_file=runtime_schema_file,
                verb="wait",
                global_args=["--wait-timeout", wait_timeout],
                log_context=DbmateLogContext(target=target_name, phase=phase, step="wait"),
                on_result=on_dbmate_result,
            ),
        )
        if wait_code != 0:
            return wait_error

    create_code = _run_step_with_retry(
        retries=retries,
        delay_seconds=1.0,
        run_step=lambda: run_dbmate(
            dbmate_binary=dbmate_binary,
            url=scratch_url,
            migrations_dir=migrations_dir,
            schema_file=runtime_schema_file,
            verb="create",
            log_context=DbmateLogContext(target=target_name, phase=phase, step="create"),
            on_result=on_dbmate_result,
        ),
    )
    if create_code != 0:
        return create_error

    return None


def _cleanup_scratch(
    *,
    target_name: str,
    phase: str,
    dbmate_binary: Path,
    scratch_url: str,
    migrations_dir: Path,
    runtime_schema_file: Path,
    runtime_schema_dir: Path,
    keep_scratch: bool,
    cleanup_required: bool,
    cleanup: Callable[[], None],
    on_dbmate_result: DbmateResultCallback | None,
) -> None:
    if not keep_scratch and cleanup_required:
        run_dbmate(
            dbmate_binary=dbmate_binary,
            url=scratch_url,
            migrations_dir=migrations_dir,
            schema_file=runtime_schema_file,
            verb="drop",
            log_context=DbmateLogContext(target=target_name, phase=phase, step="drop"),
            on_result=on_dbmate_result,
        )
    if not keep_scratch:
        cleanup()
    shutil.rmtree(runtime_schema_dir, ignore_errors=True)


@contextmanager
def _temporary_tail_migrations_dir(
    *,
    source_migrations_dir: Path,
    file_names: list[str],
) -> Iterator[Path]:
    with tempfile.TemporaryDirectory(prefix="matey-tail-migrations-") as tmp_name:
        tmp_dir = Path(tmp_name)
        for name in file_names:
            source_path = source_migrations_dir / name
            if not source_path.exists():
                raise LockfileError(f"Tail migration file referenced by lock is missing: {source_path}")
            shutil.copy2(source_path, tmp_dir / name)
        yield tmp_dir


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


_DOWN_MARKER_PATTERN = re.compile(r"^\s*--\s*migrate:down\b", re.IGNORECASE | re.MULTILINE)


def _migration_has_down(migration_file: Path) -> bool:
    try:
        text = migration_file.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise LockfileError(f"Missing migration file referenced by lock: {migration_file}") from error
    return bool(_DOWN_MARKER_PATTERN.search(text))


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


def _scratch_schema_from_replay(
    *,
    target_name: str,
    dbmate_binary: Path,
    engine: str,
    head_paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    wait_timeout: str,
    anchor_checkpoint_file: Path | None,
    tail_migration_file_names: list[str],
    on_dbmate_result: DbmateResultCallback | None,
) -> tuple[str, str]:
    scratch_plan = plan_scratch_target(
        engine=engine,
        scratch_name=f"matey_{target_name}_schema_replay",
        test_url=test_url,
    )
    scratch_url = scratch_plan.target.scratch_url
    retries = _scratch_retry_count(
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
        ready_error = _ensure_scratch_ready(
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
        )
        if ready_error is not None:
            error = ready_error
            raise SchemaValidationError(error)

        if anchor_checkpoint_file is not None:
            load_schema_file.write_text(
                anchor_checkpoint_file.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
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
                raise SchemaValidationError(error)

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
                raise SchemaValidationError(error)

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
            raise SchemaValidationError(error)
        schema_sql = _extract_dump_schema(dump_result=dump_result, schema_file=runtime_schema_file)
    finally:
        _cleanup_scratch(
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
        )

    if schema_sql is None:
        raise SchemaValidationError(error or "Unable to compute replay schema.")
    return normalize_sql_text(schema_sql), scratch_url


def _expected_schema_from_lock(
    *,
    target_name: str,
    dbmate_binary: Path,
    head_paths: ResolvedPaths,
    source_repo_root: Path,
    head_repo_root: Path,
    test_url: str | None,
    keep_scratch: bool,
    base_branch: str | None,
    on_dbmate_result: DbmateResultCallback | None,
    environ: Mapping[str, str] | None = None,
) -> tuple[str, str]:
    runtime_env = load_runtime_env(environ=environ)
    wait_timeout = _dbmate_wait_timeout(runtime_env)
    head_lock = _load_and_verify_lock(head_paths)
    engine = _resolve_replay_engine(head_lock=head_lock, test_url=test_url)
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
        base_lock = _load_and_verify_lock(base_paths)

        divergence = first_divergence_index(base=base_lock, head=head_lock)
        if divergence <= 1:
            anchor_checkpoint: Path | None = None
        else:
            anchor_step = base_lock.steps[divergence - 2]
            anchor_checkpoint = base_paths.db_dir / anchor_step.checkpoint_file
            if not anchor_checkpoint.exists():
                raise LockfileError(
                    "Unable to resolve base anchor checkpoint for divergence replay: "
                    f"{anchor_checkpoint}"
                )
        if divergence <= head_lock.head_index:
            tail_steps = head_lock.steps[divergence - 1 :]
            tail_files = migration_file_names_for_steps(tail_steps)
        else:
            tail_files = []
        return _scratch_schema_from_replay(
            target_name=target_name,
            dbmate_binary=dbmate_binary,
            engine=engine,
            head_paths=head_paths,
            test_url=test_url,
            keep_scratch=keep_scratch,
            wait_timeout=wait_timeout,
            anchor_checkpoint_file=anchor_checkpoint,
            tail_migration_file_names=tail_files,
            on_dbmate_result=on_dbmate_result,
        )


def expected_schema_from_head_lock(
    *,
    paths: ResolvedPaths,
) -> str:
    _load_and_verify_lock(paths)
    return read_schema_sql(paths.schema_file)


def _run_down_roundtrip(
    *,
    target_name: str,
    dbmate_binary: Path,
    head_paths: ResolvedPaths,
    engine: str,
    expected_base_schema_sql: str | None,
    anchor_checkpoint: Path | None,
    tail_files: list[str],
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
    retries = _scratch_retry_count(
        engine=engine,
        auto_provisioned=scratch_plan.target.auto_provisioned,
    )
    runtime_schema_dir = Path(tempfile.mkdtemp(prefix="matey-down-schema-file-"))
    runtime_schema_file = runtime_schema_dir / f"{target_name}.down.sql"
    runtime_schema_file.write_text("", encoding="utf-8")
    load_schema_file = runtime_schema_dir / f"{target_name}.down.anchor.sql"
    try:
        ready_error = _ensure_scratch_ready(
            target_name=target_name,
            phase="down",
            dbmate_binary=dbmate_binary,
            scratch_url=scratch_url,
            migrations_dir=head_paths.migrations_dir,
            runtime_schema_file=runtime_schema_file,
            engine=engine,
            retries=retries,
            wait_timeout=wait_timeout,
            wait_error="dbmate wait failed for --down validation scratch target.",
            create_error="dbmate create failed for --down validation scratch target.",
            on_dbmate_result=on_dbmate_result,
        )
        if ready_error is not None:
            return SchemaDownResult(
                target_name=target_name,
                success=False,
                scratch_url=scratch_url,
                error=ready_error,
            )

        resolved_expected_base = expected_base_schema_sql
        if resolved_expected_base is None:
            baseline_dump = _run_capture_with_retry(
                retries=retries,
                delay_seconds=1.0,
                run_step=lambda: run_dbmate_capture(
                    dbmate_binary=dbmate_binary,
                    url=scratch_url,
                    migrations_dir=head_paths.migrations_dir,
                    schema_file=runtime_schema_file,
                    verb="dump",
                    log_context=DbmateLogContext(
                        target=target_name,
                        phase="down",
                        step="dump-base",
                    ),
                    on_result=on_dbmate_result,
                ),
            )
            if baseline_dump.returncode != 0:
                details = (baseline_dump.stderr or baseline_dump.stdout or "").strip()
                return SchemaDownResult(
                    target_name=target_name,
                    success=False,
                    scratch_url=scratch_url,
                    error=f"dbmate dump failed for --down baseline. {details}".strip(),
                )
            resolved_expected_base = normalize_sql_text(
                _extract_dump_schema(dump_result=baseline_dump, schema_file=runtime_schema_file)
            )

        if anchor_checkpoint is not None:
            load_schema_file.write_text(anchor_checkpoint.read_text(encoding="utf-8"), encoding="utf-8")
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
                        phase="down",
                        step="load-anchor",
                    ),
                    on_result=on_dbmate_result,
                ),
            )
            if load_code != 0:
                return SchemaDownResult(
                    target_name=target_name,
                    success=False,
                    scratch_url=scratch_url,
                    error="dbmate load failed while loading anchor for --down validation.",
                )

        if tail_files:
            with _temporary_tail_migrations_dir(
                source_migrations_dir=head_paths.migrations_dir,
                file_names=tail_files,
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
                        log_context=DbmateLogContext(target=target_name, phase="down", step="up-tail"),
                        on_result=on_dbmate_result,
                    ),
                )
                if up_code != 0:
                    return SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        error="dbmate up failed while preparing --down validation tail.",
                    )

                rollback_code = _run_step_with_retry(
                    retries=retries,
                    delay_seconds=1.0,
                    run_step=lambda: run_dbmate(
                        dbmate_binary=dbmate_binary,
                        url=scratch_url,
                        migrations_dir=tail_dir,
                        schema_file=runtime_schema_file,
                        verb="rollback",
                        global_args=["--no-dump-schema"],
                        extra_args=[str(len(tail_files))],
                        log_context=DbmateLogContext(
                            target=target_name,
                            phase="down",
                            step="rollback-tail",
                        ),
                        on_result=on_dbmate_result,
                    ),
                )
                if rollback_code != 0:
                    return SchemaDownResult(
                        target_name=target_name,
                        success=False,
                        scratch_url=scratch_url,
                        error="dbmate rollback failed during --down validation.",
                    )

        final_dump = _run_capture_with_retry(
            retries=retries,
            delay_seconds=1.0,
            run_step=lambda: run_dbmate_capture(
                dbmate_binary=dbmate_binary,
                url=scratch_url,
                migrations_dir=head_paths.migrations_dir,
                schema_file=runtime_schema_file,
                verb="dump",
                log_context=DbmateLogContext(target=target_name, phase="down", step="dump-final"),
                on_result=on_dbmate_result,
            ),
        )
        if final_dump.returncode != 0:
            details = (final_dump.stderr or final_dump.stdout or "").strip()
            return SchemaDownResult(
                target_name=target_name,
                success=False,
                scratch_url=scratch_url,
                error=f"dbmate dump failed after --down validation rollback. {details}".strip(),
            )
        final_schema_sql = normalize_sql_text(
            _extract_dump_schema(dump_result=final_dump, schema_file=runtime_schema_file)
        )

        diff_text = schema_diff_text(
            resolved_expected_base,
            final_schema_sql,
            expected_name="base-anchor",
            actual_name="post-down",
        )
        return SchemaDownResult(
            target_name=target_name,
            success=not diff_text,
            scratch_url=scratch_url,
            diff_text=diff_text or None,
            error=(
                None
                if not diff_text
                else "Down validation failed: schema did not round-trip to base anchor."
            ),
        )
    finally:
        _cleanup_scratch(
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
        )


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
    runtime_env = load_runtime_env(environ=environ)
    wait_timeout = _dbmate_wait_timeout(runtime_env)
    source_repo_root = _resolve_repo_root(cwd)
    head_paths = paths
    head_repo_root = source_repo_root

    head_lock = _load_and_verify_lock(head_paths)
    engine = _resolve_replay_engine(head_lock=head_lock, test_url=test_url)

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
        base_lock = _load_and_verify_lock(base_paths)
        divergence = first_divergence_index(base=base_lock, head=head_lock)
        if divergence > head_lock.head_index:
            return SchemaDownResult(
                target_name=target_name,
                success=True,
                scratch_url="",
                diff_text=None,
                error=None,
            )

        tail_steps = head_lock.steps[divergence - 1 :]
        missing_down: list[str] = []
        for step in tail_steps:
            migration_file = head_paths.db_dir / step.migration_file
            if not _migration_has_down(migration_file):
                missing_down.append(step.migration_file)
        if missing_down:
            missing = ", ".join(missing_down)
            return SchemaDownResult(
                target_name=target_name,
                success=False,
                scratch_url="",
                diff_text=None,
                error=(
                    "Down validation failed: changed tail contains irreversible migrations "
                    f"(missing -- migrate:down): {missing}"
                ),
            )

        if divergence <= 1:
            anchor_checkpoint: Path | None = None
            expected_base_schema_sql: str | None = None
        else:
            anchor_step = base_lock.steps[divergence - 2]
            anchor_checkpoint = base_paths.db_dir / anchor_step.checkpoint_file
            if not anchor_checkpoint.exists():
                raise LockfileError(
                    "Unable to resolve base anchor checkpoint for down validation: "
                    f"{anchor_checkpoint}"
                )
            expected_base_schema_sql = normalize_sql_text(anchor_checkpoint.read_text(encoding="utf-8"))
        tail_files = migration_file_names_for_steps(tail_steps)
        return _run_down_roundtrip(
            target_name=target_name,
            dbmate_binary=dbmate_binary,
            head_paths=head_paths,
            engine=engine,
            expected_base_schema_sql=expected_base_schema_sql,
            anchor_checkpoint=anchor_checkpoint,
            tail_files=tail_files,
            test_url=test_url,
            keep_scratch=keep_scratch,
            wait_timeout=wait_timeout,
            on_dbmate_result=on_dbmate_result,
        )


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
    source_repo_root = _resolve_repo_root(cwd)
    head_paths = paths
    head_repo_root = source_repo_root

    head_schema_sql = read_schema_sql(head_paths.schema_file)
    head_lock = _load_and_verify_lock(head_paths)
    if clean:
        runtime_env = load_runtime_env(environ=environ)
        engine = _resolve_replay_engine(head_lock=head_lock, test_url=test_url)
        clean_result = run_clean_check(
            target_name=target_name,
            engine=engine,
            dbmate_binary=dbmate_binary,
            head_paths=head_paths,
            test_url=test_url,
            keep_scratch=keep_scratch,
            wait_timeout=_dbmate_wait_timeout(runtime_env),
            on_dbmate_result=on_dbmate_result,
        )
        if clean_result.error:
            return SchemaReplayResult(
                target_name=target_name,
                success=False,
                head_schema_sql=head_schema_sql,
                expected_schema_sql=clean_result.schema_sql or "",
                diff_text=None,
                scratch_url=clean_result.scratch_url,
                error=clean_result.error,
            )
        expected_schema_sql = normalize_sql_text(clean_result.schema_sql or "")
        scratch_url = clean_result.scratch_url
    else:
        expected_schema_sql, scratch_url = _expected_schema_from_lock(
            target_name=target_name,
            dbmate_binary=dbmate_binary,
            head_paths=head_paths,
            source_repo_root=source_repo_root,
            head_repo_root=head_repo_root,
            test_url=test_url,
            keep_scratch=keep_scratch,
            base_branch=base_branch,
            on_dbmate_result=on_dbmate_result,
            environ=environ,
        )

    diff_text = schema_diff_text(
        head_schema_sql,
        expected_schema_sql,
        expected_name=str(head_paths.schema_file),
        actual_name=f"{target_name}.replay.sql",
    )
    return SchemaReplayResult(
        target_name=target_name,
        success=not diff_text,
        head_schema_sql=head_schema_sql,
        expected_schema_sql=expected_schema_sql,
        diff_text=diff_text or None,
        scratch_url=scratch_url,
        error=None,
    )
