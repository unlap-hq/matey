from __future__ import annotations

import secrets
import tempfile
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from matey.domain import ResolvedPaths, SchemaValidationError, ScratchProvisionError
from matey.drivers.dbmate import (
    DbmateLogContext,
    DbmateResultCallback,
    run_dbmate,
    run_dbmate_capture,
)
from matey.drivers.scratch import detect_engine, plan_scratch_target
from matey.env import RuntimeEnv, load_runtime_env
from matey.workflows.schema_diff import (
    _normalize_sql,
    _read_text,
    _schema_diff,
    normalize_sql_text,
    read_schema_sql,
    schema_diff_text,
)
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
    "SchemaValidateResult",
    "dump_schema_for_url",
    "normalize_sql_text",
    "read_schema_sql",
    "schema_diff_text",
    "validate_schema_clean_target",
]


@dataclass(frozen=True)
class SchemaValidateResult:
    target_name: str
    success: bool
    scratch_url: str
    diff_text: str | None = None
    upgrade_diff_text: str | None = None
    clean_schema_sql: str | None = None
    upgrade_schema_sql: str | None = None
    scratch_urls: tuple[str, ...] = ()
    error: str | None = None


@dataclass(frozen=True)
class _ScratchRunResult:
    scratch_url: str
    schema_sql: str | None
    error: str | None


def _new_scratch_name() -> str:
    epoch = int(time.time())
    suffix = secrets.token_hex(3)
    return f"matey_{epoch}_{suffix}"


def _dbmate_wait_timeout(runtime_env: RuntimeEnv) -> str:
    return runtime_env.dbmate_wait_timeout


def _canonicalize_bigquery_scratch_schema(schema_sql: str, scratch_url: str) -> str:
    parsed = urlsplit(scratch_url)
    project = parsed.netloc
    path_parts = [part for part in parsed.path.split("/") if part]
    if not project or not path_parts:
        return schema_sql
    scratch_dataset = path_parts[-1]
    canonical_dataset = "__matey_scratch_dataset__"
    qualifier = f"{project}.{scratch_dataset}"
    canonical_qualifier = f"{project}.{canonical_dataset}"

    canonical_sql = schema_sql
    for old, new in (
        (f"`{qualifier}.", f"`{canonical_qualifier}."),
        (f"{qualifier}.", f"{canonical_qualifier}."),
        (f"`{qualifier}`", f"`{canonical_qualifier}`"),
        (qualifier, canonical_qualifier),
    ):
        canonical_sql = canonical_sql.replace(old, new)
    return canonical_sql


def dump_schema_for_url(
    *,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    url: str,
    target_name: str,
    on_dbmate_result: DbmateResultCallback | None = None,
) -> str:
    with tempfile.TemporaryDirectory(prefix="matey-live-dump-") as temp_dir_name:
        temp_dir = Path(temp_dir_name)
        temp_schema_file = temp_dir / f"{target_name}.live.sql"
        dump_result = run_dbmate_capture(
            dbmate_binary=dbmate_binary,
            url=url,
            migrations_dir=paths.migrations_dir,
            schema_file=temp_schema_file,
            verb="dump",
            log_context=DbmateLogContext(target=target_name, phase="live", step="dump"),
            on_result=on_dbmate_result,
        )
        if dump_result.returncode != 0:
            details = (dump_result.stderr or dump_result.stdout or "").strip()
            raise SchemaValidationError(f"dbmate dump failed on live target. {details}".strip())
        return _extract_dump_schema(
            dump_result=dump_result,
            schema_file=temp_schema_file,
        )


def _run_check_on_scratch(
    *,
    target_name: str,
    engine: str,
    dbmate_binary: Path,
    head_paths: ResolvedPaths,
    apply_phases: list[tuple[str, ResolvedPaths]],
    test_url: str | None,
    keep_scratch: bool,
    wait_timeout: str,
    check_name: str,
    on_dbmate_result: DbmateResultCallback | None,
) -> _ScratchRunResult:
    scratch_name = _new_scratch_name()
    scratch_plan = plan_scratch_target(engine=engine, scratch_name=scratch_name, test_url=test_url)
    scratch_url = scratch_plan.target.scratch_url
    retry_count = 12 if scratch_plan.target.auto_provisioned and engine != "sqlite" else 1

    error_message: str | None = None
    schema_sql: str | None = None

    try:
        # For BigQuery scratch datasets, wait-before-create fails by definition
        # because the dataset is created by the subsequent `create` step.
        if engine != "bigquery":
            wait_code = _run_step_with_retry(
                retries=retry_count,
                delay_seconds=1.0,
                run_step=lambda: run_dbmate(
                    dbmate_binary=dbmate_binary,
                    url=scratch_url,
                    migrations_dir=head_paths.migrations_dir,
                    schema_file=head_paths.schema_file,
                    verb="wait",
                    global_args=["--wait-timeout", wait_timeout],
                    log_context=DbmateLogContext(
                        target=target_name,
                        phase=check_name,
                        step="wait",
                    ),
                    on_result=on_dbmate_result,
                ),
            )
            if wait_code != 0:
                error_message = f"dbmate wait failed on {check_name} scratch target."
                return _ScratchRunResult(scratch_url=scratch_url, schema_sql=None, error=error_message)

        create_code = _run_step_with_retry(
            retries=retry_count,
            delay_seconds=1.0,
            run_step=lambda: run_dbmate(
                dbmate_binary=dbmate_binary,
                url=scratch_url,
                migrations_dir=head_paths.migrations_dir,
                schema_file=head_paths.schema_file,
                verb="create",
                log_context=DbmateLogContext(
                    target=target_name,
                    phase=check_name,
                    step="create",
                ),
                on_result=on_dbmate_result,
            ),
        )
        if create_code != 0:
            error_message = f"dbmate create failed on {check_name} scratch target."
            return _ScratchRunResult(scratch_url=scratch_url, schema_sql=None, error=error_message)

        for phase_name, phase_paths in apply_phases:
            up_code = _run_step_with_retry(
                retries=retry_count,
                delay_seconds=1.0,
                run_step=lambda phase_paths=phase_paths, phase_name=phase_name: run_dbmate(
                    dbmate_binary=dbmate_binary,
                    url=scratch_url,
                    migrations_dir=phase_paths.migrations_dir,
                    schema_file=phase_paths.schema_file,
                    verb="up",
                    log_context=DbmateLogContext(
                        target=target_name,
                        phase=check_name,
                        step=f"up({phase_name})",
                    ),
                    on_result=on_dbmate_result,
                ),
            )
            if up_code != 0:
                error_message = f"dbmate up failed on {check_name} scratch target ({phase_name} phase)."
                return _ScratchRunResult(scratch_url=scratch_url, schema_sql=None, error=error_message)

        dump_result = _run_capture_with_retry(
            retries=retry_count,
            delay_seconds=1.0,
            run_step=lambda: run_dbmate_capture(
                dbmate_binary=dbmate_binary,
                url=scratch_url,
                migrations_dir=head_paths.migrations_dir,
                schema_file=head_paths.schema_file,
                verb="dump",
                log_context=DbmateLogContext(
                    target=target_name,
                    phase=check_name,
                    step="dump",
                ),
                on_result=on_dbmate_result,
            ),
        )
        if dump_result.returncode != 0:
            details = (dump_result.stderr or dump_result.stdout or "").strip()
            error_message = f"dbmate dump failed on {check_name} scratch target. {details}".strip()
            return _ScratchRunResult(scratch_url=scratch_url, schema_sql=None, error=error_message)

        schema_sql = _extract_dump_schema(
            dump_result=dump_result,
            schema_file=head_paths.schema_file,
        )
    finally:
        cleanup_error: str | None = None
        if not keep_scratch and scratch_plan.target.cleanup_required:
            drop_code = run_dbmate(
                dbmate_binary=dbmate_binary,
                url=scratch_url,
                migrations_dir=head_paths.migrations_dir,
                schema_file=head_paths.schema_file,
                verb="drop",
                log_context=DbmateLogContext(
                    target=target_name,
                    phase=check_name,
                    step="drop",
                ),
                on_result=on_dbmate_result,
            )
            if drop_code != 0:
                cleanup_error = "dbmate drop failed while cleaning scratch target."
        if not keep_scratch:
            scratch_plan.cleanup()
        if cleanup_error and error_message is None:
            error_message = cleanup_error

    return _ScratchRunResult(scratch_url=scratch_url, schema_sql=schema_sql, error=error_message)


def _run_clean_check(
    *,
    target_name: str,
    engine: str,
    dbmate_binary: Path,
    head_paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    wait_timeout: str,
    on_dbmate_result: DbmateResultCallback | None,
) -> _ScratchRunResult:
    return _run_check_on_scratch(
        target_name=target_name,
        engine=engine,
        dbmate_binary=dbmate_binary,
        head_paths=head_paths,
        apply_phases=[("head", head_paths)],
        test_url=test_url,
        keep_scratch=keep_scratch,
        wait_timeout=wait_timeout,
        check_name="clean",
        on_dbmate_result=on_dbmate_result,
    )


def _run_upgrade_check(
    *,
    target_name: str,
    engine: str,
    dbmate_binary: Path,
    head_paths: ResolvedPaths,
    test_url: str | None,
    keep_scratch: bool,
    wait_timeout: str,
    base_branch: str | None,
    runtime_env: RuntimeEnv,
    on_dbmate_result: DbmateResultCallback | None,
    cwd: Path | None = None,
) -> _ScratchRunResult:
    repo_root = _resolve_repo_root(cwd)
    base_ref = _detect_base_ref(
        explicit_base_branch=base_branch,
        repo_root=repo_root,
        runtime_env=runtime_env,
    )
    merge_base = _resolve_merge_base(repo_root, base_ref)

    with _temporary_worktree(repo_root, merge_base) as worktree:
        base_migrations = _map_to_worktree(head_paths.migrations_dir, repo_root, worktree)
        base_schema = _map_to_worktree(head_paths.schema_file, repo_root, worktree)
        if not base_migrations.exists():
            base_migrations.mkdir(parents=True, exist_ok=True)
        base_schema.parent.mkdir(parents=True, exist_ok=True)
        base_paths = ResolvedPaths(
            db_dir=base_migrations.parent,
            migrations_dir=base_migrations,
            schema_file=base_schema,
        )
        return _run_check_on_scratch(
            target_name=target_name,
            engine=engine,
            dbmate_binary=dbmate_binary,
            head_paths=head_paths,
            apply_phases=[("base", base_paths), ("head", head_paths)],
            test_url=test_url,
            keep_scratch=keep_scratch,
            wait_timeout=wait_timeout,
            check_name="upgrade",
            on_dbmate_result=on_dbmate_result,
        )


def validate_schema_clean_target(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    real_url: str | None,
    test_url: str | None,
    keep_scratch: bool,
    no_repo_check: bool,
    schema_only: bool = True,
    path_only: bool = False,
    no_upgrade_diff: bool = False,
    base_branch: str | None = None,
    cwd: Path | None = None,
    on_dbmate_result: DbmateResultCallback | None = None,
    environ: Mapping[str, str] | None = None,
) -> SchemaValidateResult:
    if schema_only and path_only:
        raise SchemaValidationError("Cannot enable both schema-only and path-only modes.")

    engine_source_url = test_url or real_url
    if not engine_source_url:
        raise SchemaValidationError(
            "Cannot infer database engine for scratch validation. "
            "Set --test-url or provide a real URL via --url / configured url_env."
        )
    try:
        engine = detect_engine(engine_source_url)
    except ScratchProvisionError as error:
        raise SchemaValidationError(str(error)) from error
    if engine == "bigquery" and test_url is None:
        raise SchemaValidationError(
            "BigQuery scratch requires --test-url (or configured test_url_env)."
        )

    run_clean = schema_only or (not schema_only and not path_only)
    run_upgrade = path_only or (not schema_only and not path_only)
    runtime_env = load_runtime_env(environ=environ)
    wait_timeout = _dbmate_wait_timeout(runtime_env)

    scratch_urls: list[str] = []
    clean_schema: str | None = None
    upgrade_schema: str | None = None
    error_message: str | None = None

    if run_clean:
        clean_result = _run_clean_check(
            target_name=target_name,
            engine=engine,
            dbmate_binary=dbmate_binary,
            head_paths=paths,
            test_url=test_url,
            keep_scratch=keep_scratch,
            wait_timeout=wait_timeout,
            on_dbmate_result=on_dbmate_result,
        )
        scratch_urls.append(clean_result.scratch_url)
        error_message = clean_result.error
        clean_schema = clean_result.schema_sql

    if error_message is None and run_upgrade:
        upgrade_result = _run_upgrade_check(
            target_name=target_name,
            engine=engine,
            dbmate_binary=dbmate_binary,
            head_paths=paths,
            test_url=test_url,
            keep_scratch=keep_scratch,
            wait_timeout=wait_timeout,
            base_branch=base_branch,
            runtime_env=runtime_env,
            on_dbmate_result=on_dbmate_result,
            cwd=cwd,
        )
        scratch_urls.append(upgrade_result.scratch_url)
        error_message = upgrade_result.error
        upgrade_schema = upgrade_result.schema_sql

    diff_text: str | None = None
    upgrade_diff_text: str | None = None

    if error_message is None and run_clean and not no_repo_check:
        repo_schema = _normalize_sql(_read_text(paths.schema_file))
        diff_text = _schema_diff(
            repo_schema,
            clean_schema or "",
            expected_name=str(paths.schema_file),
            actual_name=f"{target_name}.clean.sql",
        ) or None

    if error_message is None and run_clean and run_upgrade and not no_upgrade_diff:
        clean_schema_for_diff = clean_schema or ""
        upgrade_schema_for_diff = upgrade_schema or ""
        if engine == "bigquery" and len(scratch_urls) >= 2:
            clean_schema_for_diff = _canonicalize_bigquery_scratch_schema(
                clean_schema_for_diff,
                scratch_urls[0],
            )
            upgrade_schema_for_diff = _canonicalize_bigquery_scratch_schema(
                upgrade_schema_for_diff,
                scratch_urls[1],
            )
        upgrade_diff_text = _schema_diff(
            clean_schema_for_diff,
            upgrade_schema_for_diff,
            expected_name=f"{target_name}.clean.sql",
            actual_name=f"{target_name}.upgrade.sql",
        ) or None

    success = error_message is None and diff_text is None and upgrade_diff_text is None
    primary_url = scratch_urls[0] if scratch_urls else ""
    return SchemaValidateResult(
        target_name=target_name,
        success=success,
        scratch_url=primary_url,
        diff_text=diff_text,
        upgrade_diff_text=upgrade_diff_text,
        clean_schema_sql=clean_schema,
        upgrade_schema_sql=upgrade_schema,
        scratch_urls=tuple(scratch_urls),
        error=error_message,
    )
