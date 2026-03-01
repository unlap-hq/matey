from __future__ import annotations

import shutil
from collections.abc import Callable
from pathlib import Path

from matey.adapters.dbmate import DbmateLogContext, DbmateResultCallback, run_dbmate
from matey.adapters.fs import _run_step_with_retry

__all__ = [
    "cleanup_scratch",
    "ensure_scratch_ready",
    "scratch_retry_count",
]


def scratch_retry_count(*, engine: str, auto_provisioned: bool) -> int:
    return 12 if auto_provisioned and engine != "sqlite" else 1


def ensure_scratch_ready(
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
    run_dbmate_fn: Callable[..., int] = run_dbmate,
) -> str | None:
    if engine != "bigquery":
        wait_code = _run_step_with_retry(
            retries=retries,
            delay_seconds=1.0,
            run_step=lambda: run_dbmate_fn(
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
        run_step=lambda: run_dbmate_fn(
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


def cleanup_scratch(
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
    drop_error: str,
    run_dbmate_fn: Callable[..., int] = run_dbmate,
) -> str | None:
    error_parts: list[str] = []
    if not keep_scratch and cleanup_required:
        drop_code = run_dbmate_fn(
            dbmate_binary=dbmate_binary,
            url=scratch_url,
            migrations_dir=migrations_dir,
            schema_file=runtime_schema_file,
            verb="drop",
            log_context=DbmateLogContext(target=target_name, phase=phase, step="drop"),
            on_result=on_dbmate_result,
        )
        if drop_code != 0:
            error_parts.append(drop_error)

    if not keep_scratch:
        try:
            cleanup()
        except Exception as error:  # pragma: no cover - defensive fallback
            error_parts.append(f"Scratch cleanup callback failed: {error}")

    shutil.rmtree(runtime_schema_dir, ignore_errors=True)
    if not error_parts:
        return None
    return " ".join(error_parts)
