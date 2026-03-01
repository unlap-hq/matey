from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from matey.adapters.dbmate import (
    DbmateLogContext,
    DbmateResultCallback,
    run_dbmate,
    run_dbmate_capture,
)
from matey.core import LockfileError, ResolvedPaths, SchemaValidationError
from matey.core.diff import normalize_sql_text, schema_diff_text
from matey.core.lock import SchemaLock, doctor_schema_lock
from matey.core.replay import expected_schema_from_head_lock
from matey.services.schema import dump_schema_for_url

_STATUS_LINE_PATTERN = re.compile(r"^\[(?P<mark>[ X])\]\s+(?P<file>.+?)\s*$")
_MISSING_DB_PATTERNS = (
    re.compile(r"\bdoes not exist\b", re.IGNORECASE),
    re.compile(r"\bunknown database\b", re.IGNORECASE),
    re.compile(r"\bnot found\b", re.IGNORECASE),
    re.compile(r"\bcannot open database file\b", re.IGNORECASE),
)

__all__ = [
    "DbMutationResult",
    "LiveDiffResult",
    "guarded_mutate_live_db",
    "run_live_db_diff",
]


@dataclass(frozen=True)
class LiveDiffResult:
    target_name: str
    success: bool
    diff_text: str | None
    expected_schema_sql: str
    live_schema_sql: str
    scratch_url: str
    error: str | None = None


@dataclass(frozen=True)
class DbMutationResult:
    target_name: str
    success: bool
    error: str | None = None
    precheck_diff_text: str | None = None
    postcheck_diff_text: str | None = None


@dataclass(frozen=True)
class _StatusSnapshot:
    applied_migration_files: tuple[str, ...]
    applied_count: int


@dataclass(frozen=True)
class _LiveStatusError(SchemaValidationError):
    message: str
    missing_db: bool = False

    def __str__(self) -> str:
        return self.message


def _status_error_indicates_missing_db(details: str) -> bool:
    lowered = details.strip().lower()
    if not lowered:
        return False
    if "connection refused" in lowered:
        return False
    return any(pattern.search(lowered) is not None for pattern in _MISSING_DB_PATTERNS)


def _parse_status_output(status_output: str) -> _StatusSnapshot:
    applied: list[str] = []
    explicit_applied_count: int | None = None
    for raw_line in status_output.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _STATUS_LINE_PATTERN.match(line)
        if match is not None:
            if match.group("mark") == "X":
                applied.append(match.group("file"))
            continue
        if line.lower().startswith("applied:"):
            try:
                explicit_applied_count = int(line.split(":", 1)[1].strip())
            except ValueError:
                explicit_applied_count = None
    if explicit_applied_count is not None and explicit_applied_count != len(applied):
        raise SchemaValidationError(
            "Unable to parse dbmate status output: applied count does not match listed rows."
        )
    return _StatusSnapshot(
        applied_migration_files=tuple(applied),
        applied_count=len(applied),
    )


def _live_applied_index(
    *,
    lock: SchemaLock,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    live_url: str,
    target_name: str,
    on_dbmate_result: DbmateResultCallback | None,
    status_step: str,
) -> int:
    status_result = run_dbmate_capture(
        dbmate_binary=dbmate_binary,
        url=live_url,
        migrations_dir=paths.migrations_dir,
        schema_file=paths.schema_file,
        verb="status",
        log_context=DbmateLogContext(target=target_name, phase="live", step=status_step),
        on_result=on_dbmate_result,
    )
    if status_result.returncode != 0:
        details = (status_result.stderr or status_result.stdout or "").strip()
        raise _LiveStatusError(
            message=f"dbmate status failed on live target. {details}".strip(),
            missing_db=_status_error_indicates_missing_db(details),
        )
    snapshot = _parse_status_output(status_result.stdout or "")
    lock_files = [Path(step.migration_file).name for step in lock.steps]
    applied_files = list(snapshot.applied_migration_files)
    expected_prefix = lock_files[: len(applied_files)]
    if applied_files != expected_prefix:
        raise SchemaValidationError(
            "Live migration status does not match lockfile prefix. "
            "Run `matey db diff` to inspect drift and reconcile migration history."
        )
    return snapshot.applied_count


def _expected_schema_for_index_from_lock(
    *,
    lock: SchemaLock,
    paths: ResolvedPaths,
    index: int,
) -> str | None:
    if index < 0 or index > lock.head_index:
        raise LockfileError(
            f"Requested schema index {index} outside lock bounds 0..{lock.head_index}."
        )
    if index == 0:
        return None
    checkpoint_path = paths.db_dir / lock.steps[index - 1].checkpoint_file
    try:
        checkpoint_sql = checkpoint_path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise LockfileError(f"Missing checkpoint referenced by lock: {checkpoint_path}") from error
    return normalize_sql_text(checkpoint_sql)


def _expected_schema_for_index(
    *,
    lock: SchemaLock,
    paths: ResolvedPaths,
    index: int,
) -> str | None:
    return _expected_schema_for_index_from_lock(lock=lock, paths=paths, index=index)


def _live_applied_index_with_create_if_needed(
    *,
    lock: SchemaLock,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    live_url: str,
    target_name: str,
    on_dbmate_result: DbmateResultCallback | None,
) -> int:
    try:
        return _live_applied_index(
            lock=lock,
            dbmate_binary=dbmate_binary,
            paths=paths,
            live_url=live_url,
            target_name=target_name,
            on_dbmate_result=on_dbmate_result,
            status_step="status-pre",
        )
    except _LiveStatusError as initial_error:
        if not initial_error.missing_db:
            raise
        create_code = run_dbmate(
            dbmate_binary=dbmate_binary,
            url=live_url,
            migrations_dir=paths.migrations_dir,
            schema_file=paths.schema_file,
            verb="create",
            log_context=DbmateLogContext(target=target_name, phase="live", step="create-if-needed"),
            on_result=on_dbmate_result,
        )
        try:
            return _live_applied_index(
                lock=lock,
                dbmate_binary=dbmate_binary,
                paths=paths,
                live_url=live_url,
                target_name=target_name,
                on_dbmate_result=on_dbmate_result,
                status_step="status-pre-after-create",
            )
        except _LiveStatusError as status_after_create_error:
            if create_code != 0:
                raise SchemaValidationError(
                    "db up precheck failed; create-if-needed did not recover status. "
                    f"initial={initial_error} post-create={status_after_create_error}"
                ) from status_after_create_error
            raise


def run_live_db_diff(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    live_url: str,
    on_dbmate_result: DbmateResultCallback | None,
) -> LiveDiffResult:
    try:
        expected_schema_sql = expected_schema_from_head_lock(
            paths=paths,
        )
        live_schema_sql = dump_schema_for_url(
            dbmate_binary=dbmate_binary,
            paths=paths,
            url=live_url,
            target_name=target_name,
            on_dbmate_result=on_dbmate_result,
        )
    except (SchemaValidationError, LockfileError) as error:
        return LiveDiffResult(
            target_name=target_name,
            success=False,
            diff_text=None,
            expected_schema_sql="",
            live_schema_sql="",
            scratch_url="",
            error=str(error),
        )
    except Exception as error:
        return LiveDiffResult(
            target_name=target_name,
            success=False,
            diff_text=None,
            expected_schema_sql="",
            live_schema_sql="",
            scratch_url="",
            error=str(error),
        )

    diff_text = schema_diff_text(
        expected_schema_sql,
        live_schema_sql,
        expected_name="expected(lock)",
        actual_name="live",
    )
    return LiveDiffResult(
        target_name=target_name,
        success=not diff_text,
        diff_text=diff_text or None,
        expected_schema_sql=expected_schema_sql,
        live_schema_sql=live_schema_sql,
        scratch_url="",
        error=None,
    )


def guarded_mutate_live_db(
    *,
    target_name: str,
    dbmate_binary: Path,
    paths: ResolvedPaths,
    live_url: str,
    verb: str,
    down_steps: int | None,
    on_dbmate_result: DbmateResultCallback | None,
) -> DbMutationResult:
    try:
        lock = doctor_schema_lock(paths=paths)
        if verb == "up":
            current_index = _live_applied_index_with_create_if_needed(
                lock=lock,
                dbmate_binary=dbmate_binary,
                paths=paths,
                live_url=live_url,
                target_name=target_name,
                on_dbmate_result=on_dbmate_result,
            )
        else:
            current_index = _live_applied_index(
                lock=lock,
                dbmate_binary=dbmate_binary,
                paths=paths,
                live_url=live_url,
                target_name=target_name,
                on_dbmate_result=on_dbmate_result,
                status_step="status-pre",
            )

        expected_pre_sql = _expected_schema_for_index(lock=lock, paths=paths, index=current_index)
        if expected_pre_sql is not None:
            live_pre_sql = dump_schema_for_url(
                dbmate_binary=dbmate_binary,
                paths=paths,
                url=live_url,
                target_name=target_name,
                on_dbmate_result=on_dbmate_result,
            )
            pre_diff = schema_diff_text(
                expected_pre_sql,
                live_pre_sql,
                expected_name="expected(current)",
                actual_name="live(current)",
            )
            if pre_diff:
                return DbMutationResult(
                    target_name=target_name,
                    success=False,
                    error="Precheck failed: live database schema drifts from lockfile-expected current version.",
                    precheck_diff_text=pre_diff,
                )

        extra_args: list[str] = []
        if verb == "rollback" and down_steps is not None:
            extra_args = [str(down_steps)]

        mutate_code = run_dbmate(
            dbmate_binary=dbmate_binary,
            url=live_url,
            migrations_dir=paths.migrations_dir,
            schema_file=paths.schema_file,
            verb=verb,
            global_args=["--no-dump-schema"],
            extra_args=extra_args,
            log_context=DbmateLogContext(target=target_name, phase="live", step=f"mutate-{verb}"),
            on_result=on_dbmate_result,
        )
        if mutate_code != 0:
            return DbMutationResult(
                target_name=target_name,
                success=False,
                error=f"dbmate {verb} failed.",
            )

        after_index = _live_applied_index(
            lock=lock,
            dbmate_binary=dbmate_binary,
            paths=paths,
            live_url=live_url,
            target_name=target_name,
            on_dbmate_result=on_dbmate_result,
            status_step="status-post",
        )

        if verb in {"up", "migrate"}:
            expected_index = lock.head_index
        elif verb == "rollback":
            steps = down_steps or 1
            expected_index = max(0, current_index - steps)
        else:
            expected_index = after_index

        if after_index != expected_index:
            return DbMutationResult(
                target_name=target_name,
                success=False,
                error=(
                    f"Postcheck failed: expected applied index {expected_index} after {verb}, "
                    f"but live status is {after_index}."
                ),
            )

        expected_post_sql = _expected_schema_for_index(lock=lock, paths=paths, index=expected_index)
        if expected_post_sql is not None:
            live_post_sql = dump_schema_for_url(
                dbmate_binary=dbmate_binary,
                paths=paths,
                url=live_url,
                target_name=target_name,
                on_dbmate_result=on_dbmate_result,
            )
            post_diff = schema_diff_text(
                expected_post_sql,
                live_post_sql,
                expected_name="expected(target)",
                actual_name="live(target)",
            )
            if post_diff:
                return DbMutationResult(
                    target_name=target_name,
                    success=False,
                    error="Postcheck failed: live database schema does not match expected target version.",
                    postcheck_diff_text=post_diff,
                )
    except (LockfileError, SchemaValidationError) as error:
        return DbMutationResult(
            target_name=target_name,
            success=False,
            error=str(error),
        )
    except Exception as error:
        return DbMutationResult(
            target_name=target_name,
            success=False,
            error=str(error),
        )

    return DbMutationResult(target_name=target_name, success=True)
