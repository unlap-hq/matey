from __future__ import annotations

import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from matey.dbmate import CmdResult, DbConnection, Dbmate, DbmateError
from matey.lockfile import LockState, build_lock_state
from matey.project import TargetConfig
from matey.repo import Snapshot
from matey.scratch import engine_from_url as scratch_engine_from_url
from matey.sql import (
    SqlError,
    SqlProgram,
    decode_sql_text,
    engine_from_url,
    first_migration_violation_message,
    is_bigquery_family,
)
from matey.tx import recover_artifacts, serialized_target
from matey.zero import zero_schema_sql

_STATUS_LINE_PATTERN = re.compile(r"^\[(?P<mark>[ X])\]\s+(?P<file>.+?)\s*$")
_BIGQUERY_MISSING_DB_PATTERNS = (
    re.compile(r"\bnot found:\s*dataset\b", re.IGNORECASE),
    re.compile(r"\bdataset .* not found\b", re.IGNORECASE),
)
_MISSING_DB_PATTERNS = {
    "postgres": (
        re.compile(r'database "[^"]+" does not exist', re.IGNORECASE),
        re.compile(r"\bdatabase [^\s]+ does not exist\b", re.IGNORECASE),
    ),
    "mysql": (re.compile(r"\bunknown database\b", re.IGNORECASE),),
    "sqlite": (
        re.compile(r"\bcannot open database file\b", re.IGNORECASE),
        re.compile(r"\bunable to open database file\b", re.IGNORECASE),
    ),
    "clickhouse": (
        re.compile(r"\bdatabase [^\s]+ does not exist\b", re.IGNORECASE),
        re.compile(r"\bunknown database\b", re.IGNORECASE),
    ),
    "bigquery": _BIGQUERY_MISSING_DB_PATTERNS,
}


class DbError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    target: TargetConfig
    snapshot: Snapshot
    state: LockState
    conn: DbConnection


@dataclass(frozen=True, slots=True)
class LiveStatus:
    applied_files: tuple[str, ...]
    applied_count: int


@dataclass(frozen=True, slots=True)
class StatusError(RuntimeError):
    result: CmdResult
    missing_db: bool


@contextmanager
def open_runtime(
    *,
    target: TargetConfig,
    url: str | None,
    dbmate_bin: Path | None,
) -> Iterator[RuntimeContext]:
    with serialized_target(target.root):
        recover_artifacts(target.root)
        snapshot = Snapshot.from_worktree(target)
        state = build_lock_state(snapshot)
        if not state.is_clean:
            raise DbError(format_lock_diagnostics(state))
        live_url = resolve_live_url(target=target, url=url)
        dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=dbmate_bin)
        conn = dbmate.database(live_url)
        yield RuntimeContext(target=target, snapshot=snapshot, state=state, conn=conn)


def resolve_live_url(*, target: TargetConfig, url: str | None) -> str:
    if url is not None and url.strip():
        return url.strip()
    env_value = os.getenv(target.url_env, "").strip()
    if env_value:
        return env_value
    raise DbError(f"Live database URL is missing. Pass --url or set {target.url_env}.")


def read_status(conn: DbConnection) -> tuple[CmdResult, LiveStatus]:
    result = conn.status()
    if result.exit_code != 0:
        details = (result.stderr or result.stdout).strip()
        raise StatusError(
            result=result,
            missing_db=is_missing_db_status_error(conn.url, details),
        )
    return result, parse_status(result.stdout)


def inspect_live(runtime: RuntimeContext, *, context: str) -> LiveStatus:
    try:
        _, live = read_status(runtime.conn)
    except StatusError as error:
        raise DbError(format_command_error(context, error.result)) from error
    ensure_prefix(state=runtime.state, live=live)
    return live


def parse_status(text: str) -> LiveStatus:
    applied: list[str] = []
    explicit_applied: int | None = None
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        match = _STATUS_LINE_PATTERN.match(line)
        if match is not None:
            if match.group("mark") == "X":
                applied.append(match.group("file"))
            continue
        if line.lower().startswith("applied:"):
            count_text = line.split(":", 1)[1].strip()
            try:
                explicit_applied = int(count_text)
            except ValueError as error:
                raise DbError(
                    f"Unable to parse dbmate status applied count: {count_text!r}"
                ) from error
    if explicit_applied is not None and explicit_applied != len(applied):
        raise DbError(
            "Unable to parse dbmate status output: applied count does not match listed rows."
        )
    return LiveStatus(applied_files=tuple(applied), applied_count=len(applied))


def is_missing_db_status_error(url: str, details: str) -> bool:
    lowered = details.strip().lower()
    if not lowered:
        return False
    if "connection refused" in lowered:
        return False
    engine = engine_from_url(url)
    patterns = (
        _BIGQUERY_MISSING_DB_PATTERNS
        if is_bigquery_family(engine)
        else _MISSING_DB_PATTERNS.get(engine, ())
    )
    return any(pattern.search(lowered) is not None for pattern in patterns)


def ensure_prefix(*, state: LockState, live: LiveStatus) -> None:
    worktree_paths = tuple(step.migration_file for step in state.worktree_steps)
    expected_prefix = worktree_paths[: len(live.applied_files)]
    expected_basenames = tuple(status_basename(path) for path in expected_prefix)
    status_mode = live_status_path_mode(live.applied_files)
    if status_mode == "mixed":
        raise DbError(
            "dbmate status returned mixed path styles; cannot validate live migration prefix safely."
        )
    if status_mode == "basename" and len(set(expected_basenames)) != len(expected_basenames):
        raise DbError(
            "Cannot validate live migration prefix: applied worktree prefix has duplicate "
            "migration basenames, but dbmate status output is basename-only."
        )

    for live_entry, expected_path in zip(live.applied_files, expected_prefix, strict=False):
        if status_mode == "path" and normalize_status_path(live_entry) == normalize_status_path(
            expected_path
        ):
            continue
        if status_mode == "basename" and status_basename(live_entry) == status_basename(
            expected_path
        ):
            continue
        raise DbError("Live migration status does not match worktree migration prefix.")


def live_status_path_mode(applied_files: tuple[str, ...]) -> str:
    if not applied_files:
        return "basename"
    path_like = tuple("/" in entry or "\\" in entry for entry in applied_files)
    if all(path_like):
        return "path"
    if not any(path_like):
        return "basename"
    return "mixed"


def normalize_status_path(path: str) -> str:
    return PurePosixPath(path.replace("\\", "/")).as_posix()


def status_basename(path: str) -> str:
    return PurePosixPath(path.replace("\\", "/")).name


def live_relation(*, state: LockState, live: LiveStatus) -> str:
    head_count = len(state.worktree_steps)
    if live.applied_count < head_count:
        return "behind"
    if live.applied_count > head_count:
        return "ahead"
    return "equal"


def ensure_live_not_ahead(*, state: LockState, live: LiveStatus, context: str) -> None:
    if live_relation(state=state, live=live) != "ahead":
        return
    raise DbError(f"{context} failed: live migration status is ahead of worktree.")


def ensure_pending_up_allowed(
    *,
    runtime: RuntimeContext,
    applied_count: int,
    context: str,
) -> None:
    scheme = engine_from_url(runtime.conn.url)
    engine = scheme if is_bigquery_family(scheme) or scheme in {"mysql", "clickhouse"} else None
    if engine is None:
        return
    message = first_migration_violation_message(
        entries=(
            (
                step.migration_file,
                migration_payload(runtime=runtime, migration_file=step.migration_file),
            )
            for step in runtime.state.worktree_steps[applied_count:]
        ),
        engine=engine,
        section="up",
        context=context,
    )
    if message is not None:
        raise DbError(message)


def ensure_rollback_allowed(
    *,
    runtime: RuntimeContext,
    applied_count: int,
    steps: int,
    context: str,
) -> None:
    scheme = engine_from_url(runtime.conn.url)
    engine = scheme if is_bigquery_family(scheme) or scheme in {"mysql", "clickhouse"} else None
    if engine is None or applied_count <= 0:
        return
    start = max(applied_count - steps, 0)
    message = first_migration_violation_message(
        entries=(
            (
                step.migration_file,
                migration_payload(runtime=runtime, migration_file=step.migration_file),
            )
            for step in runtime.state.worktree_steps[start:applied_count]
        ),
        engine=engine,
        section="down",
        context=context,
    )
    if message is not None:
        raise DbError(message)


def migration_payload(*, runtime: RuntimeContext, migration_file: str) -> bytes:
    payload = runtime.snapshot.migrations.get(migration_file)
    if payload is None:
        raise DbError(f"Missing migration payload for {migration_file}.")
    return payload


def verify_expected_schema(
    *,
    runtime: RuntimeContext,
    expected_index: int,
    context: str,
) -> bool | None:
    schema_match, _expected_sql, _live_sql = compare_expected_schema(
        runtime=runtime,
        expected_index=expected_index,
        context=context,
    )
    if schema_match is False:
        raise DbError(f"{context} failed: live schema differs from expected schema.")
    return schema_match


def compare_expected_schema(
    *,
    runtime: RuntimeContext,
    expected_index: int,
    context: str,
) -> tuple[bool | None, str | None, str | None]:
    expected_sql = expected_sql_for_index(runtime=runtime, index=expected_index)
    if expected_sql is None:
        return None, None, None

    live_sql = dump_live_schema(runtime.conn, context=f"{context} dump")
    engine = engine_from_url(runtime.conn.url)
    try:
        schema_match = SqlProgram(expected_sql, engine=engine).schema_equals(
            SqlProgram(live_sql, engine=engine),
            left_context_url=runtime.conn.url,
            right_context_url=runtime.conn.url,
        )
    except SqlError as error:
        raise DbError(f"SQL analysis failed while comparing expected schema: {error}") from error
    if schema_match:
        return True, expected_sql, live_sql
    return False, expected_sql, live_sql


def expected_sql_for_index(*, runtime: RuntimeContext, index: int) -> str | None:
    target_index = len(runtime.state.worktree_steps)
    if index < 0 or index > target_index:
        raise DbError(
            f"Expected schema index {index} is outside worktree lock range 0..{target_index}."
        )
    if index == 0:
        return zero_schema_sql(engine=scratch_engine_from_url(runtime.conn.url))
    if index == target_index:
        if runtime.snapshot.schema_sql is None:
            raise DbError("Worktree schema.sql is missing.")
        return decode_sql_text(runtime.snapshot.schema_sql, label="worktree schema.sql")

    step = runtime.state.worktree_steps[index - 1]
    checkpoint_sql = runtime.snapshot.checkpoints.get(step.checkpoint_file)
    if checkpoint_sql is None:
        raise DbError(f"Missing checkpoint for expected index {index}: {step.checkpoint_file}.")
    return decode_sql_text(checkpoint_sql, label=f"checkpoint {step.checkpoint_file}")


def dump_live_schema(conn: DbConnection, *, context: str) -> str:
    try:
        dump_result = conn.dump()
    except DbmateError as error:
        raise DbError(f"{context} failed: {error}") from error
    if dump_result.exit_code != 0:
        raise DbError(format_command_error(context, dump_result))
    return dump_result.stdout


def format_command_error(context: str, result: CmdResult) -> str:
    return (
        f"{context} failed (exit_code={result.exit_code}): "
        f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
    )


def format_lock_diagnostics(state: LockState) -> str:
    details = "; ".join(
        f"{diag.code.value}@{diag.path}: {diag.detail}" for diag in state.diagnostics
    )
    return (
        f"Worktree schema artifacts are not clean; refusing live DB command. Diagnostics: {details}"
    )


__all__ = [
    "DbError",
    "LiveStatus",
    "RuntimeContext",
    "StatusError",
    "compare_expected_schema",
    "ensure_live_not_ahead",
    "ensure_pending_up_allowed",
    "ensure_prefix",
    "ensure_rollback_allowed",
    "expected_sql_for_index",
    "format_command_error",
    "format_lock_diagnostics",
    "inspect_live",
    "is_missing_db_status_error",
    "live_relation",
    "live_status_path_mode",
    "migration_payload",
    "open_runtime",
    "parse_status",
    "read_status",
    "resolve_live_url",
    "verify_expected_schema",
]
