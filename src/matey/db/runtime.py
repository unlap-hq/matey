from __future__ import annotations

import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit

from matey.config import TargetConfig
from matey.dbmate import CmdResult, DbConnection, Dbmate
from matey.lockfile import LockState, build_lock_state
from matey.repo import Snapshot
from matey.sql import (
    SqlError,
    SqlProgram,
    SqlTextDecodeError,
    WriteViolation,
    decode_sql_text,
    engine_from_url,
)
from matey.tx import TxError, recover_artifacts, serialized_target

_STATUS_LINE_PATTERN = re.compile(r"^\[(?P<mark>[ X])\]\s+(?P<file>.+?)\s*$")
_MISSING_DB_PATTERNS = {
    "postgres": (
        re.compile(r'database "[^"]+" does not exist', re.IGNORECASE),
        re.compile(r"\bdatabase [^\s]+ does not exist\b", re.IGNORECASE),
    ),
    "mysql": (
        re.compile(r"\bunknown database\b", re.IGNORECASE),
    ),
    "sqlite": (
        re.compile(r"\bcannot open database file\b", re.IGNORECASE),
        re.compile(r"\bunable to open database file\b", re.IGNORECASE),
    ),
    "clickhouse": (
        re.compile(r"\bdatabase [^\s]+ does not exist\b", re.IGNORECASE),
        re.compile(r"\bunknown database\b", re.IGNORECASE),
    ),
    "bigquery": (
        re.compile(r"\bnot found:\s*dataset\b", re.IGNORECASE),
        re.compile(r"\bdataset .* not found\b", re.IGNORECASE),
    ),
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
    with serialized_target(target.dir):
        recover_target(target)
        snapshot = Snapshot.from_worktree(target)
        state = build_lock_state(snapshot)
        if not state.is_clean:
            raise DbError(format_lock_diagnostics(state))
        live_url = resolve_live_url(target=target, url=url)
        dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=dbmate_bin)
        conn = dbmate.database(live_url)
        yield RuntimeContext(target=target, snapshot=snapshot, state=state, conn=conn)


def recover_target(target: TargetConfig) -> None:
    try:
        recover_artifacts(target.dir)
    except TxError as error:
        raise DbError(f"db: artifact recovery failed: {error}") from error


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


def read_status_checked(conn: DbConnection, *, context: str) -> tuple[CmdResult, LiveStatus]:
    try:
        return read_status(conn)
    except StatusError as error:
        raise DbError(format_command_error(context, error.result)) from error


def inspect_live(runtime: RuntimeContext, *, context: str) -> LiveStatus:
    _, live = read_status_checked(runtime.conn, context=context)
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
                raise DbError(f"Unable to parse dbmate status applied count: {count_text!r}") from error
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
    patterns = _MISSING_DB_PATTERNS.get(engine, ())
    return any(pattern.search(lowered) is not None for pattern in patterns)


def ensure_prefix(*, state: LockState, live: LiveStatus) -> None:
    worktree_paths = tuple(step.migration_file for step in state.worktree_steps)
    expected_prefix = worktree_paths[: len(live.applied_files)]
    expected_basenames = tuple(Path(path).name for path in expected_prefix)
    status_is_basename_only = any(
        "/" not in entry and "\\" not in entry for entry in live.applied_files
    )
    if status_is_basename_only and len(set(expected_basenames)) != len(expected_basenames):
        raise DbError(
            "Cannot validate live migration prefix: applied worktree prefix has duplicate "
            "migration basenames, but dbmate status output is basename-only."
        )

    for live_entry, expected_path in zip(live.applied_files, expected_prefix, strict=False):
        if live_entry == expected_path:
            continue
        if Path(live_entry).name == Path(expected_path).name:
            continue
        raise DbError("Live migration status does not match worktree migration prefix.")


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
    engine = migration_guard_engine(runtime.conn.url)
    if engine is None:
        return
    for step in runtime.state.worktree_steps[applied_count:]:
        try:
            program = SqlProgram(
                migration_sql(runtime=runtime, migration_file=step.migration_file),
                engine=engine,
            )
            violations = program.section_write_violations("up")
        except SqlError as error:
            raise DbError(
                f"{context} failed: SQL analysis failed for {step.migration_file}: {error}"
            ) from error
        raise_on_write_violations(
            migration_file=step.migration_file,
            engine=engine,
            violations=violations,
            context=context,
        )


def ensure_rollback_allowed(
    *,
    runtime: RuntimeContext,
    applied_count: int,
    steps: int,
    context: str,
) -> None:
    engine = migration_guard_engine(runtime.conn.url)
    if engine is None or applied_count <= 0:
        return
    start = max(applied_count - steps, 0)
    for step in runtime.state.worktree_steps[start:applied_count]:
        try:
            program = SqlProgram(
                migration_sql(runtime=runtime, migration_file=step.migration_file),
                engine=engine,
            )
            violations = program.section_write_violations("down")
        except SqlError as error:
            raise DbError(
                f"{context} failed: SQL analysis failed for {step.migration_file}: {error}"
            ) from error
        raise_on_write_violations(
            migration_file=step.migration_file,
            engine=engine,
            violations=violations,
            context=context,
        )


def migration_guard_engine(url: str) -> str | None:
    scheme = urlsplit(url).scheme.lower().split("+", 1)[0]
    if scheme == "postgresql":
        scheme = "postgres"
    return scheme if scheme in {"bigquery", "mysql", "clickhouse"} else None


def migration_sql(*, runtime: RuntimeContext, migration_file: str) -> str:
    payload = runtime.snapshot.migrations.get(migration_file)
    if payload is None:
        raise DbError(f"Missing migration payload for {migration_file}.")
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise DbError(f"Unable to decode migration {migration_file} as UTF-8.") from error


def raise_on_write_violations(
    *,
    migration_file: str,
    engine: str,
    violations: tuple[WriteViolation, ...],
    context: str,
) -> None:
    if not violations:
        return
    violation = violations[0]
    if violation.reason == "qualified write target":
        reason = f"qualified {engine} write target"
    else:
        reason = f"unsupported {engine} mutating syntax"
    raise DbError(
        f"{context} failed: {migration_file} {violation.section} contains a {reason} "
        f"{violation.target!r}. Use unqualified target-local names or "
        f"split this into another matey target. Statement: {violation.excerpt()!r}"
    )


def verify_expected_schema(
    *,
    runtime: RuntimeContext,
    expected_index: int,
    context: str,
) -> bool | None:
    schema_match, _expected_sql, _live_sql = compare_expected_schema(
        runtime=runtime,
        expected_index=expected_index,
    )
    if schema_match is False:
        raise DbError(f"{context} failed: live schema differs from expected schema.")
    return schema_match


def compare_expected_schema(
    *,
    runtime: RuntimeContext,
    expected_index: int,
) -> tuple[bool | None, str | None, str | None]:
    expected_sql = expected_sql_for_index(runtime=runtime, index=expected_index)
    if expected_sql is None:
        return None, None, None

    dump_result = runtime.conn.dump()
    require_success(dump_result, context="db dump")
    live_sql = dump_result.stdout
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
        return None
    if index == target_index:
        if runtime.snapshot.schema_sql is None:
            raise DbError("Worktree schema.sql is missing.")
        try:
            return decode_sql_text(runtime.snapshot.schema_sql, label="worktree schema.sql")
        except SqlTextDecodeError as error:
            raise DbError(str(error)) from error

    step = runtime.state.worktree_steps[index - 1]
    checkpoint_sql = runtime.snapshot.checkpoints.get(step.checkpoint_file)
    if checkpoint_sql is None:
        raise DbError(
            f"Missing checkpoint for expected index {index}: {step.checkpoint_file}."
        )
    try:
        return decode_sql_text(checkpoint_sql, label=f"checkpoint {step.checkpoint_file}")
    except SqlTextDecodeError as error:
        raise DbError(str(error)) from error


def is_bigquery_url(url: str) -> bool:
    scheme = urlsplit(url).scheme.lower().split("+", 1)[0]
    return scheme == "bigquery"


def ensure_bigquery_dataset_exists(*, conn: DbConnection, context: str) -> None:
    dump_result = conn.dump()
    if dump_result.exit_code == 0:
        return
    raise DbError(format_command_error(context, dump_result))


def require_success(result: CmdResult, *, context: str) -> None:
    if result.exit_code == 0:
        return
    raise DbError(format_command_error(context, result))


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
        "Worktree schema artifacts are not clean; refusing live DB command. "
        f"Diagnostics: {details}"
    )


__all__ = [
    "DbError",
    "LiveStatus",
    "RuntimeContext",
    "StatusError",
    "compare_expected_schema",
    "ensure_bigquery_dataset_exists",
    "ensure_live_not_ahead",
    "ensure_pending_up_allowed",
    "ensure_prefix",
    "ensure_rollback_allowed",
    "expected_sql_for_index",
    "format_command_error",
    "format_lock_diagnostics",
    "inspect_live",
    "is_bigquery_url",
    "is_missing_db_status_error",
    "live_relation",
    "migration_guard_engine",
    "migration_sql",
    "open_runtime",
    "parse_status",
    "raise_on_write_violations",
    "read_status",
    "read_status_checked",
    "recover_target",
    "require_success",
    "resolve_live_url",
    "verify_expected_schema",
]
