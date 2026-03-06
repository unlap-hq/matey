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
from matey.snapshot import Snapshot
from matey.sql import (
    SqlProgram,
    WriteViolation,
    compare_schema_sql,
    engine_from_url,
    schema_sql_diff,
)
from matey.tx import TxError, recover_artifacts, serialized_target

_STATUS_LINE_PATTERN = re.compile(r"^\[(?P<mark>[ X])\]\s+(?P<file>.+?)\s*$")
_MISSING_DB_PATTERNS = (
    re.compile(r"\bdoes not exist\b", re.IGNORECASE),
    re.compile(r"\bunknown database\b", re.IGNORECASE),
    re.compile(r"\bnot found\b", re.IGNORECASE),
    re.compile(r"\bcannot open database file\b", re.IGNORECASE),
    re.compile(r"\bunable to open database file\b", re.IGNORECASE),
)


class DbError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class MutationResult:
    target_name: str
    before_index: int
    after_index: int


@dataclass(frozen=True, slots=True)
class DriftResult:
    target_name: str
    applied_index: int
    drifted: bool


@dataclass(frozen=True, slots=True)
class PlanResult:
    target_name: str
    applied_index: int
    target_index: int
    matches: bool


@dataclass(frozen=True, slots=True)
class _Ctx:
    target: TargetConfig
    snapshot: Snapshot
    state: LockState
    conn: DbConnection


@dataclass(frozen=True, slots=True)
class _ParsedStatus:
    applied_files: tuple[str, ...]
    applied_count: int


@dataclass(frozen=True, slots=True)
class _StatusError(RuntimeError):
    result: CmdResult
    missing_db: bool


def status_raw(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> CmdResult:
    live_url = _resolve_live_url(target=target, url=url)
    dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=dbmate_bin)
    return dbmate.database(live_url).status()


def new(
    target: TargetConfig,
    *,
    name: str,
    dbmate_bin: Path | None = None,
) -> CmdResult:
    migration_name = name.strip()
    if not migration_name:
        raise DbError("Migration name is required.")
    dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=dbmate_bin)
    return dbmate.new(migration_name)


def up(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    with _open_ctx(target=target, url=url, dbmate_bin=dbmate_bin) as ctx:
        validated_pending_up = False
        try:
            _, parsed_before = _status(ctx.conn)
        except _StatusError as error:
            if not error.missing_db:
                raise DbError(_format_command_error("db up pre-status", error.result)) from error
            _validate_pending_up_targets(
                ctx=ctx,
                applied_count=0,
                context="db up precheck",
            )
            validated_pending_up = True
            create_result = ctx.conn.create()
            _require_success(create_result, context="db up create-if-needed")
            _, parsed_before = _status_checked(
                ctx.conn, context="db up pre-status after create"
            )

        _ensure_prefix(state=ctx.state, parsed=parsed_before)
        _ensure_live_not_ahead(
            state=ctx.state,
            parsed=parsed_before,
            context="db up pre-status",
        )
        if not validated_pending_up:
            _validate_pending_up_targets(
                ctx=ctx,
                applied_count=parsed_before.applied_count,
                context="db up precheck",
            )
        command_result = ctx.conn.up()
        _require_success(command_result, context="db up")
        _, parsed_after = _status_checked(ctx.conn, context="db up post-status")
        _ensure_prefix(state=ctx.state, parsed=parsed_after)
        _ensure_live_not_ahead(
            state=ctx.state,
            parsed=parsed_after,
            context="db up post-status",
        )

        expected_index = len(ctx.state.worktree_steps)
        _verify_expected_schema(
            ctx=ctx,
            expected_index=expected_index,
            context="db up postcheck",
        )
        return MutationResult(
            target_name=target.name,
            before_index=parsed_before.applied_count,
            after_index=parsed_after.applied_count,
        )


def migrate(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    with _open_ctx(target=target, url=url, dbmate_bin=dbmate_bin) as ctx:
        conn_url = getattr(ctx.conn, "url", "")
        if _is_bigquery_url(conn_url):
            _ensure_bigquery_dataset_exists(
                conn=ctx.conn,
                context="db migrate pre-status",
            )
        _, parsed_before = _status_checked(ctx.conn, context="db migrate pre-status")
        _ensure_prefix(state=ctx.state, parsed=parsed_before)
        _ensure_live_not_ahead(
            state=ctx.state,
            parsed=parsed_before,
            context="db migrate pre-status",
        )
        _validate_pending_up_targets(
            ctx=ctx,
            applied_count=parsed_before.applied_count,
            context="db migrate precheck",
        )

        command_result = ctx.conn.migrate()
        _require_success(command_result, context="db migrate")
        _, parsed_after = _status_checked(ctx.conn, context="db migrate post-status")
        _ensure_prefix(state=ctx.state, parsed=parsed_after)
        _ensure_live_not_ahead(
            state=ctx.state,
            parsed=parsed_after,
            context="db migrate post-status",
        )

        expected_index = len(ctx.state.worktree_steps)
        _verify_expected_schema(
            ctx=ctx,
            expected_index=expected_index,
            context="db migrate postcheck",
        )
        return MutationResult(
            target_name=target.name,
            before_index=parsed_before.applied_count,
            after_index=parsed_after.applied_count,
        )


def down(
    target: TargetConfig,
    *,
    steps: int = 1,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    if steps <= 0:
        raise ValueError("down steps must be greater than zero.")

    with _open_ctx(target=target, url=url, dbmate_bin=dbmate_bin) as ctx:
        _, parsed_before = _status_checked(ctx.conn, context="db down pre-status")
        _ensure_prefix(state=ctx.state, parsed=parsed_before)
        _ensure_live_not_ahead(
            state=ctx.state,
            parsed=parsed_before,
            context="db down pre-status",
        )
        _validate_rollback_down_targets(
            ctx=ctx,
            applied_count=parsed_before.applied_count,
            steps=steps,
            context="db down precheck",
        )

        command_result = ctx.conn.rollback(steps)
        _require_success(command_result, context=f"db down ({steps})")
        _, parsed_after = _status_checked(ctx.conn, context="db down post-status")
        _ensure_prefix(state=ctx.state, parsed=parsed_after)
        _ensure_live_not_ahead(
            state=ctx.state,
            parsed=parsed_after,
            context="db down post-status",
        )

        expected_index = parsed_after.applied_count
        _verify_expected_schema(
            ctx=ctx,
            expected_index=expected_index,
            context="db down postcheck",
        )
        return MutationResult(
            target_name=target.name,
            before_index=parsed_before.applied_count,
            after_index=parsed_after.applied_count,
        )


def drift(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> DriftResult:
    with _open_ctx(target=target, url=url, dbmate_bin=dbmate_bin) as ctx:
        parsed_before = _status_prefix_checked(ctx, context="db drift status")
        if _live_relation(state=ctx.state, parsed=parsed_before) == "ahead":
            return DriftResult(
                target_name=target.name,
                applied_index=parsed_before.applied_count,
                drifted=True,
            )

        expected_index = parsed_before.applied_count
        schema_match, _, _ = _compare_expected_schema(
            ctx=ctx,
            expected_index=expected_index,
        )
        return DriftResult(
            target_name=target.name,
            applied_index=parsed_before.applied_count,
            drifted=(schema_match is False),
        )


def plan(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> PlanResult:
    with _open_ctx(target=target, url=url, dbmate_bin=dbmate_bin) as ctx:
        parsed_status = _status_prefix_checked(ctx, context="db plan status")
        expected_index = len(ctx.state.worktree_steps)
        if _live_relation(state=ctx.state, parsed=parsed_status) == "ahead":
            return PlanResult(
                target_name=target.name,
                applied_index=parsed_status.applied_count,
                target_index=expected_index,
                matches=False,
            )
        schema_match, _, _ = _compare_expected_schema(
            ctx=ctx,
            expected_index=expected_index,
        )
        return PlanResult(
            target_name=target.name,
            applied_index=parsed_status.applied_count,
            target_index=expected_index,
            matches=(schema_match is not False),
        )


def plan_sql(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> str:
    with _open_ctx(target=target, url=url, dbmate_bin=dbmate_bin) as ctx:
        _ = _status_prefix_checked(ctx, context="db plan sql status")
        target_index = len(ctx.state.worktree_steps)
        expected_sql = _expected_sql_for_index(ctx=ctx, index=target_index)
        return expected_sql or ""


def plan_diff(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> str:
    with _open_ctx(target=target, url=url, dbmate_bin=dbmate_bin) as ctx:
        _ = _status_prefix_checked(ctx, context="db plan diff status")
        expected_index = len(ctx.state.worktree_steps)
        _, expected_sql, live_sql = _compare_expected_schema(
            ctx=ctx,
            expected_index=expected_index,
        )
        return _unified_sql_diff(
            left_sql=live_sql or "",
            right_sql=expected_sql or "",
            left_label="live/schema.sql",
            right_label="expected/worktree.sql",
            engine=engine_from_url(ctx.conn.url),
            context_url=ctx.conn.url,
        )


@contextmanager
def _open_ctx(
    *,
    target: TargetConfig,
    url: str | None,
    dbmate_bin: Path | None,
) -> Iterator[_Ctx]:
    with serialized_target(target.dir):
        _recover_target_artifacts(target)
        snapshot = Snapshot.from_worktree(target)
        state = build_lock_state(snapshot)
        if not state.is_clean:
            raise DbError(_format_lock_diagnostics(state))
        live_url = _resolve_live_url(target=target, url=url)
        dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=dbmate_bin)
        conn = dbmate.database(live_url)
        yield _Ctx(target=target, snapshot=snapshot, state=state, conn=conn)


def _recover_target_artifacts(target: TargetConfig) -> None:
    try:
        recover_artifacts(target.dir)
    except TxError as error:
        raise DbError(f"db: artifact recovery failed: {error}") from error


def _resolve_live_url(*, target: TargetConfig, url: str | None) -> str:
    if url is not None and url.strip():
        return url.strip()
    env_value = os.getenv(target.url_env, "").strip()
    if env_value:
        return env_value
    raise DbError(
        f"Live database URL is missing. Pass --url or set {target.url_env}."
    )


def _status(conn: DbConnection) -> tuple[CmdResult, _ParsedStatus]:
    result = conn.status()
    if result.exit_code != 0:
        details = (result.stderr or result.stdout).strip()
        raise _StatusError(
            result=result,
            missing_db=_is_missing_db_status_error(details),
        )
    return result, _parse_status(result.stdout)


def _status_checked(conn: DbConnection, *, context: str) -> tuple[CmdResult, _ParsedStatus]:
    try:
        return _status(conn)
    except _StatusError as error:
        raise DbError(_format_command_error(context, error.result)) from error


def _parse_status(text: str) -> _ParsedStatus:
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
    return _ParsedStatus(applied_files=tuple(applied), applied_count=len(applied))


def _is_missing_db_status_error(details: str) -> bool:
    lowered = details.strip().lower()
    if not lowered:
        return False
    if "connection refused" in lowered:
        return False
    return any(pattern.search(lowered) is not None for pattern in _MISSING_DB_PATTERNS)


def _ensure_prefix(*, state: LockState, parsed: _ParsedStatus) -> None:
    worktree_paths = tuple(step.migration_file for step in state.worktree_steps)
    worktree_basenames = tuple(Path(path).name for path in worktree_paths)
    status_is_basename_only = any(
        "/" not in entry and "\\" not in entry for entry in parsed.applied_files
    )
    if status_is_basename_only and len(set(worktree_basenames)) != len(worktree_basenames):
        raise DbError(
            "Cannot validate live migration prefix: worktree has duplicate migration basenames, "
            "but dbmate status output is basename-only."
        )

    expected_prefix = worktree_paths[: len(parsed.applied_files)]
    for live_entry, expected_path in zip(parsed.applied_files, expected_prefix, strict=False):
        if live_entry == expected_path:
            continue
        if Path(live_entry).name == Path(expected_path).name:
            continue
        raise DbError("Live migration status does not match worktree migration prefix.")


def _live_relation(*, state: LockState, parsed: _ParsedStatus) -> str:
    head_count = len(state.worktree_steps)
    if parsed.applied_count < head_count:
        return "behind"
    if parsed.applied_count > head_count:
        return "ahead"
    return "equal"


def _ensure_live_not_ahead(*, state: LockState, parsed: _ParsedStatus, context: str) -> None:
    if _live_relation(state=state, parsed=parsed) != "ahead":
        return
    raise DbError(f"{context} failed: live migration status is ahead of worktree.")


def _validate_pending_up_targets(*, ctx: _Ctx, applied_count: int, context: str) -> None:
    engine = _migration_guard_engine(ctx.conn.url)
    if engine is None:
        return
    for step in ctx.state.worktree_steps[applied_count:]:
        program = SqlProgram(
            _migration_sql(ctx=ctx, migration_file=step.migration_file),
            engine=engine,
        )
        _raise_on_write_violations(
            migration_file=step.migration_file,
            engine=engine,
            violations=program.section_write_violations("up"),
            context=context,
        )


def _validate_rollback_down_targets(
    *,
    ctx: _Ctx,
    applied_count: int,
    steps: int,
    context: str,
) -> None:
    engine = _migration_guard_engine(ctx.conn.url)
    if engine is None or applied_count <= 0:
        return
    start = max(applied_count - steps, 0)
    for step in ctx.state.worktree_steps[start:applied_count]:
        program = SqlProgram(
            _migration_sql(ctx=ctx, migration_file=step.migration_file),
            engine=engine,
        )
        _raise_on_write_violations(
            migration_file=step.migration_file,
            engine=engine,
            violations=program.section_write_violations("down"),
            context=context,
        )


def _migration_guard_engine(url: str) -> str | None:
    scheme = urlsplit(url).scheme.lower().split("+", 1)[0]
    if scheme == "postgresql":
        scheme = "postgres"
    return scheme if scheme in {"bigquery", "mysql", "clickhouse"} else None


def _migration_sql(*, ctx: _Ctx, migration_file: str) -> str:
    payload = ctx.snapshot.migrations.get(migration_file)
    if payload is None:
        raise DbError(f"Missing migration payload for {migration_file}.")
    return payload.decode("utf-8")


def _raise_on_write_violations(
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


def _verify_expected_schema(
    *,
    ctx: _Ctx,
    expected_index: int,
    context: str,
) -> bool | None:
    schema_match, _expected_sql, _live_sql = _compare_expected_schema(
        ctx=ctx,
        expected_index=expected_index,
    )
    if schema_match is False:
        raise DbError(f"{context} failed: live schema differs from expected schema.")
    return schema_match


def _compare_expected_schema(
    *,
    ctx: _Ctx,
    expected_index: int,
) -> tuple[bool | None, str | None, str | None]:
    expected_sql = _expected_sql_for_index(ctx=ctx, index=expected_index)
    if expected_sql is None:
        return None, None, None

    dump_result = ctx.conn.dump()
    _require_success(dump_result, context="db dump")
    live_sql = dump_result.stdout
    engine = engine_from_url(ctx.conn.url)
    schema_match = compare_schema_sql(
        expected_sql,
        live_sql,
        engine=engine,
        left_context_url=ctx.conn.url,
        right_context_url=ctx.conn.url,
    )
    if schema_match:
        return True, expected_sql, live_sql
    return False, expected_sql, live_sql


def _status_prefix_checked(ctx: _Ctx, *, context: str) -> _ParsedStatus:
    _, parsed = _status_checked(ctx.conn, context=context)
    _ensure_prefix(state=ctx.state, parsed=parsed)
    return parsed


def _is_bigquery_url(url: str) -> bool:
    scheme = urlsplit(url).scheme.lower().split("+", 1)[0]
    return scheme == "bigquery"


def _ensure_bigquery_dataset_exists(*, conn: DbConnection, context: str) -> None:
    dump_result = conn.dump()
    if dump_result.exit_code == 0:
        return
    raise DbError(_format_command_error(context, dump_result))


def _expected_sql_for_index(*, ctx: _Ctx, index: int) -> str | None:
    target_index = len(ctx.state.worktree_steps)
    if index < 0 or index > target_index:
        raise DbError(
            f"Expected schema index {index} is outside worktree lock range 0..{target_index}."
        )
    if index == 0:
        return None
    if index == target_index:
        if ctx.snapshot.schema_sql is None:
            raise DbError("Worktree schema.sql is missing.")
        return ctx.snapshot.schema_sql.decode("utf-8")

    step = ctx.state.worktree_steps[index - 1]
    checkpoint_sql = ctx.snapshot.checkpoints.get(step.checkpoint_file)
    if checkpoint_sql is None:
        raise DbError(
            f"Missing checkpoint for expected index {index}: {step.checkpoint_file}."
        )
    return checkpoint_sql.decode("utf-8")


def _unified_sql_diff(
    *,
    left_sql: str,
    right_sql: str,
    left_label: str,
    right_label: str,
    engine: str,
    context_url: str | None,
) -> str:
    return schema_sql_diff(
        left_sql,
        right_sql,
        engine=engine,
        left_label=left_label,
        right_label=right_label,
        left_context_url=context_url,
        right_context_url=context_url,
    )


def _require_success(result: CmdResult, *, context: str) -> None:
    if result.exit_code == 0:
        return
    raise DbError(_format_command_error(context, result))


def _format_command_error(context: str, result: CmdResult) -> str:
    return (
        f"{context} failed (exit_code={result.exit_code}): "
        f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
    )


def _format_lock_diagnostics(state: LockState) -> str:
    details = "; ".join(
        f"{diag.code.value}@{diag.path}: {diag.detail}" for diag in state.diagnostics
    )
    return (
        "Worktree schema artifacts are not clean; refusing live DB command. "
        f"Diagnostics: {details}"
    )


__all__ = [
    "DbError",
    "DriftResult",
    "MutationResult",
    "PlanResult",
    "down",
    "drift",
    "migrate",
    "new",
    "plan",
    "plan_diff",
    "plan_sql",
    "status_raw",
    "up",
]
