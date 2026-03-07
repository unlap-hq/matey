from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar

from matey.config import TargetConfig
from matey.dbmate import CmdResult, Dbmate
from matey.paths import PathBoundaryError, describe_path_boundary_error, safe_descendant
from matey.sql import SqlError, SqlProgram, unified_sql_diff

from . import runtime
from .runtime import DbError

T = TypeVar("T")


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


def status_raw(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> CmdResult:
    live_url = runtime.resolve_live_url(target=target, url=url)
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
        raise runtime.DbError("Migration name is required.")
    try:
        migrations_dir = safe_descendant(
            root=target.dir,
            candidate=target.migrations,
            label=f"migrations directory for target {target.name}",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
        migrations_dir.mkdir(parents=True, exist_ok=True)
    except PathBoundaryError as error:
        raise runtime.DbError(describe_path_boundary_error(error)) from error
    except OSError as error:
        raise runtime.DbError(
            f"Unable to create migrations directory {target.migrations}: {error.strerror or error}"
        ) from error
    dbmate = Dbmate(migrations_dir=migrations_dir, dbmate_bin=dbmate_bin)
    return dbmate.new(migration_name)


def up(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    return execute_head_mutation(
        target=target,
        url=url,
        dbmate_bin=dbmate_bin,
        command="up",
        create_if_missing=True,
    )


def migrate(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    return execute_head_mutation(
        target=target,
        url=url,
        dbmate_bin=dbmate_bin,
        command="migrate",
        create_if_missing=False,
        preflight=ensure_migrate_preflight,
    )


def down(
    target: TargetConfig,
    *,
    steps: int = 1,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    if steps <= 0:
        raise runtime.DbError("down steps must be greater than zero.")

    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        before = runtime.inspect_live(rt, context="db down pre-status")
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=before,
            context="db down pre-status",
        )
        runtime.ensure_rollback_allowed(
            runtime=rt,
            applied_count=before.applied_count,
            steps=steps,
            context="db down precheck",
        )
        target_index = max(before.applied_count - steps, 0)
        if target_index == 0:
            raise runtime.DbError(
                "db down to migration index 0 is not supported because matey has no zero-migration schema baseline."
            )

        runtime.require_success(rt.conn.rollback(steps), context=f"db down ({steps})")
        after = runtime.inspect_live(rt, context="db down post-status")
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=after,
            context="db down post-status",
        )
        runtime.verify_expected_schema(
            runtime=rt,
            expected_index=after.applied_count,
            context="db down postcheck",
        )
        return MutationResult(
            target_name=target.name,
            before_index=before.applied_count,
            after_index=after.applied_count,
        )


def drift(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> DriftResult:
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        live = runtime.inspect_live(rt, context="db drift status")
        if runtime.live_relation(state=rt.state, live=live) == "ahead":
            return DriftResult(
                target_name=target.name,
                applied_index=live.applied_count,
                drifted=True,
            )
        if live.applied_count == 0:
            raise runtime.DbError(
                "db drift is unavailable before the first applied migration checkpoint."
            )
        schema_match, _, _ = runtime.compare_expected_schema(
            runtime=rt,
            expected_index=live.applied_count,
            context="db drift",
        )
        return DriftResult(
            target_name=target.name,
            applied_index=live.applied_count,
            drifted=(schema_match is False),
        )


def plan(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> PlanResult:
    return _with_plan_runtime(
        target=target,
        url=url,
        dbmate_bin=dbmate_bin,
        context="db plan",
        action=lambda rt, live, target_index: _plan_result(
            target=target,
            rt=rt,
            live=live,
            target_index=target_index,
        ),
    )


def _plan_result(
    *,
    target: TargetConfig,
    rt: runtime.RuntimeContext,
    live: runtime.LiveStatus,
    target_index: int,
) -> PlanResult:
    if runtime.live_relation(state=rt.state, live=live) == "ahead":
        return PlanResult(
            target_name=target.name,
            applied_index=live.applied_count,
            target_index=target_index,
            matches=False,
        )
    schema_match, _, _ = runtime.compare_expected_schema(
        runtime=rt,
        expected_index=target_index,
        context="db plan",
    )
    return PlanResult(
        target_name=target.name,
        applied_index=live.applied_count,
        target_index=target_index,
        matches=(schema_match is not False),
    )


def plan_sql(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> str:
    return _with_plan_runtime(
        target=target,
        url=url,
        dbmate_bin=dbmate_bin,
        context="db plan sql",
        action=lambda rt, _live, target_index: runtime.expected_sql_for_index(
            runtime=rt,
            index=target_index,
        )
        or "",
    )


def plan_diff(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> str:
    return _with_plan_runtime(
        target=target,
        url=url,
        dbmate_bin=dbmate_bin,
        context="db plan diff",
        action=_plan_diff_result,
    )


def _plan_diff_result(
    rt: runtime.RuntimeContext,
    _live: runtime.LiveStatus,
    target_index: int,
) -> str:
    expected_sql = runtime.expected_sql_for_index(runtime=rt, index=target_index) or ""
    live_sql = runtime.dump_live_schema(rt.conn, context="db plan diff dump")
    engine = runtime.engine_from_url(rt.conn.url)
    try:
        left = SqlProgram(live_sql, engine=engine).schema_fingerprint(
            context_url=rt.conn.url
        )
        right = SqlProgram(expected_sql, engine=engine).schema_fingerprint(
            context_url=rt.conn.url
        )
    except SqlError as error:
        raise runtime.DbError(f"db plan diff failed: SQL analysis failed: {error}") from error
    return unified_sql_diff(
        left_sql=left,
        right_sql=right,
        left_label="live/schema.sql",
        right_label="expected/worktree.sql",
    )


def execute_head_mutation(
    *,
    target: TargetConfig,
    url: str | None,
    dbmate_bin: Path | None,
    command: str,
    create_if_missing: bool,
    preflight: Callable[[runtime.RuntimeContext, str], None] | None = None,
) -> MutationResult:
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        require_head_baseline(rt, command)
        if preflight is not None:
            preflight(rt, command)
        before = read_head_mutation_status(
            rt,
            command=command,
            create_if_missing=create_if_missing,
        )
        runtime.require_success(getattr(rt.conn, command)(), context=f"db {command}")
        after = runtime.inspect_live(rt, context=f"db {command} post-status")
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=after,
            context=f"db {command} post-status",
        )
        runtime.verify_expected_schema(
            runtime=rt,
            expected_index=len(rt.state.worktree_steps),
            context=f"db {command} postcheck",
        )
        return MutationResult(
            target_name=target.name,
            before_index=before.applied_count,
            after_index=after.applied_count,
        )


def read_head_mutation_status(
    rt: runtime.RuntimeContext,
    *,
    command: str,
    create_if_missing: bool,
) -> runtime.LiveStatus:
    validated_pending_up = False
    try:
        _, before = runtime.read_status(rt.conn)
    except runtime.StatusError as error:
        if not create_if_missing or not error.missing_db:
            raise runtime.DbError(
                runtime.format_command_error(f"db {command} pre-status", error.result)
            ) from error
        runtime.ensure_pending_up_allowed(
            runtime=rt,
            applied_count=0,
            context=f"db {command} precheck",
        )
        validated_pending_up = True
        runtime.require_success(rt.conn.create(), context=f"db {command} create-if-needed")
        _, before = runtime.read_status_checked(
            rt.conn,
            context=f"db {command} pre-status after create",
        )

    runtime.ensure_prefix(state=rt.state, live=before)
    runtime.ensure_live_not_ahead(
        state=rt.state,
        live=before,
        context=f"db {command} pre-status",
    )
    if not validated_pending_up:
        runtime.ensure_pending_up_allowed(
            runtime=rt,
            applied_count=before.applied_count,
            context=f"db {command} precheck",
        )
    return before


def require_head_baseline(rt: runtime.RuntimeContext, command: str) -> None:
    if rt.state.worktree_steps:
        return
    raise runtime.DbError(
        f"db {command} is unavailable before the first worktree migration checkpoint."
    )


def ensure_migrate_preflight(rt: runtime.RuntimeContext, command: str) -> None:
    if runtime.is_bigquery_url(rt.conn.url):
        runtime.ensure_bigquery_dataset_exists(
            conn=rt.conn,
            context=f"db {command} pre-status",
        )


@contextmanager
def open_plan_runtime(
    *,
    target: TargetConfig,
    url: str | None,
    dbmate_bin: Path | None,
    context: str,
) -> Iterator[tuple[runtime.RuntimeContext, runtime.LiveStatus, int]]:
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        live = runtime.inspect_live(rt, context=f"{context} status")
        target_index = len(rt.state.worktree_steps)
        if target_index == 0:
            raise runtime.DbError(
                f"{context} is unavailable before the first worktree migration checkpoint."
            )
        yield rt, live, target_index


def _with_plan_runtime(
    *,
    target: TargetConfig,
    url: str | None,
    dbmate_bin: Path | None,
    context: str,
    action: Callable[[runtime.RuntimeContext, runtime.LiveStatus, int], T],
) -> T:
    with open_plan_runtime(
        target=target,
        url=url,
        dbmate_bin=dbmate_bin,
        context=context,
    ) as (rt, live, target_index):
        return action(rt, live, target_index)


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
