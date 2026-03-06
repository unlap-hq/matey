from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from matey.config import TargetConfig
from matey.dbmate import CmdResult, Dbmate
from matey.sql import SqlProgram, unified_sql_diff

from . import runtime
from .runtime import DbError


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
    dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=dbmate_bin)
    return dbmate.new(migration_name)


def up(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        validated_pending_up = False
        try:
            _, before = runtime.read_status(rt.conn)
        except runtime.StatusError as error:
            if not error.missing_db:
                raise runtime.DbError(
                    runtime.format_command_error("db up pre-status", error.result)
                ) from error
            runtime.ensure_pending_up_allowed(
                runtime=rt,
                applied_count=0,
                context="db up precheck",
            )
            validated_pending_up = True
            runtime.require_success(rt.conn.create(), context="db up create-if-needed")
            _, before = runtime.read_status_checked(
                rt.conn,
                context="db up pre-status after create",
            )

        runtime.ensure_prefix(state=rt.state, live=before)
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=before,
            context="db up pre-status",
        )
        if not validated_pending_up:
            runtime.ensure_pending_up_allowed(
                runtime=rt,
                applied_count=before.applied_count,
                context="db up precheck",
            )

        runtime.require_success(rt.conn.up(), context="db up")
        after = runtime.inspect_live(rt, context="db up post-status")
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=after,
            context="db up post-status",
        )
        runtime.verify_expected_schema(
            runtime=rt,
            expected_index=len(rt.state.worktree_steps),
            context="db up postcheck",
        )
        return MutationResult(
            target_name=target.name,
            before_index=before.applied_count,
            after_index=after.applied_count,
        )


def migrate(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> MutationResult:
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        if runtime.is_bigquery_url(rt.conn.url):
            runtime.ensure_bigquery_dataset_exists(
                conn=rt.conn,
                context="db migrate pre-status",
            )
        before = runtime.inspect_live(rt, context="db migrate pre-status")
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=before,
            context="db migrate pre-status",
        )
        runtime.ensure_pending_up_allowed(
            runtime=rt,
            applied_count=before.applied_count,
            context="db migrate precheck",
        )

        runtime.require_success(rt.conn.migrate(), context="db migrate")
        after = runtime.inspect_live(rt, context="db migrate post-status")
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=after,
            context="db migrate post-status",
        )
        runtime.verify_expected_schema(
            runtime=rt,
            expected_index=len(rt.state.worktree_steps),
            context="db migrate postcheck",
        )
        return MutationResult(
            target_name=target.name,
            before_index=before.applied_count,
            after_index=after.applied_count,
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
        schema_match, _, _ = runtime.compare_expected_schema(
            runtime=rt,
            expected_index=live.applied_count,
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
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        live = runtime.inspect_live(rt, context="db plan status")
        target_index = len(rt.state.worktree_steps)
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
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        runtime.inspect_live(rt, context="db plan sql status")
        return (
            runtime.expected_sql_for_index(runtime=rt, index=len(rt.state.worktree_steps)) or ""
        )


def plan_diff(
    target: TargetConfig,
    *,
    url: str | None = None,
    dbmate_bin: Path | None = None,
) -> str:
    with runtime.open_runtime(target=target, url=url, dbmate_bin=dbmate_bin) as rt:
        runtime.inspect_live(rt, context="db plan diff status")
        _, expected_sql, live_sql = runtime.compare_expected_schema(
            runtime=rt,
            expected_index=len(rt.state.worktree_steps),
        )
        engine = runtime.engine_from_url(rt.conn.url)
        left = SqlProgram(live_sql or "", engine=engine).schema_fingerprint(
            context_url=rt.conn.url
        )
        right = SqlProgram(expected_sql or "", engine=engine).schema_fingerprint(
            context_url=rt.conn.url
        )
        return unified_sql_diff(
            left_sql=left,
            right_sql=right,
            left_label="live/schema.sql",
            right_label="expected/worktree.sql",
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
