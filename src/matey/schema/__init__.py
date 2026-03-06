from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from matey.config import TargetConfig
from matey.lockfile import LockPolicy, LockState, build_lock_state
from matey.repo import Snapshot
from matey.sql import SqlError, SqlProgram, unified_sql_diff
from matey.tx import TxError, recover_artifacts, serialized_target

from . import artifacts, replay
from . import plan as planning
from .plan import SchemaError


@dataclass(frozen=True, slots=True)
class PlanResult:
    target_name: str
    divergence_index: int | None
    anchor_index: int
    tail_count: int
    matches: bool
    replay_scratch_url: str
    down_scratch_url: str | None
    down_checked: tuple[str, ...]
    down_skipped: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ApplyResult:
    target_name: str
    wrote: bool
    changed_files: tuple[str, ...]
    replay_scratch_url: str
    down_scratch_url: str | None


def status(target: TargetConfig, *, policy: LockPolicy | None = None) -> LockState:
    with serialized_target(target.dir):
        _recover_target_artifacts(target, context="status")
        snapshot = Snapshot.from_worktree(target)
        return build_lock_state(snapshot, policy=policy)


def plan(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> PlanResult:
    with serialized_target(target.dir):
        structural, replay_outcome = _prepare_plan(
            target=target,
            context="plan",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        worktree_sql = (
            structural.head_snapshot.schema_sql.decode("utf-8")
            if structural.head_snapshot.schema_sql is not None
            else ""
        )
        try:
            matches = SqlProgram(worktree_sql, engine=structural.engine.value).schema_equals(
                SqlProgram(replay_outcome.replay_schema_sql, engine=structural.engine.value),
                left_context_url=replay_outcome.replay_scratch_url,
                right_context_url=replay_outcome.replay_scratch_url,
            )
        except SqlError as error:
            raise SchemaError(f"plan: SQL analysis failed: {error}") from error
        return PlanResult(
            target_name=target.name,
            divergence_index=structural.divergence_index,
            anchor_index=structural.anchor_index,
            tail_count=len(structural.tail_steps),
            matches=matches,
            replay_scratch_url=replay_outcome.replay_scratch_url,
            down_scratch_url=replay_outcome.down_scratch_url,
            down_checked=replay_outcome.down_checked,
            down_skipped=replay_outcome.down_skipped,
        )


def plan_sql(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> str:
    with serialized_target(target.dir):
        _structural, replay_outcome = _prepare_plan(
            target=target,
            context="plan sql",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        return replay_outcome.replay_schema_sql


def plan_diff(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> str:
    with serialized_target(target.dir):
        structural, replay_outcome = _prepare_plan(
            target=target,
            context="plan diff",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        worktree_sql = (
            structural.head_snapshot.schema_sql.decode("utf-8")
            if structural.head_snapshot.schema_sql is not None
            else ""
        )
        try:
            left = SqlProgram(worktree_sql, engine=structural.engine.value).schema_fingerprint(
                context_url=replay_outcome.replay_scratch_url
            )
            right = SqlProgram(
                replay_outcome.replay_schema_sql,
                engine=structural.engine.value,
            ).schema_fingerprint(context_url=replay_outcome.replay_scratch_url)
        except SqlError as error:
            raise SchemaError(f"plan diff: SQL analysis failed: {error}") from error
        return unified_sql_diff(
            left_sql=left,
            right_sql=right,
            left_label="worktree/schema.sql",
            right_label="replay/schema.sql",
        )


def apply(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> ApplyResult:
    with serialized_target(target.dir):
        structural, replay_outcome = _prepare_plan(
            target=target,
            context="apply",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        desired_artifacts = artifacts.build_desired_artifacts(
            structural=structural,
            replay=replay_outcome,
        )
        writes, deletes = artifacts.compute_artifact_delta(
            target=structural.target,
            desired_artifacts=desired_artifacts,
        )
        if not writes and not deletes:
            return ApplyResult(
                target_name=target.name,
                wrote=False,
                changed_files=(),
                replay_scratch_url=replay_outcome.replay_scratch_url,
                down_scratch_url=replay_outcome.down_scratch_url,
            )

        changed_files = artifacts.apply_artifact_delta(
            target=structural.target,
            writes=writes,
            deletes=deletes,
        )
        return ApplyResult(
            target_name=target.name,
            wrote=True,
            changed_files=changed_files,
            replay_scratch_url=replay_outcome.replay_scratch_url,
            down_scratch_url=replay_outcome.down_scratch_url,
        )


def _prepare_plan(
    *,
    target: TargetConfig,
    context: str,
    base_ref: str | None,
    clean: bool,
    test_base_url: str | None,
    keep_scratch: bool,
    dbmate_bin: Path | None,
    policy: LockPolicy | None,
) -> tuple[planning.StructuralPlan, replay.ReplayOutcome]:
    _recover_target_artifacts(target, context=context)
    structural = planning.build_structural_plan(
        target=target,
        base_ref=base_ref,
        clean=clean,
        test_base_url=test_base_url,
        policy=policy,
    )
    replay_outcome = replay.run_replay_checks(
        structural,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
    )
    return structural, replay_outcome


def _recover_target_artifacts(target: TargetConfig, *, context: str) -> None:
    try:
        recover_artifacts(target.dir)
    except TxError as error:
        raise SchemaError(f"{context}: artifact recovery failed: {error}") from error


__all__ = [
    "ApplyResult",
    "PlanResult",
    "SchemaError",
    "apply",
    "plan",
    "plan_diff",
    "plan_sql",
    "status",
]
