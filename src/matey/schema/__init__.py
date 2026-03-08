from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TypeVar

from matey.config import TargetConfig
from matey.lockfile import LockPolicy, LockState, build_lock_state
from matey.paths import PathBoundaryError, describe_path_boundary_error, safe_descendant
from matey.repo import Snapshot, SnapshotError
from matey.scratch import Engine
from matey.sql import SqlError, SqlProgram, SqlTextDecodeError, decode_sql_text, unified_sql_diff
from matey.sql.policy import normalize_engine
from matey.tx import TxError, recover_artifacts, serialized_target

from . import artifacts, replay
from . import plan as planning
from .plan import SchemaError

T = TypeVar("T")


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


@dataclass(frozen=True, slots=True)
class InitResult:
    target_name: str
    engine: str
    wrote: bool
    changed_files: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class InitPlan:
    target: TargetConfig
    engine: Engine
    writes: dict[Path, bytes]
    deletes: tuple[Path, ...]
    created_dirs: tuple[str, ...]


def status(target: TargetConfig, *, policy: LockPolicy | None = None) -> LockState:
    with serialized_target(target.dir):
        _recover_target_artifacts(target, context="status")
        try:
            snapshot = Snapshot.from_worktree(target)
        except SnapshotError as error:
            raise SchemaError(str(error)) from error
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
    return _run_plan_mode(
        target=target,
        mode="summary",
        base_ref=base_ref,
        clean=clean,
        test_base_url=test_base_url,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
        policy=policy,
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
    return _run_plan_mode(
        target=target,
        mode="sql",
        base_ref=base_ref,
        clean=clean,
        test_base_url=test_base_url,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
        policy=policy,
    )


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
    return _run_plan_mode(
        target=target,
        mode="diff",
        base_ref=base_ref,
        clean=clean,
        test_base_url=test_base_url,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
        policy=policy,
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
    return _with_replay_plan(
        target=target,
        context="apply",
        base_ref=base_ref,
        clean=clean,
        test_base_url=test_base_url,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
        policy=policy,
        action=lambda structural, replay_outcome: _apply_result(
            target=target,
            structural=structural,
            replay_outcome=replay_outcome,
        ),
    )


def init_target(
    target: TargetConfig,
    *,
    engine: str | None = None,
    force: bool = False,
    policy: LockPolicy | None = None,
) -> InitResult:
    plan = prepare_init_target(
        target,
        engine=engine,
        force=force,
        policy=policy,
    )
    return apply_init_target(plan)


def prepare_init_target(
    target: TargetConfig,
    *,
    engine: str | None = None,
    force: bool = False,
    policy: LockPolicy | None = None,
) -> InitPlan:
    with serialized_target(target.dir):
        _recover_target_artifacts(target, context="init target")
        effective_policy = policy or LockPolicy()
        resolved_engine = _resolve_init_engine(
            target=target,
            engine=engine,
            policy=effective_policy,
        )
        target_root = _safe_target_dir(target)
        migrations_dir = _safe_target_dir(target, path=target.migrations, label="migrations directory")
        checkpoints_dir = _safe_target_dir(target, path=target.checkpoints, label="checkpoints directory")
        missing_dirs = tuple(
            directory
            for directory in (migrations_dir, checkpoints_dir)
            if not directory.exists()
        )

        existing_paths = tuple(
            path
            for path in (target.schema, target.lockfile, migrations_dir, checkpoints_dir)
            if path.exists()
        )
        if existing_paths and not force:
            rendered = ", ".join(str(path) for path in existing_paths)
            raise SchemaError(
                "init target refused to overwrite existing target artifacts: "
                f"{rendered}. Pass --force to reset the target."
            )

        migrations_dir.mkdir(parents=True, exist_ok=True)
        checkpoints_dir.mkdir(parents=True, exist_ok=True)

        desired_artifacts = artifacts.build_zero_target_artifacts(
            target=target,
            engine=resolved_engine,
            policy=effective_policy,
        )
        migration_deletes = (
            tuple(sorted(migrations_dir.rglob("*.sql"), key=lambda path: path.as_posix()))
            if force and migrations_dir.exists()
            else ()
        )
        checkpoint_deletes = (
            tuple(sorted(checkpoints_dir.rglob("*.sql"), key=lambda path: path.as_posix()))
            if force and checkpoints_dir.exists()
            else ()
        )
        writes, checkpoint_delta = artifacts.compute_artifact_delta(
            target=target,
            desired_artifacts=desired_artifacts,
        )
        deletes = tuple(
            sorted(
                {
                    *migration_deletes,
                    *checkpoint_delta,
                    *checkpoint_deletes,
                },
                key=lambda path: path.as_posix(),
            )
        )
        created_dirs = []
        for directory in missing_dirs:
            try:
                rel_dir = directory.relative_to(target_root).as_posix()
            except ValueError:
                rel_dir = directory.as_posix()
            created_dirs.append(rel_dir)
        return InitPlan(
            target=target,
            engine=resolved_engine,
            writes=writes,
            deletes=deletes,
            created_dirs=tuple(sorted(created_dirs)),
        )


def apply_init_target(plan: InitPlan) -> InitResult:
    changed_files = artifacts.apply_artifact_delta(
        target=plan.target,
        writes=plan.writes,
        deletes=plan.deletes,
    )
    return InitResult(
        target_name=plan.target.name,
        engine=plan.engine.value,
        wrote=bool(plan.writes or plan.deletes or plan.created_dirs),
        changed_files=tuple(sorted({*changed_files, *plan.created_dirs})),
    )


def _apply_result(
    *,
    target: TargetConfig,
    structural: planning.StructuralPlan,
    replay_outcome: replay.ReplayOutcome,
) -> ApplyResult:
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


def _with_replay_plan(
    *,
    target: TargetConfig,
    context: str,
    base_ref: str | None,
    clean: bool,
    test_base_url: str | None,
    keep_scratch: bool,
    dbmate_bin: Path | None,
    policy: LockPolicy | None,
    action: Callable[[planning.StructuralPlan, replay.ReplayOutcome], T],
) -> T:
    with serialized_target(target.dir):
        structural, replay_outcome = execute_replay_plan(
            target=target,
            context=context,
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        return action(structural, replay_outcome)


def _run_plan_mode(
    *,
    target: TargetConfig,
    mode: Literal["summary", "sql", "diff"],
    base_ref: str | None,
    clean: bool,
    test_base_url: str | None,
    keep_scratch: bool,
    dbmate_bin: Path | None,
    policy: LockPolicy | None,
) -> PlanResult | str:
    contexts: dict[str, str] = {
        "summary": "plan",
        "sql": "plan sql",
        "diff": "plan diff",
    }
    return _with_replay_plan(
        target=target,
        context=contexts[mode],
        base_ref=base_ref,
        clean=clean,
        test_base_url=test_base_url,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
        policy=policy,
        action=lambda structural, replay_outcome: (
            replay_outcome.replay_schema_sql
            if mode == "sql"
            else _plan_result_for_mode(
                target=target,
                mode=mode,
                structural=structural,
                replay_outcome=replay_outcome,
            )
        ),
    )


def _plan_result_for_mode(
    *,
    target: TargetConfig,
    mode: Literal["summary", "diff"],
    structural: planning.StructuralPlan,
    replay_outcome: replay.ReplayOutcome,
) -> PlanResult | str:
    context = "plan" if mode == "summary" else "plan diff"
    left, right = replay_sql_fingerprints(
        target=target,
        structural=structural,
        replay_outcome=replay_outcome,
        context=context,
    )
    if mode == "diff":
        return unified_sql_diff(
            left_sql=left,
            right_sql=right,
            left_label="worktree/schema.sql",
            right_label="replay/schema.sql",
        )
    return PlanResult(
        target_name=target.name,
        divergence_index=structural.divergence_index,
        anchor_index=structural.anchor_index,
        tail_count=len(structural.tail_steps),
        matches=left == right,
        replay_scratch_url=replay_outcome.replay_scratch_url,
        down_scratch_url=replay_outcome.down_scratch_url,
        down_checked=replay_outcome.down_checked,
        down_skipped=replay_outcome.down_skipped,
    )


def execute_replay_plan(
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
    try:
        structural = planning.build_structural_plan(
            target=target,
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            policy=policy,
        )
    except SnapshotError as error:
        raise SchemaError(str(error)) from error
    replay_outcome = replay.run_replay_checks(
        structural,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
    )
    return structural, replay_outcome


def replay_sql_fingerprints(
    *,
    target: TargetConfig,
    structural: planning.StructuralPlan,
    replay_outcome: replay.ReplayOutcome,
    context: str,
) -> tuple[str, str]:
    worktree_sql = _decode_optional_schema(
        structural.head_snapshot.schema_sql,
        label=f"{target.name} worktree schema.sql",
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
        raise SchemaError(f"{context}: SQL analysis failed: {error}") from error
    return left, right


def _recover_target_artifacts(target: TargetConfig, *, context: str) -> None:
    try:
        recover_artifacts(target.dir)
    except TxError as error:
        raise SchemaError(f"{context}: artifact recovery failed: {error}") from error


def _decode_optional_schema(payload: bytes | None, *, label: str) -> str:
    if payload is None:
        return ""
    try:
        return decode_sql_text(payload, label=label)
    except SqlTextDecodeError as error:
        raise SchemaError(str(error)) from error


def _resolve_init_engine(
    *,
    target: TargetConfig,
    engine: str | None,
    policy: LockPolicy,
) -> Engine:
    requested = engine.strip() if engine is not None and engine.strip() else target.engine.strip()
    if requested:
        normalized = normalize_engine(requested)
        try:
            return Engine(normalized)
        except ValueError as error:
            raise SchemaError(f"Unsupported engine {requested!r} for init target.") from error

    try:
        snapshot = Snapshot.from_worktree(target)
    except SnapshotError as error:
        raise SchemaError(str(error)) from error
    state = build_lock_state(snapshot, policy=policy)
    if state.lock is None:
        raise SchemaError(
            "init target requires --engine for a fresh target without an existing lockfile."
        )
    try:
        return Engine(state.lock.engine)
    except ValueError as error:
        raise SchemaError(
            f"Invalid lockfile engine {state.lock.engine!r}. Pass --engine explicitly."
        ) from error


def _safe_target_dir(
    target: TargetConfig,
    *,
    path: Path | None = None,
    label: str | None = None,
) -> Path:
    candidate = target.dir if path is None else path
    try:
        return safe_descendant(
            root=target.dir,
            candidate=candidate,
            label=label or f"target {target.name} directory",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
    except PathBoundaryError as error:
        raise SchemaError(describe_path_boundary_error(error)) from error


__all__ = [
    "ApplyResult",
    "InitPlan",
    "InitResult",
    "PlanResult",
    "SchemaError",
    "apply",
    "apply_init_target",
    "init_target",
    "plan",
    "plan_diff",
    "plan_sql",
    "prepare_init_target",
    "status",
]
