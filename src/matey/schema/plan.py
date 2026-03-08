from __future__ import annotations

import os
from dataclasses import dataclass

from matey import Engine
from matey.lockfile import (
    DiagnosticCode,
    Divergence,
    LockFile,
    LockPolicy,
    LockState,
    WorktreeStep,
    build_lock_state,
    divergence_between_states,
    lock_worktree_divergence,
)
from matey.project import TargetConfig
from matey.repo import GitRepo, Snapshot
from matey.scratch import ScratchError
from matey.scratch import engine_from_url as scratch_engine_from_url
from matey.sql import decode_sql_text

_HEAD_FATAL_DIAGNOSTICS = frozenset(
    {
        DiagnosticCode.LOCKFILE_PARSE_ERROR,
        DiagnosticCode.LOCKFILE_VERSION_MISMATCH,
        DiagnosticCode.LOCKFILE_HASH_ALGORITHM_MISMATCH,
        DiagnosticCode.LOCKFILE_CANONICALIZER_MISMATCH,
        DiagnosticCode.LOCKFILE_SCHEMA_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_MIGRATIONS_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_CHECKPOINTS_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_STEP_PATH_INVALID,
        DiagnosticCode.LOCKFILE_STEP_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_DUPLICATE_MIGRATION,
        DiagnosticCode.LOCKFILE_DUPLICATE_STEP_INDEX,
        DiagnosticCode.LOCKFILE_STEP_INDEX_INVALID,
        DiagnosticCode.INPUT_PATH_INVALID,
        DiagnosticCode.INPUT_PATH_DUPLICATE,
        DiagnosticCode.COHERENCE_TARGET_MISMATCH,
    }
)


class SchemaError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class StructuralPlan:
    target: TargetConfig
    policy: LockPolicy
    head_snapshot: Snapshot
    head_state: LockState
    divergence_index: int | None
    anchor_index: int
    tail_steps: tuple[WorktreeStep, ...]
    anchor_sql: str | None
    engine: Engine
    test_base_url: str | None


def build_structural_plan(
    *,
    target: TargetConfig,
    base_ref: str | None,
    clean: bool,
    test_base_url: str | None,
    policy: LockPolicy | None,
) -> StructuralPlan:
    """Build the non-mutating replay plan for one target.

    This computes the divergence/anchor boundary and the scratch replay context,
    but does not touch scratch or execute any migrations. Downstream code uses
    the returned plan to run replay and artifact generation consistently.
    """
    if clean and base_ref is not None:
        raise SchemaError("Cannot combine clean=True with base_ref.")

    effective_policy = policy or LockPolicy()
    head_snapshot = Snapshot.from_worktree(target)
    head_state = build_lock_state(head_snapshot, policy=effective_policy)
    require_head_state_usable(head_state)

    base_snapshot: Snapshot | None = None
    base_state: LockState | None = None

    if clean:
        divergence = (
            Divergence(index=1, field="clean", base_value="clean=false", head_value="clean=true")
            if head_state.worktree_steps
            else None
        )
    elif base_ref is None:
        divergence = lock_worktree_divergence(head_state)
    else:
        git_repo = GitRepo.open(target.root)
        merge_base = git_repo.resolve_merge_base(base_ref)
        target_rel_dir = target.root.resolve().relative_to(git_repo.repo_root).as_posix()
        base_snapshot = Snapshot.from_tree(
            target_name=target.name,
            target_rel_dir=target_rel_dir,
            root_tree=git_repo.tree_for(merge_base.merge_base_oid),
        )
        base_state = build_lock_state(base_snapshot, policy=effective_policy)
        require_clean_state(base_state, label=f"base snapshot ({base_ref})")
        divergence = divergence_between_states(base_state, head_state)

    if divergence is None:
        anchor_index = len(head_state.worktree_steps)
        divergence_index = None
        tail_steps: tuple[WorktreeStep, ...] = ()
    else:
        anchor_index = divergence.index - 1
        divergence_index = divergence.index
        tail_steps = head_state.worktree_steps[anchor_index:]

    anchor_sql: str | None = None
    if anchor_index > 0:
        anchor_sql = select_anchor_sql(
            anchor_index=anchor_index,
            head_snapshot=head_snapshot,
            head_steps=head_state.worktree_steps,
            base_snapshot=base_snapshot,
            base_steps=base_state.worktree_steps if base_state is not None else (),
            use_base=base_ref is not None and not clean and base_state is not None,
        )

    engine, resolved_test_base_url = resolve_replay_context(
        target=target,
        lock=head_state.lock,
        explicit_test_base_url=test_base_url,
    )
    return StructuralPlan(
        target=target,
        policy=effective_policy,
        head_snapshot=head_snapshot,
        head_state=head_state,
        divergence_index=divergence_index,
        anchor_index=anchor_index,
        tail_steps=tail_steps,
        anchor_sql=anchor_sql,
        engine=engine,
        test_base_url=resolved_test_base_url,
    )


def select_anchor_sql(
    *,
    anchor_index: int,
    head_snapshot: Snapshot,
    head_steps: tuple[WorktreeStep, ...],
    base_snapshot: Snapshot | None,
    base_steps: tuple[WorktreeStep, ...],
    use_base: bool,
) -> str:
    if use_base:
        if base_snapshot is None:
            raise SchemaError("Base snapshot is required for base-aware anchoring.")
        if len(base_steps) < anchor_index:
            raise SchemaError(
                f"Base snapshot has {len(base_steps)} steps; cannot anchor at index {anchor_index}."
            )
        anchor_step = base_steps[anchor_index - 1]
        anchor_bytes = base_snapshot.checkpoints.get(anchor_step.checkpoint_file)
        source_label = "base"
    else:
        anchor_step = head_steps[anchor_index - 1]
        anchor_bytes = head_snapshot.checkpoints.get(anchor_step.checkpoint_file)
        source_label = "head"

    if anchor_bytes is None:
        raise SchemaError(
            f"Missing anchor checkpoint {anchor_step.checkpoint_file!r} in {source_label} snapshot."
        )
    return decode_sql_text(
        anchor_bytes,
        label=f"anchor checkpoint {anchor_step.checkpoint_file}",
    )


def resolve_replay_context(
    *,
    target: TargetConfig,
    lock: LockFile | None,
    explicit_test_base_url: str | None,
) -> tuple[Engine, str | None]:
    test_base_from_arg = _normalized_optional(explicit_test_base_url)
    test_base_from_env = _normalized_optional(os.getenv(target.test_url_env))
    url_from_env = _normalized_optional(os.getenv(target.url_env))
    lock_engine = resolved_lock_engine(lock)

    inferred_engine: Engine | None = None
    resolved_test_base_url: str | None = None
    invalid_candidates: list[str] = []
    for source, candidate in (
        ("test_base_url", test_base_from_arg),
        (target.test_url_env, test_base_from_env),
    ):
        if candidate is None:
            continue
        try:
            inferred_engine = scratch_engine_from_url(candidate)
        except (SchemaError, ScratchError) as error:
            invalid_candidates.append(f"{source}: {error}")
            continue
        resolved_test_base_url = candidate
        break

    if inferred_engine is None and url_from_env is not None:
        try:
            inferred_engine = scratch_engine_from_url(url_from_env)
        except (SchemaError, ScratchError) as error:
            invalid_candidates.append(f"{target.url_env}: {error}")

    if inferred_engine is None:
        inferred_engine = lock_engine

    if inferred_engine is None:
        details = (
            f" Invalid URL values: {'; '.join(invalid_candidates)}." if invalid_candidates else ""
        )
        raise SchemaError(
            "Unable to infer replay engine. Provide test_base_url, set test_url_env, or add schema.lock.toml. "
            "url_env is used only for engine inference."
            + details
        )

    if lock_engine is not None and inferred_engine is not lock_engine:
        raise SchemaError(
            f"Replay engine mismatch: inferred {inferred_engine.value!r} from URL, "
            f"but lockfile engine is {lock_engine.value!r}."
        )

    if inferred_engine is Engine.BIGQUERY and resolved_test_base_url is None:
        raise SchemaError(
            f"BigQuery scratch requires test_base_url or {target.test_url_env}; "
            f"{target.url_env} is used only for engine inference."
        )

    return inferred_engine, resolved_test_base_url


def resolved_lock_engine(lock: LockFile | None) -> Engine | None:
    if lock is None:
        return None
    try:
        return Engine(lock.engine)
    except ValueError as error:
        raise SchemaError(
            f"Invalid lockfile engine {lock.engine!r}. Regenerate schema artifacts."
        ) from error
def require_clean_state(state: LockState, *, label: str) -> None:
    if state.is_clean:
        return
    details = "; ".join(
        f"{diag.code.value}@{diag.path}: {diag.detail}" for diag in state.diagnostics
    )
    raise SchemaError(f"{label} lock state is not clean: {details}")


def require_head_state_usable(state: LockState) -> None:
    fatal = head_state_errors(state)
    if not fatal:
        return
    details = "; ".join(f"{code}@{path}: {detail}" for code, path, detail in fatal)
    raise SchemaError(f"head worktree lock state is invalid: {details}")


def head_state_errors(state: LockState) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (diag.code.value, diag.path, diag.detail)
        for diag in state.diagnostics
        if diag.code in _HEAD_FATAL_DIAGNOSTICS
    )


def _normalized_optional(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


__all__ = [
    "SchemaError",
    "StructuralPlan",
    "build_structural_plan",
    "head_state_errors",
    "require_clean_state",
    "require_head_state_usable",
    "resolve_replay_context",
    "select_anchor_sql",
]
