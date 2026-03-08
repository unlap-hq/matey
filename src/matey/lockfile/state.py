from __future__ import annotations

from matey.repo import Snapshot
from matey.sql import SqlTextDecodeError, decode_sql_text

from .model import (
    Diagnostic,
    DiagnosticCode,
    Divergence,
    LockPolicy,
    LockState,
    Step,
    generated_sql_digest,
)
from .parse import build_worktree_steps, parse_lockfile
from .validate import validate_state


def build_lock_state(
    input_files: Snapshot,
    *,
    policy: LockPolicy | None = None,
) -> LockState:
    effective_policy = policy or LockPolicy()
    lock, lock_diagnostics = parse_lockfile(input_files.lock_toml)
    steps, orphans, snapshot_diagnostics = build_worktree_steps(
        input_files,
        policy=effective_policy,
        lock=lock,
    )
    schema_diagnostics = ()
    try:
        current_schema_digest = generated_sql_digest(
            decode_sql_text(input_files.schema_sql, label=effective_policy.schema_file)
            if input_files.schema_sql is not None
            else None,
            policy=effective_policy,
        )
    except SqlTextDecodeError as error:
        current_schema_digest = None
        schema_diagnostics = (
            Diagnostic(DiagnosticCode.INPUT_PATH_INVALID, effective_policy.schema_file, str(error)),
        )
    diagnostics = (
        lock_diagnostics
        + snapshot_diagnostics
        + schema_diagnostics
        + validate_state(
            input_files=input_files,
            lock=lock,
            steps=steps,
            schema_digest=current_schema_digest,
            orphans=orphans,
            policy=effective_policy,
        )
    )
    return LockState(
        target_name=input_files.target_name,
        lock=lock,
        worktree_steps=steps,
        schema_digest=current_schema_digest,
        orphan_checkpoints=orphans,
        diagnostics=diagnostics,
    )


def lock_worktree_divergence(state: LockState) -> Divergence | None:
    lock = state.lock
    base_target = lock.target if lock is not None else state.target_name
    if base_target != state.target_name:
        return Divergence(
            index=1,
            field="target_name",
            base_value=base_target,
            head_value=state.target_name,
        )
    return first_signature_divergence(
        base_signatures=step_signatures(lock.steps) if lock is not None else (),
        head_signatures=step_signatures(state.worktree_steps),
    )


def divergence_between_states(base: LockState, head: LockState) -> Divergence | None:
    if base.target_name != head.target_name:
        return Divergence(
            index=1,
            field="target_name",
            base_value=base.target_name,
            head_value=head.target_name,
        )
    return first_signature_divergence(
        base_signatures=step_signatures(base.worktree_steps),
        head_signatures=step_signatures(head.worktree_steps),
    )


def first_lock_divergence(base: LockState, head: LockState) -> Divergence | None:
    if not base.is_clean or not head.is_clean:
        raise ValueError("Cannot compare divergence for non-clean lock states.")
    return divergence_between_states(base, head)


def step_signatures(steps: tuple[Step, ...]) -> tuple[tuple[str, str, str], ...]:
    return tuple((step.version, step.migration_file, step.migration_digest) for step in steps)


def first_signature_divergence(
    *,
    base_signatures: tuple[tuple[str, str, str], ...],
    head_signatures: tuple[tuple[str, str, str], ...],
) -> Divergence | None:
    fields = ("version", "migration_file", "migration_digest")
    shared = min(len(base_signatures), len(head_signatures))
    for idx in range(shared):
        left = base_signatures[idx]
        right = head_signatures[idx]
        for field, base_value, head_value in zip(fields, left, right, strict=True):
            if base_value == head_value:
                continue
            return Divergence(
                index=idx + 1,
                field=field,
                base_value=base_value,
                head_value=head_value,
            )
    if len(base_signatures) == len(head_signatures):
        return None
    return Divergence(
        index=shared + 1,
        field="step_count",
        base_value=str(len(base_signatures)),
        head_value=str(len(head_signatures)),
    )


__all__ = [
    "build_lock_state",
    "divergence_between_states",
    "first_lock_divergence",
    "lock_worktree_divergence",
]
