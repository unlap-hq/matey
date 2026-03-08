from __future__ import annotations

from collections.abc import Mapping

from matey.repo import Snapshot

from .model import Diagnostic, DiagnosticCode, LockFile, LockPolicy, LockStep, WorktreeStep


def validate_state(
    *,
    input_files: Snapshot,
    lock: LockFile | None,
    steps: tuple[WorktreeStep, ...],
    schema_digest: str | None,
    orphans: tuple[str, ...],
    policy: LockPolicy,
) -> tuple[Diagnostic, ...]:
    return (
        validate_lock_header(lock=lock, policy=policy)
        + validate_target_coherence(input_files=input_files, lock=lock)
        + validate_schema(lock=lock, schema_digest=schema_digest, policy=policy)
        + validate_orphans(orphans)
        + validate_step_alignment(lock=lock, steps=steps, policy=policy)
    )


def validate_lock_header(*, lock: LockFile | None, policy: LockPolicy) -> tuple[Diagnostic, ...]:
    if lock is None:
        return ()

    checks = (
        (
            lock.lock_version != policy.lock_version,
            DiagnosticCode.LOCKFILE_VERSION_MISMATCH,
            f"lock_version={lock.lock_version}, expected {policy.lock_version}.",
        ),
        (
            lock.hash_algorithm != policy.hash_algorithm,
            DiagnosticCode.LOCKFILE_HASH_ALGORITHM_MISMATCH,
            f"hash_algorithm={lock.hash_algorithm!r}, expected {policy.hash_algorithm!r}.",
        ),
        (
            lock.canonicalizer != policy.canonicalizer,
            DiagnosticCode.LOCKFILE_CANONICALIZER_MISMATCH,
            f"canonicalizer={lock.canonicalizer!r}, expected {policy.canonicalizer!r}.",
        ),
        (
            lock.schema_file != policy.schema_file,
            DiagnosticCode.LOCKFILE_SCHEMA_PATH_MISMATCH,
            f"schema_file={lock.schema_file!r}, expected {policy.schema_file!r}.",
        ),
        (
            lock.migrations_dir != policy.migrations_dir,
            DiagnosticCode.LOCKFILE_MIGRATIONS_PATH_MISMATCH,
            f"migrations_dir={lock.migrations_dir!r}, expected {policy.migrations_dir!r}.",
        ),
        (
            lock.checkpoints_dir != policy.checkpoints_dir,
            DiagnosticCode.LOCKFILE_CHECKPOINTS_PATH_MISMATCH,
            f"checkpoints_dir={lock.checkpoints_dir!r}, expected {policy.checkpoints_dir!r}.",
        ),
    )
    return tuple(Diagnostic(code, "schema.lock.toml", detail) for failed, code, detail in checks if failed)


def validate_target_coherence(
    *, input_files: Snapshot, lock: LockFile | None
) -> tuple[Diagnostic, ...]:
    if lock is None or lock.target == input_files.target_name:
        return ()
    return (
        Diagnostic(
            DiagnosticCode.COHERENCE_TARGET_MISMATCH,
            "schema.lock.toml",
            f"lock target {lock.target!r} does not match input target {input_files.target_name!r}.",
        ),
    )


def validate_schema(
    *, lock: LockFile | None, schema_digest: str | None, policy: LockPolicy
) -> tuple[Diagnostic, ...]:
    if lock is None:
        return ()
    if schema_digest is None:
        return (
            Diagnostic(
                DiagnosticCode.INPUT_SCHEMA_MISSING,
                policy.schema_file,
                "schema.sql is missing from input.",
            ),
        )
    if schema_digest != lock.head_schema_digest:
        return (
            Diagnostic(
                DiagnosticCode.COHERENCE_SCHEMA_DIGEST_MISMATCH,
                policy.schema_file,
                "schema.sql digest differs from lock head_schema_digest.",
            ),
        )
    return ()


def validate_orphans(orphans: tuple[str, ...]) -> tuple[Diagnostic, ...]:
    return tuple(
        Diagnostic(
            DiagnosticCode.INPUT_ORPHAN_CHECKPOINT,
            path,
            "Checkpoint does not map to any migration in this input.",
        )
        for path in orphans
    )


def validate_step_alignment(
    *, lock: LockFile | None, steps: tuple[WorktreeStep, ...], policy: LockPolicy
) -> tuple[Diagnostic, ...]:
    if lock is None:
        return ()
    lock_by_file = {step.migration_file: step for step in lock.steps}
    step_by_file = {step.migration_file: step for step in steps}
    return (
        validate_lock_structure(lock=lock, steps=steps, policy=policy)
        + validate_step_coherence(steps=steps, lock_by_file=lock_by_file)
        + validate_missing_from_input(lock=lock, step_by_file=step_by_file)
    )


def validate_lock_structure(
    *,
    lock: LockFile,
    steps: tuple[WorktreeStep, ...],
    policy: LockPolicy,
) -> tuple[Diagnostic, ...]:
    rows: list[Diagnostic] = []
    for expected_index, lock_step in enumerate(lock.steps, start=1):
        if lock_step.index != expected_index:
            rows.append(
                Diagnostic(
                    DiagnosticCode.LOCKFILE_STEP_INDEX_INVALID,
                    lock_step.migration_file,
                    f"lock step index {lock_step.index} is not sequential at position {expected_index}.",
                )
            )

    if lock.head_index != len(lock.steps):
        rows.append(
            Diagnostic(
                DiagnosticCode.COHERENCE_HEAD_INDEX_MISMATCH,
                "schema.lock.toml",
                f"head_index={lock.head_index} while lock has {len(lock.steps)} steps.",
            )
        )
    if lock.head_index != len(steps):
        rows.append(
            Diagnostic(
                DiagnosticCode.COHERENCE_HEAD_INDEX_MISMATCH,
                "schema.lock.toml",
                f"head_index={lock.head_index} while input has {len(steps)} migrations.",
            )
        )

    expected_head_chain = (
        steps[-1].chain_hash if steps else policy.chain_seed(engine=lock.engine, target=lock.target)
    )
    if lock.head_chain_hash != expected_head_chain:
        rows.append(
            Diagnostic(
                DiagnosticCode.COHERENCE_HEAD_CHAIN_MISMATCH,
                "schema.lock.toml",
                "head_chain_hash differs from deterministic recomputation.",
            )
        )
    return tuple(rows)


def validate_step_coherence(
    *,
    steps: tuple[WorktreeStep, ...],
    lock_by_file: Mapping[str, LockStep],
) -> tuple[Diagnostic, ...]:
    rows: list[Diagnostic] = []
    for step in steps:
        lock_step = lock_by_file.get(step.migration_file)
        if lock_step is None:
            rows.append(
                Diagnostic(
                    DiagnosticCode.COHERENCE_NEW_IN_INPUT,
                    step.migration_file,
                    "Migration exists in input but not in lockfile.",
                )
            )
            continue

        if lock_step.index != step.index:
            rows.append(
                Diagnostic(
                    DiagnosticCode.COHERENCE_STEP_INDEX_MISMATCH,
                    step.migration_file,
                    f"lock step index {lock_step.index} does not match migration index {step.index}.",
                )
            )
        if lock_step.version != step.version:
            rows.append(
                Diagnostic(
                    DiagnosticCode.COHERENCE_STEP_VERSION_MISMATCH,
                    step.migration_file,
                    f"lock step version {lock_step.version!r} does not match {step.version!r}.",
                )
            )
        if lock_step.migration_digest != step.migration_digest:
            rows.append(
                Diagnostic(
                    DiagnosticCode.COHERENCE_MIGRATION_DIGEST_MISMATCH,
                    step.migration_file,
                    "Migration digest differs from lock step.",
                )
            )
        if lock_step.checkpoint_file != step.checkpoint_file:
            rows.append(
                Diagnostic(
                    DiagnosticCode.COHERENCE_CHECKPOINT_MISMATCH,
                    step.checkpoint_file,
                    "Checkpoint mapping differs from lock step.",
                )
            )

        if step.checkpoint_digest is None:
            rows.append(
                Diagnostic(
                    DiagnosticCode.COHERENCE_CHECKPOINT_MISSING,
                    step.checkpoint_file,
                    "Expected checkpoint file is missing.",
                )
            )
        else:
            if step.checkpoint_digest != lock_step.checkpoint_digest:
                rows.append(
                    Diagnostic(
                        DiagnosticCode.COHERENCE_CHECKPOINT_MISMATCH,
                        step.checkpoint_file,
                        "Checkpoint digest differs from lock step.",
                    )
                )
            if step.checkpoint_digest != lock_step.schema_digest:
                rows.append(
                    Diagnostic(
                        DiagnosticCode.COHERENCE_STEP_SCHEMA_MISMATCH,
                        step.checkpoint_file,
                        "Lock step schema_digest differs from deterministic checkpoint digest.",
                    )
                )

        if lock_step.chain_hash != step.chain_hash:
            rows.append(
                Diagnostic(
                    DiagnosticCode.COHERENCE_CHAIN_HASH_MISMATCH,
                    step.migration_file,
                    "Chain hash differs from deterministic recomputation.",
                )
            )
    return tuple(rows)


def validate_missing_from_input(
    *,
    lock: LockFile,
    step_by_file: Mapping[str, WorktreeStep],
) -> tuple[Diagnostic, ...]:
    return tuple(
        Diagnostic(
            DiagnosticCode.COHERENCE_MISSING_FROM_INPUT,
            lock_step.migration_file,
            "Lock references a migration that is absent from input.",
        )
        for lock_step in lock.steps
        if lock_step.migration_file not in step_by_file
    )


__all__ = ["validate_state"]
