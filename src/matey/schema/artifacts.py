from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from matey import Engine
from matey.lockfile import LockFile, LockPolicy, LockStep, WorktreeStep, generated_sql_digest
from matey.paths import (
    PathBoundaryError,
    describe_path_boundary_error,
    safe_descendant,
    safe_relative_descendant,
)
from matey.project import TargetConfig
from matey.sql import decode_sql_text, ensure_newline
from matey.tx import TxError, commit_artifacts
from matey.zero import zero_schema_sql

from .plan import SchemaError, StructuralPlan
from .replay import ReplayOutcome


def build_desired_artifacts(
    *,
    structural: StructuralPlan,
    replay: ReplayOutcome,
) -> dict[Path, bytes]:
    target = structural.target
    head_steps = structural.head_state.worktree_steps

    checkpoint_texts = collect_checkpoint_texts(
        structural=structural,
        replay=replay,
    )
    schema_sql = ensure_newline(replay.replay_schema_sql)
    lock_sql = build_lock_toml(
        policy=structural.policy,
        target=target,
        engine=structural.engine,
        steps=head_steps,
        checkpoint_texts=checkpoint_texts,
        schema_sql=schema_sql,
    )

    artifacts: dict[Path, bytes] = {
        target.schema: schema_sql.encode("utf-8"),
        target.lockfile: lock_sql.encode("utf-8"),
    }
    for step in head_steps:
        checkpoint_sql = checkpoint_texts.get(step.checkpoint_file)
        if checkpoint_sql is None:
            raise SchemaError(f"Missing checkpoint SQL for {step.checkpoint_file}.")
        checkpoint_path = target.root / step.checkpoint_file
        artifacts[checkpoint_path] = ensure_newline(checkpoint_sql).encode("utf-8")
    return artifacts


def build_zero_target_artifacts(
    *,
    target: TargetConfig,
    engine: Engine,
    policy: LockPolicy,
) -> dict[Path, bytes]:
    schema_sql = zero_schema_sql(engine=engine)
    lock_sql = build_lock_toml(
        policy=policy,
        target=target,
        engine=engine,
        steps=(),
        checkpoint_texts={},
        schema_sql=schema_sql,
    )
    return {
        target.schema: ensure_newline(schema_sql).encode("utf-8"),
        target.lockfile: lock_sql.encode("utf-8"),
    }


def compute_artifact_delta(
    *,
    target: TargetConfig,
    desired_artifacts: Mapping[Path, bytes],
) -> tuple[dict[Path, bytes], tuple[Path, ...]]:
    writes: dict[Path, bytes] = {}
    for path, payload in desired_artifacts.items():
        try:
            safe_path = safe_descendant(
                root=target.root,
                candidate=path,
                label=f"artifact path {path}",
                allow_missing_leaf=True,
                expected_kind="file",
            )
        except PathBoundaryError as error:
            raise SchemaError(describe_path_boundary_error(error)) from error
        if safe_path.exists() and safe_path.read_bytes() == payload:
            continue
        writes[safe_path] = payload

    target_root = target.root
    checkpoints_root = safe_descendant(
        root=target_root,
        candidate=target.checkpoints,
        label=f"checkpoints directory for target {target.name}",
        allow_missing_leaf=True,
        expected_kind="dir",
    )
    desired_checkpoints = {
        safe_descendant(
            root=target_root,
            candidate=path,
            label=f"checkpoint artifact {path}",
            allow_missing_leaf=True,
            expected_kind="file",
        )
        for path in desired_artifacts
        if path.is_relative_to(checkpoints_root)
    }
    existing_checkpoints: set[Path] = set()
    if checkpoints_root.exists():
        for path in checkpoints_root.rglob("*.sql"):
            safe_path = safe_descendant(
                root=target_root,
                candidate=path,
                label=f"checkpoint artifact {path}",
                allow_missing_leaf=False,
                expected_kind="file",
            )
            existing_checkpoints.add(safe_path)

    deletes = tuple(
        sorted(
            (path for path in existing_checkpoints if path not in desired_checkpoints),
            key=lambda path: path.as_posix(),
        )
    )
    return writes, deletes


def apply_artifact_delta(
    *,
    target: TargetConfig,
    writes: Mapping[Path, bytes],
    deletes: tuple[Path, ...],
) -> tuple[str, ...]:
    try:
        changed_paths = commit_artifacts(target.root, writes=writes, deletes=deletes)
    except TxError as error:
        raise SchemaError(f"apply: artifact commit failed: {error}") from error
    changed_files: list[str] = []
    for path in changed_paths:
        try:
            changed_files.append(
                safe_relative_descendant(
                    root=target.root,
                    candidate=path,
                    label=f"changed artifact {path}",
                    allow_missing_leaf=True,
                )
            )
        except PathBoundaryError as error:
            raise SchemaError(describe_path_boundary_error(error)) from error
    return tuple(sorted(changed_files))


def collect_checkpoint_texts(
    *,
    structural: StructuralPlan,
    replay: ReplayOutcome,
) -> dict[str, str]:
    checkpoints: dict[str, str] = {}

    for step in structural.head_state.worktree_steps[: structural.anchor_index]:
        payload = structural.head_snapshot.checkpoints.get(step.checkpoint_file)
        if payload is None:
            raise SchemaError(f"Missing unchanged checkpoint {step.checkpoint_file}.")
        checkpoints[step.checkpoint_file] = decode_sql_text(
            payload,
            label=f"checkpoint {step.checkpoint_file}",
        )

    for step in structural.tail_steps:
        checkpoint_sql = replay.checkpoint_sql_by_file.get(step.checkpoint_file)
        if checkpoint_sql is None:
            raise SchemaError(f"Missing replay checkpoint for {step.checkpoint_file}.")
        checkpoints[step.checkpoint_file] = checkpoint_sql
    return checkpoints


def build_lock_toml(
    *,
    policy: LockPolicy,
    target: TargetConfig,
    engine: Engine,
    steps: tuple[WorktreeStep, ...],
    checkpoint_texts: Mapping[str, str],
    schema_sql: str,
) -> str:
    chain = policy.chain_seed(engine=engine.value, target=target.name)
    lock_steps: list[LockStep] = []
    for index, step in enumerate(steps, start=1):
        checkpoint_sql = checkpoint_texts.get(step.checkpoint_file)
        if checkpoint_sql is None:
            raise SchemaError(f"Missing checkpoint SQL for lock step {step.checkpoint_file}.")
        checkpoint_digest = generated_sql_digest(checkpoint_sql, policy=policy)
        if checkpoint_digest is None:
            raise SchemaError(f"Unable to digest checkpoint SQL for {step.checkpoint_file}.")
        chain = policy.chain_step(
            previous=chain,
            version=step.version,
            migration_file=step.migration_file,
            migration_digest=step.migration_digest,
        )
        lock_steps.append(
            LockStep(
                index=index,
                version=step.version,
                migration_file=step.migration_file,
                migration_digest=step.migration_digest,
                checkpoint_file=step.checkpoint_file,
                checkpoint_digest=checkpoint_digest,
                schema_digest=checkpoint_digest,
                chain_hash=chain,
            )
        )

    head_schema_digest = generated_sql_digest(schema_sql, policy=policy)
    if head_schema_digest is None:
        raise SchemaError("Unable to digest schema.sql for lockfile output.")

    lock = LockFile(
        lock_version=policy.lock_version,
        hash_algorithm=policy.hash_algorithm,
        canonicalizer=policy.canonicalizer,
        engine=engine.value,
        target=target.name,
        schema_file=policy.schema_file,
        migrations_dir=policy.migrations_dir,
        checkpoints_dir=policy.checkpoints_dir,
        head_index=len(lock_steps),
        head_chain_hash=chain,
        head_schema_digest=head_schema_digest,
        steps=tuple(lock_steps),
    )
    return lock.to_toml()


__all__ = [
    "apply_artifact_delta",
    "build_desired_artifacts",
    "build_lock_toml",
    "build_zero_target_artifacts",
    "collect_checkpoint_texts",
    "compute_artifact_delta",
    "zero_schema_sql",
]
