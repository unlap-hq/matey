from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from matey.config import TargetConfig
from matey.lockfile import LockFile, LockPolicy, LockStep, WorktreeStep, generated_sql_digest
from matey.sql import ensure_newline
from matey.tx import TxError, commit_artifacts

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
        checkpoint_path = target.dir / step.checkpoint_file
        artifacts[checkpoint_path] = ensure_newline(checkpoint_sql).encode("utf-8")
    return artifacts


def compute_artifact_delta(
    *,
    target: TargetConfig,
    desired_artifacts: Mapping[Path, bytes],
) -> tuple[dict[Path, bytes], tuple[Path, ...]]:
    writes: dict[Path, bytes] = {}
    for path, payload in desired_artifacts.items():
        if path.exists():
            if not path.is_file():
                raise SchemaError(f"Cannot write artifact to non-file path: {path}")
            if path.read_bytes() == payload:
                continue
        writes[path] = payload

    desired_checkpoints = {
        path.resolve()
        for path in desired_artifacts
        if path.resolve().is_relative_to(target.checkpoints.resolve())
    }
    existing_checkpoints: set[Path] = set()
    if target.checkpoints.exists():
        for path in target.checkpoints.rglob("*.sql"):
            if path.is_file():
                existing_checkpoints.add(path.resolve())

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
        changed_paths = commit_artifacts(target.dir, writes=writes, deletes=deletes)
    except TxError as error:
        raise SchemaError(f"apply: artifact commit failed: {error}") from error
    return tuple(sorted(relative_target_path(path, target) for path in changed_paths))


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
        checkpoints[step.checkpoint_file] = payload.decode("utf-8")

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
    engine: object,
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


def relative_target_path(path: Path, target: TargetConfig) -> str:
    try:
        return path.resolve().relative_to(target.dir.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


__all__ = [
    "apply_artifact_delta",
    "build_desired_artifacts",
    "build_lock_toml",
    "collect_checkpoint_texts",
    "compute_artifact_delta",
    "relative_target_path",
]
