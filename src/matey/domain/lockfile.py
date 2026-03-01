from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath

from mashumaro.mixins.toml import DataClassTOMLMixin

from matey.domain.errors import LockfileError
from matey.domain.model import (
    CANONICALIZER,
    CHAIN_PREFIX,
    HASH_ALGORITHM,
    LOCK_VERSION,
    Engine,
    TargetKey,
)


@dataclass(frozen=True)
class LockStep(DataClassTOMLMixin):
    index: int
    version: str
    migration_file: str
    migration_digest: str
    checkpoint_file: str
    checkpoint_digest: str
    schema_digest: str
    chain_hash: str


@dataclass(frozen=True)
class SchemaLock(DataClassTOMLMixin):
    lock_version: int
    hash_algorithm: str
    canonicalizer: str
    engine: str
    target: str
    schema_file: str
    migrations_dir: str
    checkpoints_dir: str
    head_index: int
    head_chain_hash: str
    head_schema_digest: str
    steps: tuple[LockStep, ...]


def digest_bytes_blake2b256(payload: bytes) -> str:
    return hashlib.blake2b(payload, digest_size=32).hexdigest()


def digest_text_blake2b256(text: str) -> str:
    return digest_bytes_blake2b256(text.encode("utf-8"))


def lock_chain_seed(engine: Engine, target_key: TargetKey) -> str:
    seed = f"{CHAIN_PREFIX}|{engine.value}|{target_key.value}".encode()
    return digest_bytes_blake2b256(seed)


def lock_chain_step(prev: str, version: str, migration_file: str, migration_digest: str) -> str:
    payload = f"{prev}|{version}|{migration_file}|{migration_digest}".encode()
    return digest_bytes_blake2b256(payload)


def _validate_rel_path(*, value: str, field_name: str, must_be_under: str | None = None) -> None:
    path = PurePosixPath(value)
    if path.is_absolute():
        raise LockfileError(f"Invalid {field_name}: absolute paths are not allowed ({value}).")
    if ".." in path.parts:
        raise LockfileError(f"Invalid {field_name}: path traversal is not allowed ({value}).")
    if must_be_under is not None:
        root = PurePosixPath(must_be_under)
        try:
            path.relative_to(root)
        except ValueError as error:
            raise LockfileError(
                f"Invalid {field_name}: expected path under {must_be_under!r}, got {value!r}."
            ) from error


def validate_lock_shape(lock: SchemaLock) -> None:
    if lock.lock_version != LOCK_VERSION:
        raise LockfileError(f"Unsupported lock_version={lock.lock_version}; expected {LOCK_VERSION}.")
    if lock.hash_algorithm != HASH_ALGORITHM:
        raise LockfileError(
            f"Unsupported hash_algorithm={lock.hash_algorithm!r}; expected {HASH_ALGORITHM!r}."
        )
    if lock.canonicalizer != CANONICALIZER:
        raise LockfileError(
            f"Unsupported canonicalizer={lock.canonicalizer!r}; expected {CANONICALIZER!r}."
        )
    if lock.head_index != len(lock.steps):
        raise LockfileError(
            f"head_index mismatch: lock reports {lock.head_index} but contains {len(lock.steps)} steps."
        )

    _validate_rel_path(value=lock.schema_file, field_name="schema_file")
    _validate_rel_path(value=lock.migrations_dir, field_name="migrations_dir")
    _validate_rel_path(value=lock.checkpoints_dir, field_name="checkpoints_dir")

    versions_seen: set[str] = set()
    files_seen: set[str] = set()
    for expected_index, step in enumerate(lock.steps, start=1):
        if step.index != expected_index:
            raise LockfileError(
                f"Invalid step index ordering: expected {expected_index}, got {step.index}."
            )
        if step.version in versions_seen:
            raise LockfileError(f"Duplicate migration version in lockfile: {step.version}")
        versions_seen.add(step.version)

        if step.migration_file in files_seen:
            raise LockfileError(f"Duplicate migration_file in lockfile: {step.migration_file}")
        files_seen.add(step.migration_file)

        _validate_rel_path(
            value=step.migration_file,
            field_name=f"steps[{step.index}].migration_file",
            must_be_under=lock.migrations_dir,
        )
        _validate_rel_path(
            value=step.checkpoint_file,
            field_name=f"steps[{step.index}].checkpoint_file",
            must_be_under=lock.checkpoints_dir,
        )


def load_lock_from_text(text: str) -> SchemaLock:
    try:
        lock = SchemaLock.from_toml(text)
    except Exception as error:
        raise LockfileError(f"Unable to parse lockfile TOML: {error}") from error
    validate_lock_shape(lock)
    return lock


def recompute_lock_chains(
    *,
    steps: Sequence[LockStep],
    engine: Engine,
    target_key: TargetKey,
) -> tuple[str, ...]:
    chain = lock_chain_seed(engine, target_key)
    chains: list[str] = []
    for step in steps:
        chain = lock_chain_step(
            chain,
            step.version,
            step.migration_file,
            step.migration_digest,
        )
        chains.append(chain)
    return tuple(chains)


def first_lock_mismatch(
    *,
    lock: SchemaLock,
    steps: Sequence[LockStep],
    engine: Engine,
    target_key: TargetKey,
    compare_checkpoints: bool,
) -> tuple[int, str] | None:
    chains = recompute_lock_chains(
        steps=steps,
        engine=engine,
        target_key=target_key,
    )
    shared = min(len(lock.steps), len(steps))
    for idx in range(shared):
        lock_step = lock.steps[idx]
        step = steps[idx]
        if lock_step.migration_file != step.migration_file:
            return idx + 1, "migration_file"
        if lock_step.migration_digest != step.migration_digest:
            return idx + 1, "migration_digest"
        if compare_checkpoints:
            if lock_step.checkpoint_file != step.checkpoint_file:
                return idx + 1, "checkpoint_file"
            if lock_step.checkpoint_digest != step.checkpoint_digest:
                return idx + 1, "checkpoint_digest"
        if lock_step.chain_hash != chains[idx]:
            return idx + 1, "chain_hash"
    return None


def first_divergence_against_lock(
    *,
    lock: SchemaLock,
    steps: Sequence[LockStep],
    engine: Engine,
    target_key: TargetKey,
) -> int:
    mismatch = first_lock_mismatch(
        lock=lock,
        steps=steps,
        engine=engine,
        target_key=target_key,
        compare_checkpoints=False,
    )
    if mismatch is not None:
        return mismatch[0]
    if len(lock.steps) == len(steps):
        return len(steps) + 1
    return min(len(lock.steps), len(steps)) + 1
