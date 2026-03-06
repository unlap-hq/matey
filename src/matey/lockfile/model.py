from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from mashumaro.mixins.toml import DataClassTOMLMixin

from matey.sql import ensure_newline

DigestFn = Callable[[bytes], str]


def _digest_blake2b256(payload: bytes) -> str:
    return hashlib.blake2b(payload, digest_size=32).hexdigest()


def _generated_sql_bytes(payload: bytes | str) -> bytes:
    text = payload.decode("utf-8") if isinstance(payload, bytes) else payload
    return ensure_newline(text).encode("utf-8")


def _chain_payload(*parts: str) -> bytes:
    return json.dumps(parts, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


@dataclass(frozen=True, slots=True)
class LockPolicy:
    lock_version: int = 0
    hash_algorithm: str = "blake2b-256"
    canonicalizer: str = "matey-sql-v0"
    chain_prefix: str = "matey-lock-v0"
    schema_file: str = "schema.sql"
    migrations_dir: str = "migrations"
    checkpoints_dir: str = "checkpoints"
    digest: DigestFn = _digest_blake2b256

    def chain_seed(self, *, engine: str, target: str) -> str:
        return self.digest(_chain_payload(self.chain_prefix, engine, target))

    def chain_step(
        self, *, previous: str, version: str, migration_file: str, migration_digest: str
    ) -> str:
        return self.digest(_chain_payload(previous, version, migration_file, migration_digest))


class DiagnosticCode(StrEnum):
    LOCKFILE_PARSE_ERROR = "lockfile-parse-error"
    LOCKFILE_VERSION_MISMATCH = "lockfile-version-mismatch"
    LOCKFILE_HASH_ALGORITHM_MISMATCH = "lockfile-hash-algorithm-mismatch"
    LOCKFILE_CANONICALIZER_MISMATCH = "lockfile-canonicalizer-mismatch"
    LOCKFILE_SCHEMA_PATH_MISMATCH = "lockfile-schema-path-mismatch"
    LOCKFILE_MIGRATIONS_PATH_MISMATCH = "lockfile-migrations-path-mismatch"
    LOCKFILE_CHECKPOINTS_PATH_MISMATCH = "lockfile-checkpoints-path-mismatch"
    LOCKFILE_STEP_PATH_INVALID = "lockfile-step-path-invalid"
    LOCKFILE_STEP_PATH_MISMATCH = "lockfile-step-path-mismatch"
    LOCKFILE_DUPLICATE_MIGRATION = "lockfile-duplicate-migration"
    LOCKFILE_DUPLICATE_STEP_INDEX = "lockfile-duplicate-step-index"
    LOCKFILE_STEP_INDEX_INVALID = "lockfile-step-index-invalid"
    INPUT_PATH_INVALID = "input-path-invalid"
    INPUT_PATH_DUPLICATE = "input-path-duplicate"
    INPUT_SCHEMA_MISSING = "input-schema-missing"
    INPUT_ORPHAN_CHECKPOINT = "input-orphan-checkpoint"
    COHERENCE_TARGET_MISMATCH = "coherence-target-mismatch"
    COHERENCE_HEAD_INDEX_MISMATCH = "coherence-head-index-mismatch"
    COHERENCE_HEAD_CHAIN_MISMATCH = "coherence-head-chain-mismatch"
    COHERENCE_SCHEMA_DIGEST_MISMATCH = "coherence-schema-digest-mismatch"
    COHERENCE_NEW_IN_INPUT = "coherence-new-in-input"
    COHERENCE_MISSING_FROM_INPUT = "coherence-missing-from-input"
    COHERENCE_STEP_INDEX_MISMATCH = "coherence-step-index-mismatch"
    COHERENCE_STEP_VERSION_MISMATCH = "coherence-step-version-mismatch"
    COHERENCE_STEP_SCHEMA_MISMATCH = "coherence-step-schema-mismatch"
    COHERENCE_MIGRATION_DIGEST_MISMATCH = "coherence-migration-digest-mismatch"
    COHERENCE_CHECKPOINT_MISSING = "coherence-checkpoint-missing"
    COHERENCE_CHECKPOINT_MISMATCH = "coherence-checkpoint-mismatch"
    COHERENCE_CHAIN_HASH_MISMATCH = "coherence-chain-hash-mismatch"


@dataclass(frozen=True, slots=True)
class Diagnostic:
    code: DiagnosticCode
    path: str
    detail: str


@dataclass(frozen=True, slots=True)
class Step:
    index: int
    version: str
    migration_file: str
    migration_digest: str
    checkpoint_file: str
    chain_hash: str


@dataclass(frozen=True, slots=True)
class LockStep(Step, DataClassTOMLMixin):
    checkpoint_digest: str
    schema_digest: str


@dataclass(frozen=True, slots=True)
class WorktreeStep(Step):
    checkpoint_digest: str | None


@dataclass(frozen=True, slots=True)
class LockFile(DataClassTOMLMixin):
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


@dataclass(frozen=True, slots=True)
class LockState:
    target_name: str
    lock: LockFile | None
    worktree_steps: tuple[WorktreeStep, ...]
    schema_digest: str | None
    orphan_checkpoints: tuple[str, ...]
    diagnostics: tuple[Diagnostic, ...]

    @property
    def is_clean(self) -> bool:
        return not self.diagnostics


@dataclass(frozen=True, slots=True)
class Divergence:
    index: int
    field: str
    base_value: str
    head_value: str


def generated_sql_digest(payload: bytes | str | None, *, policy: LockPolicy) -> str | None:
    if payload is None:
        return None
    return policy.digest(_generated_sql_bytes(payload))


__all__ = [
    "Diagnostic",
    "DiagnosticCode",
    "Divergence",
    "LockFile",
    "LockPolicy",
    "LockState",
    "LockStep",
    "Step",
    "WorktreeStep",
    "generated_sql_digest",
]
