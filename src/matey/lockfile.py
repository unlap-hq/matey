from __future__ import annotations

import hashlib
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import PurePosixPath

from mashumaro.mixins.toml import DataClassTOMLMixin

DigestFn = Callable[[bytes], str]


def _digest_blake2b256(payload: bytes) -> str:
    return hashlib.blake2b(payload, digest_size=32).hexdigest()


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
        seed = f"{self.chain_prefix}|{engine}|{target}".encode()
        return self.digest(seed)

    def chain_step(self, *, previous: str, version: str, migration_file: str, migration_digest: str) -> str:
        payload = f"{previous}|{version}|{migration_file}|{migration_digest}".encode()
        return self.digest(payload)


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
class LockInput:
    target_name: str
    schema_sql: bytes | None
    lock_toml: bytes | None
    migrations: Mapping[str, bytes]
    checkpoints: Mapping[str, bytes]


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


def _diag(
    code: DiagnosticCode,
    path: str,
    detail: str,
) -> Diagnostic:
    return Diagnostic(code=code, path=path, detail=detail)


def _normalize_rel_path(path: str) -> str:
    normalized = PurePosixPath(path).as_posix()
    candidate = PurePosixPath(normalized)
    if not normalized or normalized == ".":
        raise ValueError("Path cannot be empty or current-directory.")
    if candidate.is_absolute():
        raise ValueError("Absolute paths are not allowed.")
    if any(part in {"..", "."} for part in candidate.parts):
        raise ValueError("Path traversal or dot-segment is not allowed.")
    return normalized


def _collect_sql_rows(
    values: Mapping[str, bytes],
    *,
    required_prefix: str,
    kind: str,
) -> tuple[tuple[tuple[str, bytes], ...], tuple[Diagnostic, ...]]:
    rows: dict[str, bytes] = {}
    diagnostics: list[Diagnostic] = []

    for raw_path, payload in values.items():
        try:
            path = _normalize_rel_path(raw_path)
        except ValueError as error:
            diagnostics.append(
                _diag(
                    DiagnosticCode.INPUT_PATH_INVALID,
                    raw_path,
                    f"Invalid {kind} path: {error}",
                )
            )
            continue

        if not path.startswith(required_prefix):
            diagnostics.append(
                _diag(
                    DiagnosticCode.INPUT_PATH_INVALID,
                    path,
                    f"{kind} path must be under {required_prefix!r}; got {path!r}.",
                )
            )
            continue

        if not path.endswith(".sql"):
            diagnostics.append(
                _diag(
                    DiagnosticCode.INPUT_PATH_INVALID,
                    path,
                    f"{kind} path must end with .sql.",
                )
            )
            continue

        if path in rows:
            diagnostics.append(
                _diag(
                    DiagnosticCode.INPUT_PATH_DUPLICATE,
                    path,
                    f"Duplicate normalized {kind} path.",
                )
            )
            continue

        rows[path] = payload

    ordered = tuple(sorted(rows.items(), key=lambda item: item[0]))
    return ordered, tuple(diagnostics)


def _migration_version(filename: str) -> str:
    stem = PurePosixPath(filename).name.removesuffix(".sql")
    if "_" in stem:
        prefix, _ = stem.split("_", 1)
        if prefix:
            return prefix
    return stem


def _parse_lockfile(lock_toml: bytes | None) -> tuple[LockFile | None, tuple[Diagnostic, ...]]:
    if lock_toml is None:
        return None, ()

    try:
        parsed = LockFile.from_toml(lock_toml.decode("utf-8"))
    except Exception as error:  # pragma: no cover
        return None, (
            _diag(
                DiagnosticCode.LOCKFILE_PARSE_ERROR,
                "schema.lock.toml",
                f"Unable to parse lockfile: {error}",
            ),
        )

    diagnostics: list[Diagnostic] = []
    normalized_steps: list[LockStep] = []
    seen_files: set[str] = set()
    seen_indices: set[int] = set()

    for step in parsed.steps:
        normalized_paths, path_diagnostics = _normalize_lock_step_paths(
            migration_file=step.migration_file,
            checkpoint_file=step.checkpoint_file,
            migrations_dir=parsed.migrations_dir,
            checkpoints_dir=parsed.checkpoints_dir,
        )
        diagnostics.extend(path_diagnostics)
        if normalized_paths is None:
            continue
        migration_file, checkpoint_file = normalized_paths

        duplicate_entry, duplicate_diagnostics = _check_duplicate_lock_step(
            migration_file=migration_file,
            step_index=step.index,
            seen_files=seen_files,
            seen_indices=seen_indices,
        )
        diagnostics.extend(duplicate_diagnostics)
        if duplicate_entry:
            continue

        normalized_steps.append(
            LockStep(
                index=step.index,
                version=step.version,
                migration_file=migration_file,
                migration_digest=step.migration_digest,
                chain_hash=step.chain_hash,
                checkpoint_file=checkpoint_file,
                checkpoint_digest=step.checkpoint_digest,
                schema_digest=step.schema_digest,
            )
        )

    return (
        LockFile(
            lock_version=parsed.lock_version,
            hash_algorithm=parsed.hash_algorithm,
            canonicalizer=parsed.canonicalizer,
            engine=parsed.engine,
            target=parsed.target,
            schema_file=parsed.schema_file,
            migrations_dir=parsed.migrations_dir,
            checkpoints_dir=parsed.checkpoints_dir,
            head_index=parsed.head_index,
            head_chain_hash=parsed.head_chain_hash,
            head_schema_digest=parsed.head_schema_digest,
            steps=tuple(normalized_steps),
        ),
        tuple(diagnostics),
    )


def _normalize_lock_step_paths(
    *,
    migration_file: str,
    checkpoint_file: str,
    migrations_dir: str,
    checkpoints_dir: str,
) -> tuple[tuple[str, str] | None, tuple[Diagnostic, ...]]:
    diagnostics: list[Diagnostic] = []

    try:
        normalized_migration_file = _normalize_rel_path(migration_file)
    except ValueError as error:
        diagnostics.append(
            _diag(
                DiagnosticCode.LOCKFILE_STEP_PATH_INVALID,
                migration_file,
                f"Invalid lock migration path: {error}",
            )
        )
        normalized_migration_file = None

    try:
        normalized_checkpoint_file = _normalize_rel_path(checkpoint_file)
    except ValueError as error:
        diagnostics.append(
            _diag(
                DiagnosticCode.LOCKFILE_STEP_PATH_INVALID,
                checkpoint_file,
                f"Invalid lock checkpoint path: {error}",
            )
        )
        normalized_checkpoint_file = None

    if normalized_migration_file is None or normalized_checkpoint_file is None:
        return None, tuple(diagnostics)

    if not normalized_migration_file.startswith(f"{migrations_dir}/"):
        diagnostics.append(
            _diag(
                DiagnosticCode.LOCKFILE_STEP_PATH_MISMATCH,
                normalized_migration_file,
                f"Lock step migration path is outside migrations_dir={migrations_dir!r}.",
            )
        )

    if not normalized_checkpoint_file.startswith(f"{checkpoints_dir}/"):
        diagnostics.append(
            _diag(
                DiagnosticCode.LOCKFILE_STEP_PATH_MISMATCH,
                normalized_checkpoint_file,
                f"Lock step checkpoint path is outside checkpoints_dir={checkpoints_dir!r}.",
            )
        )

    if diagnostics:
        return None, tuple(diagnostics)
    return (normalized_migration_file, normalized_checkpoint_file), ()


def _check_duplicate_lock_step(
    *,
    migration_file: str,
    step_index: int,
    seen_files: set[str],
    seen_indices: set[int],
) -> tuple[bool, tuple[Diagnostic, ...]]:
    diagnostics: list[Diagnostic] = []
    duplicate_entry = False

    if migration_file in seen_files:
        diagnostics.append(
            _diag(
                DiagnosticCode.LOCKFILE_DUPLICATE_MIGRATION,
                migration_file,
                "Lockfile contains duplicate migration_file entries.",
            )
        )
        duplicate_entry = True
    else:
        seen_files.add(migration_file)

    if step_index in seen_indices:
        diagnostics.append(
            _diag(
                DiagnosticCode.LOCKFILE_DUPLICATE_STEP_INDEX,
                migration_file,
                f"Lockfile contains duplicate step index {step_index}.",
            )
        )
        duplicate_entry = True
    else:
        seen_indices.add(step_index)

    return duplicate_entry, tuple(diagnostics)


def _build_worktree_steps(
    input_files: LockInput,
    *,
    policy: LockPolicy,
    lock: LockFile | None,
) -> tuple[tuple[WorktreeStep, ...], tuple[str, ...], tuple[Diagnostic, ...]]:
    migration_rows, migration_diagnostics = _collect_sql_rows(
        input_files.migrations,
        required_prefix=f"{policy.migrations_dir}/",
        kind="migration",
    )
    checkpoint_rows, checkpoint_diagnostics = _collect_sql_rows(
        input_files.checkpoints,
        required_prefix=f"{policy.checkpoints_dir}/",
        kind="checkpoint",
    )
    checkpoint_by_path = dict(checkpoint_rows)

    seed_engine = lock.engine if lock is not None else ""
    seed_target = lock.target if lock is not None else input_files.target_name
    chain = policy.chain_seed(engine=seed_engine, target=seed_target)

    steps: list[WorktreeStep] = []
    for index, (migration_file, migration_payload) in enumerate(migration_rows, start=1):
        migration_name = PurePosixPath(migration_file).name
        stem = migration_name.removesuffix(".sql")
        version = _migration_version(migration_name)
        checkpoint_file = f"{policy.checkpoints_dir}/{stem}.sql"

        migration_digest = policy.digest(migration_payload)
        checkpoint_payload = checkpoint_by_path.get(checkpoint_file)
        checkpoint_digest = policy.digest(checkpoint_payload) if checkpoint_payload is not None else None

        chain = policy.chain_step(
            previous=chain,
            version=version,
            migration_file=migration_file,
            migration_digest=migration_digest,
        )

        steps.append(
            WorktreeStep(
                index=index,
                version=version,
                migration_file=migration_file,
                migration_digest=migration_digest,
                chain_hash=chain,
                checkpoint_file=checkpoint_file,
                checkpoint_digest=checkpoint_digest,
            )
        )

    expected_checkpoints = {step.checkpoint_file for step in steps}
    orphans = tuple(path for path, _ in checkpoint_rows if path not in expected_checkpoints)

    diagnostics = migration_diagnostics + checkpoint_diagnostics
    return tuple(steps), orphans, diagnostics


def _schema_digest(schema_sql: bytes | None, *, policy: LockPolicy) -> str | None:
    if schema_sql is None:
        return None
    return policy.digest(schema_sql)


def _validate_lock_header(*, lock: LockFile | None, policy: LockPolicy) -> tuple[Diagnostic, ...]:
    if lock is None:
        return ()

    checks = (
        (lock.lock_version != policy.lock_version, DiagnosticCode.LOCKFILE_VERSION_MISMATCH, f"lock_version={lock.lock_version}, expected {policy.lock_version}."),
        (lock.hash_algorithm != policy.hash_algorithm, DiagnosticCode.LOCKFILE_HASH_ALGORITHM_MISMATCH, f"hash_algorithm={lock.hash_algorithm!r}, expected {policy.hash_algorithm!r}."),
        (lock.canonicalizer != policy.canonicalizer, DiagnosticCode.LOCKFILE_CANONICALIZER_MISMATCH, f"canonicalizer={lock.canonicalizer!r}, expected {policy.canonicalizer!r}."),
        (lock.schema_file != policy.schema_file, DiagnosticCode.LOCKFILE_SCHEMA_PATH_MISMATCH, f"schema_file={lock.schema_file!r}, expected {policy.schema_file!r}."),
        (lock.migrations_dir != policy.migrations_dir, DiagnosticCode.LOCKFILE_MIGRATIONS_PATH_MISMATCH, f"migrations_dir={lock.migrations_dir!r}, expected {policy.migrations_dir!r}."),
        (lock.checkpoints_dir != policy.checkpoints_dir, DiagnosticCode.LOCKFILE_CHECKPOINTS_PATH_MISMATCH, f"checkpoints_dir={lock.checkpoints_dir!r}, expected {policy.checkpoints_dir!r}."),
    )

    return tuple(
        _diag(code, "schema.lock.toml", detail)
        for failed, code, detail in checks
        if failed
    )


def _validate_target_coherence(*, input_files: LockInput, lock: LockFile | None) -> tuple[Diagnostic, ...]:
    if lock is None:
        return ()
    if lock.target == input_files.target_name:
        return ()
    return (
        _diag(
            DiagnosticCode.COHERENCE_TARGET_MISMATCH,
            "schema.lock.toml",
            f"lock target {lock.target!r} does not match input target {input_files.target_name!r}.",
        ),
    )


def _validate_schema(*, lock: LockFile | None, schema_digest: str | None, policy: LockPolicy) -> tuple[Diagnostic, ...]:
    if lock is None:
        return ()
    if schema_digest is None:
        return (
            _diag(
                DiagnosticCode.INPUT_SCHEMA_MISSING,
                policy.schema_file,
                "schema.sql is missing from input.",
            ),
        )
    if schema_digest != lock.head_schema_digest:
        return (
            _diag(
                DiagnosticCode.COHERENCE_SCHEMA_DIGEST_MISMATCH,
                policy.schema_file,
                "schema.sql digest differs from lock head_schema_digest.",
            ),
        )
    return ()


def _validate_orphans(orphans: tuple[str, ...]) -> tuple[Diagnostic, ...]:
    return tuple(
        _diag(
            DiagnosticCode.INPUT_ORPHAN_CHECKPOINT,
            path,
            "Checkpoint does not map to any migration in this input.",
        )
        for path in orphans
    )


def _validate_step_alignment(*, lock: LockFile | None, steps: tuple[WorktreeStep, ...], policy: LockPolicy) -> tuple[Diagnostic, ...]:
    if lock is None:
        return ()

    rows: list[Diagnostic] = []
    lock_by_file = {step.migration_file: step for step in lock.steps}
    step_by_file = {step.migration_file: step for step in steps}

    for expected_index, lock_step in enumerate(lock.steps, start=1):
        if lock_step.index != expected_index:
            rows.append(
                _diag(
                    DiagnosticCode.LOCKFILE_STEP_INDEX_INVALID,
                    lock_step.migration_file,
                    f"lock step index {lock_step.index} is not sequential at position {expected_index}.",
                )
            )

    if lock.head_index != len(lock.steps):
        rows.append(
            _diag(
                DiagnosticCode.COHERENCE_HEAD_INDEX_MISMATCH,
                "schema.lock.toml",
                f"head_index={lock.head_index} while lock has {len(lock.steps)} steps.",
            )
        )
    if lock.head_index != len(steps):
        rows.append(
            _diag(
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
            _diag(
                DiagnosticCode.COHERENCE_HEAD_CHAIN_MISMATCH,
                "schema.lock.toml",
                "head_chain_hash differs from deterministic recomputation.",
            )
        )

    for step in steps:
        lock_step = lock_by_file.get(step.migration_file)
        if lock_step is None:
            rows.append(
                _diag(
                    DiagnosticCode.COHERENCE_NEW_IN_INPUT,
                    step.migration_file,
                    "Migration exists in input but not in lockfile.",
                )
            )
            continue

        if lock_step.index != step.index:
            rows.append(
                _diag(
                    DiagnosticCode.COHERENCE_STEP_INDEX_MISMATCH,
                    step.migration_file,
                    f"lock step index {lock_step.index} does not match migration index {step.index}.",
                )
            )
        if lock_step.version != step.version:
            rows.append(
                _diag(
                    DiagnosticCode.COHERENCE_STEP_VERSION_MISMATCH,
                    step.migration_file,
                    f"lock step version {lock_step.version!r} does not match {step.version!r}.",
                )
            )
        if lock_step.migration_digest != step.migration_digest:
            rows.append(
                _diag(
                    DiagnosticCode.COHERENCE_MIGRATION_DIGEST_MISMATCH,
                    step.migration_file,
                    "Migration digest differs from lock step.",
                )
            )
        if lock_step.checkpoint_file != step.checkpoint_file:
            rows.append(
                _diag(
                    DiagnosticCode.COHERENCE_CHECKPOINT_MISMATCH,
                    step.checkpoint_file,
                    "Checkpoint mapping differs from lock step.",
                )
            )

        match step.checkpoint_digest:
            case None:
                rows.append(
                    _diag(
                        DiagnosticCode.COHERENCE_CHECKPOINT_MISSING,
                        step.checkpoint_file,
                        "Expected checkpoint file is missing.",
                    )
                )
            case digest if digest != lock_step.checkpoint_digest:
                rows.append(
                    _diag(
                        DiagnosticCode.COHERENCE_CHECKPOINT_MISMATCH,
                        step.checkpoint_file,
                        "Checkpoint digest differs from lock step.",
                    )
                )
            case digest if digest != lock_step.schema_digest:
                rows.append(
                    _diag(
                        DiagnosticCode.COHERENCE_STEP_SCHEMA_MISMATCH,
                        step.checkpoint_file,
                        "Lock step schema_digest differs from deterministic checkpoint digest.",
                    )
                )
            case _:
                pass

        if lock_step.chain_hash != step.chain_hash:
            rows.append(
                _diag(
                    DiagnosticCode.COHERENCE_CHAIN_HASH_MISMATCH,
                    step.migration_file,
                    "Chain hash differs from deterministic recomputation.",
                )
            )

    rows.extend(
        _diag(
            DiagnosticCode.COHERENCE_MISSING_FROM_INPUT,
            lock_step.migration_file,
            "Lock references a migration that is absent from input.",
        )
        for lock_step in lock.steps
        if lock_step.migration_file not in step_by_file
    )

    return tuple(rows)


def build_lock_state(
    input_files: LockInput,
    *,
    policy: LockPolicy | None = None,
) -> LockState:
    effective_policy = policy or LockPolicy()

    lock, lock_diagnostics = _parse_lockfile(input_files.lock_toml)
    steps, orphans, snapshot_diagnostics = _build_worktree_steps(
        input_files,
        policy=effective_policy,
        lock=lock,
    )
    schema_digest = _schema_digest(input_files.schema_sql, policy=effective_policy)

    diagnostics = (
        lock_diagnostics
        + snapshot_diagnostics
        + _validate_lock_header(lock=lock, policy=effective_policy)
        + _validate_target_coherence(input_files=input_files, lock=lock)
        + _validate_schema(lock=lock, schema_digest=schema_digest, policy=effective_policy)
        + _validate_orphans(orphans)
    )
    if not diagnostics:
        diagnostics = diagnostics + _validate_step_alignment(
            lock=lock,
            steps=steps,
            policy=effective_policy,
        )

    return LockState(
        target_name=input_files.target_name,
        lock=lock,
        worktree_steps=steps,
        schema_digest=schema_digest,
        orphan_checkpoints=orphans,
        diagnostics=diagnostics,
    )


def _lock_signatures(state: LockState) -> tuple[tuple[str, str, str, str], ...]:
    return tuple(
        (
            step.version,
            step.migration_file,
            step.migration_digest,
            step.chain_hash,
        )
        for step in state.worktree_steps
    )


def first_lock_divergence(base: LockState, head: LockState) -> Divergence | None:
    if not base.is_clean or not head.is_clean:
        raise ValueError("Cannot compare divergence for non-clean lock states.")

    if base.target_name != head.target_name:
        return Divergence(
            index=1,
            field="target_name",
            base_value=base.target_name,
            head_value=head.target_name,
        )

    base_signatures = _lock_signatures(base)
    head_signatures = _lock_signatures(head)

    shared = min(len(base_signatures), len(head_signatures))
    for idx in range(shared):
        left = base_signatures[idx]
        right = head_signatures[idx]
        if left[0] != right[0]:
            return Divergence(index=idx + 1, field="version", base_value=left[0], head_value=right[0])
        if left[1] != right[1]:
            return Divergence(index=idx + 1, field="migration_file", base_value=left[1], head_value=right[1])
        if left[2] != right[2]:
            return Divergence(
                index=idx + 1,
                field="migration_digest",
                base_value=left[2],
                head_value=right[2],
            )
        if left[3] != right[3]:
            return Divergence(index=idx + 1, field="chain_hash", base_value=left[3], head_value=right[3])

    if len(base_signatures) == len(head_signatures):
        return None

    return Divergence(
        index=shared + 1,
        field="step_count",
        base_value=str(len(base_signatures)),
        head_value=str(len(head_signatures)),
    )


__all__ = [
    "Diagnostic",
    "DiagnosticCode",
    "Divergence",
    "LockFile",
    "LockInput",
    "LockPolicy",
    "LockState",
    "LockStep",
    "Step",
    "WorktreeStep",
    "build_lock_state",
    "first_lock_divergence",
]
