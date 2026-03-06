from __future__ import annotations

import hashlib
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from matey.core import LockfileError, ResolvedPaths
from matey.core.diff import normalize_sql_text

LOCKFILE_NAME = "schema.lock.toml"
LOCK_VERSION = 0
HASH_ALGORITHM = "blake2b-256"
CANONICALIZER = "matey-sql-v0"
CHAIN_PREFIX = "matey-lock-v0"

__all__ = [
    "CANONICALIZER",
    "HASH_ALGORITHM",
    "LOCKFILE_NAME",
    "LOCK_VERSION",
    "LockStep",
    "SchemaLock",
    "build_schema_lock",
    "doctor_schema_lock",
    "first_divergence_index",
    "load_schema_lock",
    "lockfile_path",
    "migration_file_names_for_steps",
    "sync_schema_lock",
    "write_schema_lock",
]


@dataclass(frozen=True)
class LockStep:
    index: int
    version: str
    migration_file: str
    migration_digest: str
    chain_hash: str
    checkpoint_file: str
    checkpoint_digest: str
    schema_digest: str


@dataclass(frozen=True)
class SchemaLock:
    lock_version: int
    hash_algorithm: str
    canonicalizer: str
    engine: str
    target: str
    schema_file: str
    head_index: int
    head_chain_hash: str
    head_schema_digest: str
    steps: tuple[LockStep, ...]


def lockfile_path(paths: ResolvedPaths) -> Path:
    return paths.db_dir / LOCKFILE_NAME


def _digest_bytes(payload: bytes) -> str:
    return hashlib.blake2b(payload, digest_size=32).hexdigest()


def _digest_file(path: Path) -> str:
    return _digest_bytes(path.read_bytes())


def _digest_sql_file(path: Path) -> str:
    normalized = normalize_sql_text(path.read_text(encoding="utf-8"))
    return _digest_bytes(normalized.encode("utf-8"))


def _digest_sql_text(sql: str) -> str:
    normalized = normalize_sql_text(sql)
    return _digest_bytes(normalized.encode("utf-8"))


def _chain_seed(*, engine: str, target: str) -> str:
    seed = f"{CHAIN_PREFIX}|{engine}|{target}"
    return _digest_bytes(seed.encode("utf-8"))


def _chain_hash(
    previous_chain_hash: str,
    *,
    version: str,
    migration_file: str,
    migration_digest: str,
) -> str:
    payload = f"{previous_chain_hash}|{version}|{migration_file}|{migration_digest}"
    return _digest_bytes(payload.encode("utf-8"))


def _step_from_mapping(raw: dict[str, Any], *, step_idx: int) -> LockStep:
    def _required_str(name: str) -> str:
        value = raw.get(name)
        if not isinstance(value, str) or not value.strip():
            raise LockfileError(f"Invalid lock step #{step_idx}: missing/invalid '{name}'.")
        return value

    index = raw.get("index")
    if not isinstance(index, int):
        raise LockfileError(f"Invalid lock step #{step_idx}: missing/invalid 'index'.")
    version = _required_str("version")
    migration_file = _required_str("migration_file")
    migration_digest = _required_str("migration_digest")
    chain_hash = _required_str("chain_hash")
    checkpoint_file = _required_str("checkpoint_file")
    checkpoint_digest = _required_str("checkpoint_digest")
    schema_digest = _required_str("schema_digest")
    return LockStep(
        index=index,
        version=version,
        migration_file=migration_file,
        migration_digest=migration_digest,
        chain_hash=chain_hash,
        checkpoint_file=checkpoint_file,
        checkpoint_digest=checkpoint_digest,
        schema_digest=schema_digest,
    )


def load_schema_lock(path: Path) -> SchemaLock:
    try:
        payload = path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise LockfileError(f"Lockfile not found: {path}") from error
    try:
        data = tomllib.loads(payload)
    except tomllib.TOMLDecodeError as error:
        raise LockfileError(f"Invalid lockfile TOML in {path}: {error}") from error

    def _required_str(name: str) -> str:
        value = data.get(name)
        if not isinstance(value, str) or not value.strip():
            raise LockfileError(f"Invalid lockfile: missing/invalid '{name}'.")
        return value

    def _required_int(name: str) -> int:
        value = data.get(name)
        if not isinstance(value, int):
            raise LockfileError(f"Invalid lockfile: missing/invalid '{name}'.")
        return value

    steps_raw = data.get("step")
    if steps_raw is None:
        steps_raw = []
    if not isinstance(steps_raw, list):
        raise LockfileError("Invalid lockfile: 'step' must be an array of tables.")

    steps = tuple(_step_from_mapping(raw, step_idx=idx + 1) for idx, raw in enumerate(steps_raw))
    return SchemaLock(
        lock_version=_required_int("lock_version"),
        hash_algorithm=_required_str("hash_algorithm"),
        canonicalizer=_required_str("canonicalizer"),
        engine=_required_str("engine"),
        target=_required_str("target"),
        schema_file=_required_str("schema_file"),
        head_index=_required_int("head_index"),
        head_chain_hash=_required_str("head_chain_hash"),
        head_schema_digest=_required_str("head_schema_digest"),
        steps=steps,
    )


def _toml_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def write_schema_lock(path: Path, lock: SchemaLock) -> None:
    lines: list[str] = [
        f"lock_version = {lock.lock_version}",
        f"hash_algorithm = {_toml_string(lock.hash_algorithm)}",
        f"canonicalizer = {_toml_string(lock.canonicalizer)}",
        f"engine = {_toml_string(lock.engine)}",
        f"target = {_toml_string(lock.target)}",
        f"schema_file = {_toml_string(lock.schema_file)}",
        f"head_index = {lock.head_index}",
        f"head_chain_hash = {_toml_string(lock.head_chain_hash)}",
        f"head_schema_digest = {_toml_string(lock.head_schema_digest)}",
    ]
    for step in lock.steps:
        lines.extend(
            [
                "",
                "[[step]]",
                f"index = {step.index}",
                f"version = {_toml_string(step.version)}",
                f"migration_file = {_toml_string(step.migration_file)}",
                f"migration_digest = {_toml_string(step.migration_digest)}",
                f"chain_hash = {_toml_string(step.chain_hash)}",
                f"checkpoint_file = {_toml_string(step.checkpoint_file)}",
                f"checkpoint_digest = {_toml_string(step.checkpoint_digest)}",
                f"schema_digest = {_toml_string(step.schema_digest)}",
            ]
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _migration_version_from_name(name: str) -> str:
    stem = Path(name).stem
    if "_" in stem:
        version, _ = stem.split("_", 1)
        if version:
            return version
    return stem


def _sorted_migrations(paths: ResolvedPaths) -> list[Path]:
    if not paths.migrations_dir.exists():
        return []
    migrations = [
        path for path in paths.migrations_dir.iterdir() if path.is_file() and path.suffix == ".sql"
    ]
    return sorted(migrations, key=lambda item: item.name)


def _relative_to_db(path: Path, *, db_dir: Path) -> str:
    try:
        return path.relative_to(db_dir).as_posix()
    except ValueError as error:
        raise LockfileError(f"Path {path} is outside target db dir {db_dir}.") from error


def _existing_checkpoint_map(existing_lock: SchemaLock | None) -> dict[str, str]:
    if existing_lock is None:
        return {}
    return {step.migration_file: step.checkpoint_file for step in existing_lock.steps}


def _default_checkpoint_rel(migration_path: Path) -> str:
    return f"checkpoints/{migration_path.stem}.sql"


def _resolve_lock_relative_path(*, db_dir: Path, relative_path: str, field_name: str) -> Path:
    candidate_rel = Path(relative_path)
    if candidate_rel.is_absolute():
        raise LockfileError(
            f"Invalid {field_name}: absolute paths are not allowed: {relative_path}"
        )

    db_root = db_dir.resolve()
    candidate = (db_dir / candidate_rel).resolve()
    if not candidate.is_relative_to(db_root):
        raise LockfileError(
            f"Invalid {field_name}: path escapes target db dir ({db_dir}): {relative_path}"
        )
    return candidate


def _build_lock_from_artifacts(
    *,
    paths: ResolvedPaths,
    engine: str,
    target: str,
    existing_lock: SchemaLock | None,
    schema_sql_override: str | None = None,
) -> SchemaLock:
    if schema_sql_override is None and not paths.schema_file.exists():
        raise LockfileError(f"Schema file does not exist: {paths.schema_file}")

    checkpoint_map = _existing_checkpoint_map(existing_lock)
    seed = _chain_seed(engine=engine, target=target)
    previous_chain = seed
    steps: list[LockStep] = []

    for index, migration_path in enumerate(_sorted_migrations(paths), start=1):
        migration_rel = _relative_to_db(migration_path, db_dir=paths.db_dir)
        migration_digest = _digest_file(migration_path)
        version = _migration_version_from_name(migration_path.name)
        chain_hash = _chain_hash(
            previous_chain,
            version=version,
            migration_file=migration_rel,
            migration_digest=migration_digest,
        )
        checkpoint_rel = checkpoint_map.get(migration_rel, _default_checkpoint_rel(migration_path))
        checkpoint_path = paths.db_dir / checkpoint_rel
        if not checkpoint_path.exists():
            raise LockfileError(
                f"Missing checkpoint for migration '{migration_rel}': expected {checkpoint_path}"
            )
        step = LockStep(
            index=index,
            version=version,
            migration_file=migration_rel,
            migration_digest=migration_digest,
            chain_hash=chain_hash,
            checkpoint_file=checkpoint_rel,
            checkpoint_digest=_digest_file(checkpoint_path),
            schema_digest=_digest_sql_file(checkpoint_path),
        )
        steps.append(step)
        previous_chain = chain_hash

    schema_digest = (
        _digest_sql_text(schema_sql_override)
        if schema_sql_override is not None
        else _digest_sql_file(paths.schema_file)
    )
    if steps and steps[-1].schema_digest != schema_digest:
        raise LockfileError(
            "schema.sql digest does not match latest checkpoint schema digest. "
            "Run schema regeneration before syncing lockfile."
        )

    head_chain_hash = steps[-1].chain_hash if steps else seed
    head_schema_digest = steps[-1].schema_digest if steps else schema_digest
    return SchemaLock(
        lock_version=LOCK_VERSION,
        hash_algorithm=HASH_ALGORITHM,
        canonicalizer=CANONICALIZER,
        engine=engine,
        target=target,
        schema_file=_relative_to_db(paths.schema_file, db_dir=paths.db_dir),
        head_index=len(steps),
        head_chain_hash=head_chain_hash,
        head_schema_digest=head_schema_digest,
        steps=tuple(steps),
    )


def _validate_lock_shape(lock: SchemaLock) -> None:
    if lock.lock_version != LOCK_VERSION:
        raise LockfileError(
            f"Unsupported lock_version={lock.lock_version}; expected {LOCK_VERSION}."
        )
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
            f"head_index={lock.head_index} does not match step count={len(lock.steps)}."
        )


def verify_schema_lock(
    lock: SchemaLock,
    *,
    db_dir: Path,
) -> None:
    _validate_lock_shape(lock)

    previous_chain = _chain_seed(engine=lock.engine, target=lock.target)
    seen_versions: set[str] = set()
    expected_index = 1

    for step in lock.steps:
        if step.index != expected_index:
            raise LockfileError(
                f"Invalid lock step index: expected {expected_index}, found {step.index}."
            )
        expected_index += 1
        if step.version in seen_versions:
            raise LockfileError(f"Duplicate migration version in lockfile: {step.version}")
        seen_versions.add(step.version)

        migration_path = _resolve_lock_relative_path(
            db_dir=db_dir,
            relative_path=step.migration_file,
            field_name="step.migration_file",
        )
        checkpoint_path = _resolve_lock_relative_path(
            db_dir=db_dir,
            relative_path=step.checkpoint_file,
            field_name="step.checkpoint_file",
        )
        if not migration_path.exists():
            raise LockfileError(f"Missing migration file referenced by lock: {migration_path}")
        if not checkpoint_path.exists():
            raise LockfileError(f"Missing checkpoint file referenced by lock: {checkpoint_path}")

        migration_digest = _digest_file(migration_path)
        if migration_digest != step.migration_digest:
            raise LockfileError(
                f"Migration digest mismatch for {step.migration_file}: "
                f"expected {step.migration_digest}, got {migration_digest}."
            )

        checkpoint_digest = _digest_file(checkpoint_path)
        if checkpoint_digest != step.checkpoint_digest:
            raise LockfileError(
                f"Checkpoint digest mismatch for {step.checkpoint_file}: "
                f"expected {step.checkpoint_digest}, got {checkpoint_digest}."
            )

        checkpoint_schema_digest = _digest_sql_file(checkpoint_path)
        if checkpoint_schema_digest != step.schema_digest:
            raise LockfileError(
                f"Checkpoint schema digest mismatch for {step.checkpoint_file}: "
                f"expected {step.schema_digest}, got {checkpoint_schema_digest}."
            )

        expected_chain_hash = _chain_hash(
            previous_chain,
            version=step.version,
            migration_file=step.migration_file,
            migration_digest=step.migration_digest,
        )
        if expected_chain_hash != step.chain_hash:
            raise LockfileError(
                f"Chain hash mismatch at step index {step.index}: "
                f"expected {expected_chain_hash}, got {step.chain_hash}."
            )
        previous_chain = step.chain_hash

    expected_head_chain = lock.steps[-1].chain_hash if lock.steps else previous_chain
    expected_head_schema = (
        lock.steps[-1].schema_digest if lock.steps else _digest_sql_file(db_dir / lock.schema_file)
    )

    if lock.head_chain_hash != expected_head_chain:
        raise LockfileError(
            f"head_chain_hash mismatch: expected {expected_head_chain}, got {lock.head_chain_hash}."
        )
    if lock.head_schema_digest != expected_head_schema:
        raise LockfileError(
            "head_schema_digest mismatch against lock step/schema state: "
            f"expected {expected_head_schema}, got {lock.head_schema_digest}."
        )

    schema_path = _resolve_lock_relative_path(
        db_dir=db_dir,
        relative_path=lock.schema_file,
        field_name="schema_file",
    )
    if not schema_path.exists():
        raise LockfileError(f"Missing schema file referenced by lock: {schema_path}")
    schema_digest = _digest_sql_file(schema_path)
    if schema_digest != lock.head_schema_digest:
        raise LockfileError(
            f"schema.sql digest mismatch: expected {lock.head_schema_digest}, got {schema_digest}."
        )


def first_divergence_index(*, base: SchemaLock, head: SchemaLock) -> int:
    shared = min(len(base.steps), len(head.steps))
    for index in range(shared):
        if base.steps[index].chain_hash != head.steps[index].chain_hash:
            return index + 1
    if len(base.steps) == len(head.steps):
        return len(head.steps) + 1
    return shared + 1


def migration_file_names_for_steps(steps: tuple[LockStep, ...]) -> list[str]:
    return [Path(step.migration_file).name for step in steps]


def doctor_schema_lock(*, paths: ResolvedPaths) -> SchemaLock:
    lock_path = lockfile_path(paths)
    lock = load_schema_lock(lock_path)
    verify_schema_lock(lock, db_dir=paths.db_dir)
    return lock


def sync_schema_lock(
    *,
    paths: ResolvedPaths,
    engine: str,
    target: str,
) -> SchemaLock:
    lock = build_schema_lock(
        paths=paths,
        engine=engine,
        target=target,
    )
    write_schema_lock(lockfile_path(paths), lock)
    return lock


def build_schema_lock(
    *,
    paths: ResolvedPaths,
    engine: str,
    target: str,
    schema_sql_override: str | None = None,
) -> SchemaLock:
    lock_path = lockfile_path(paths)
    existing_lock = load_schema_lock(lock_path) if lock_path.exists() else None
    return _build_lock_from_artifacts(
        paths=paths,
        engine=engine,
        target=target,
        existing_lock=existing_lock,
        schema_sql_override=schema_sql_override,
    )
