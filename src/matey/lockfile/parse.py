from __future__ import annotations

from collections.abc import Mapping
from pathlib import PurePosixPath

from mashumaro.exceptions import (
    ExtraKeysError,
    InvalidFieldValue,
    MissingDiscriminatorError,
    MissingField,
)

from matey.repo import Snapshot
from matey.sql import SqlTextDecodeError

from .model import (
    Diagnostic,
    DiagnosticCode,
    LockFile,
    LockPolicy,
    LockStep,
    WorktreeStep,
    generated_sql_digest,
)

_LOCKFILE_PARSE_ERRORS = (
    UnicodeDecodeError,
    ValueError,
    TypeError,
    InvalidFieldValue,
    MissingField,
    ExtraKeysError,
    MissingDiscriminatorError,
)


def parse_lockfile(lock_toml: bytes | None) -> tuple[LockFile | None, tuple[Diagnostic, ...]]:
    if lock_toml is None:
        return None, ()

    try:
        parsed = LockFile.from_toml(lock_toml.decode("utf-8"))
    except _LOCKFILE_PARSE_ERRORS as error:
        return None, (
            diag(
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
        normalized_paths, path_diagnostics = normalize_lock_step_paths(
            migration_file=step.migration_file,
            checkpoint_file=step.checkpoint_file,
            migrations_dir=parsed.migrations_dir,
            checkpoints_dir=parsed.checkpoints_dir,
        )
        diagnostics.extend(path_diagnostics)
        if normalized_paths is None:
            continue
        migration_file, checkpoint_file = normalized_paths

        duplicate_entry, duplicate_diagnostics = check_duplicate_lock_step(
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


def build_worktree_steps(
    input_files: Snapshot,
    *,
    policy: LockPolicy,
    lock: LockFile | None,
) -> tuple[tuple[WorktreeStep, ...], tuple[str, ...], tuple[Diagnostic, ...]]:
    migration_rows, migration_diagnostics = collect_sql_rows(
        input_files.migrations,
        required_prefix=f"{policy.migrations_dir}/",
        kind="migration",
    )
    checkpoint_rows, checkpoint_diagnostics = collect_sql_rows(
        input_files.checkpoints,
        required_prefix=f"{policy.checkpoints_dir}/",
        kind="checkpoint",
    )
    checkpoint_by_path = dict(checkpoint_rows)
    diagnostics: list[Diagnostic] = list(migration_diagnostics + checkpoint_diagnostics)

    seed_engine = lock.engine if lock is not None else ""
    seed_target = lock.target if lock is not None else input_files.target_name
    chain = policy.chain_seed(engine=seed_engine, target=seed_target)

    steps: list[WorktreeStep] = []
    for index, (migration_file, migration_payload) in enumerate(migration_rows, start=1):
        version = migration_version(PurePosixPath(migration_file).name)
        checkpoint_file = checkpoint_for_migration(migration_file=migration_file, policy=policy)
        migration_digest = policy.digest(migration_payload)
        try:
            checkpoint_digest = generated_sql_digest(
                checkpoint_by_path.get(checkpoint_file),
                policy=policy,
                label=checkpoint_file,
            )
        except SqlTextDecodeError as error:
            diagnostics.append(
                diag(
                    DiagnosticCode.INPUT_PATH_INVALID,
                    checkpoint_file,
                    str(error),
                )
            )
            checkpoint_digest = None
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
    return tuple(steps), orphans, tuple(diagnostics)


def schema_digest(schema_sql: bytes | None, *, policy: LockPolicy) -> str | None:
    return generated_sql_digest(schema_sql, policy=policy, label=policy.schema_file)


def checkpoint_for_migration(*, migration_file: str, policy: LockPolicy) -> str:
    migration_path = PurePosixPath(migration_file)
    relative_path = migration_path.relative_to(PurePosixPath(policy.migrations_dir))
    return (PurePosixPath(policy.checkpoints_dir) / relative_path).as_posix()


def migration_version(filename: str) -> str:
    stem = PurePosixPath(filename).name.removesuffix(".sql")
    if "_" in stem:
        prefix, _ = stem.split("_", 1)
        if prefix:
            return prefix
    return stem


def collect_sql_rows(
    values: Mapping[str, bytes],
    *,
    required_prefix: str,
    kind: str,
) -> tuple[tuple[tuple[str, bytes], ...], tuple[Diagnostic, ...]]:
    rows: dict[str, bytes] = {}
    diagnostics: list[Diagnostic] = []

    for raw_path, payload in values.items():
        try:
            path = normalize_rel_path(raw_path)
        except ValueError as error:
            diagnostics.append(
                diag(
                    DiagnosticCode.INPUT_PATH_INVALID,
                    raw_path,
                    f"Invalid {kind} path: {error}",
                )
            )
            continue

        if not path.startswith(required_prefix):
            diagnostics.append(
                diag(
                    DiagnosticCode.INPUT_PATH_INVALID,
                    path,
                    f"{kind} path must be under {required_prefix!r}; got {path!r}.",
                )
            )
            continue

        if not path.endswith(".sql"):
            diagnostics.append(
                diag(
                    DiagnosticCode.INPUT_PATH_INVALID,
                    path,
                    f"{kind} path must end with .sql.",
                )
            )
            continue

        if path in rows:
            diagnostics.append(
                diag(
                    DiagnosticCode.INPUT_PATH_DUPLICATE,
                    path,
                    f"Duplicate normalized {kind} path.",
                )
            )
            continue

        rows[path] = payload

    return tuple(sorted(rows.items(), key=lambda item: item[0])), tuple(diagnostics)


def normalize_lock_step_paths(
    *,
    migration_file: str,
    checkpoint_file: str,
    migrations_dir: str,
    checkpoints_dir: str,
) -> tuple[tuple[str, str] | None, tuple[Diagnostic, ...]]:
    diagnostics: list[Diagnostic] = []

    normalized_migration_file, migration_diag = normalize_lock_path(
        value=migration_file,
        kind="migration",
    )
    if migration_diag is not None:
        diagnostics.append(migration_diag)

    normalized_checkpoint_file, checkpoint_diag = normalize_lock_path(
        value=checkpoint_file,
        kind="checkpoint",
    )
    if checkpoint_diag is not None:
        diagnostics.append(checkpoint_diag)

    if normalized_migration_file is None or normalized_checkpoint_file is None:
        return None, tuple(diagnostics)

    if not normalized_migration_file.startswith(f"{migrations_dir}/"):
        diagnostics.append(
            diag(
                DiagnosticCode.LOCKFILE_STEP_PATH_MISMATCH,
                normalized_migration_file,
                f"Lock step migration path is outside migrations_dir={migrations_dir!r}.",
            )
        )

    if not normalized_checkpoint_file.startswith(f"{checkpoints_dir}/"):
        diagnostics.append(
            diag(
                DiagnosticCode.LOCKFILE_STEP_PATH_MISMATCH,
                normalized_checkpoint_file,
                f"Lock step checkpoint path is outside checkpoints_dir={checkpoints_dir!r}.",
            )
        )

    if diagnostics:
        return None, tuple(diagnostics)
    return (normalized_migration_file, normalized_checkpoint_file), ()


def check_duplicate_lock_step(
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
            diag(
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
            diag(
                DiagnosticCode.LOCKFILE_DUPLICATE_STEP_INDEX,
                migration_file,
                f"Lockfile contains duplicate step index {step_index}.",
            )
        )
        duplicate_entry = True
    else:
        seen_indices.add(step_index)

    return duplicate_entry, tuple(diagnostics)


def normalize_rel_path(path: str) -> str:
    normalized = PurePosixPath(path).as_posix()
    candidate = PurePosixPath(normalized)
    if not normalized or normalized == ".":
        raise ValueError("Path cannot be empty or current-directory.")
    if candidate.is_absolute():
        raise ValueError("Absolute paths are not allowed.")
    if any(part in {"..", "."} for part in candidate.parts):
        raise ValueError("Path traversal or dot-segment is not allowed.")
    return normalized


def normalize_lock_path(*, value: str, kind: str) -> tuple[str | None, Diagnostic | None]:
    try:
        normalized = normalize_rel_path(value)
    except ValueError as error:
        return None, diag(
            DiagnosticCode.LOCKFILE_STEP_PATH_INVALID,
            value,
            f"Invalid lock {kind} path: {error}",
        )
    return normalized, None


def diag(code: DiagnosticCode, path: str, detail: str) -> Diagnostic:
    return Diagnostic(code=code, path=path, detail=detail)


__all__ = [
    "build_worktree_steps",
    "checkpoint_for_migration",
    "parse_lockfile",
    "schema_digest",
]
