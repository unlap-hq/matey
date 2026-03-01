from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from matey.domain.errors import TargetIdentityError


@dataclass(frozen=True)
class TargetId:
    name: str


@dataclass(frozen=True)
class TargetPaths:
    db_dir: Path
    migrations_dir: Path
    checkpoints_dir: Path
    schema_file: Path
    lock_file: Path


@dataclass(frozen=True)
class TargetKey:
    value: str


def canonical_db_dir_rel(*, repo_root: Path, db_dir: Path) -> str:
    repo = repo_root.resolve()
    candidate = db_dir.resolve()
    try:
        rel = candidate.relative_to(repo)
    except ValueError as error:
        raise TargetIdentityError(f"Target db dir is outside repository root: {candidate}") from error
    return rel.as_posix()


def derive_target_key(*, repo_root: Path, db_dir: Path) -> TargetKey:
    repo = repo_root.resolve().as_posix()
    rel = canonical_db_dir_rel(repo_root=repo_root, db_dir=db_dir)
    payload = f"{repo}|{rel}".encode()
    return TargetKey(value=hashlib.blake2b(payload, digest_size=32).hexdigest())


def assert_target_identity(*, repo_root: Path, db_dir: Path, target_key: TargetKey) -> None:
    derived = derive_target_key(repo_root=repo_root, db_dir=db_dir)
    if derived != target_key:
        raise TargetIdentityError(
            "Target key does not match target root identity. "
            f"expected={derived.value} got={target_key.value}"
        )
