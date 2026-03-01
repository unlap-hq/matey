from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Literal


class Engine(StrEnum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"


def parse_engine(raw: str) -> Engine:
    value = raw.strip().lower()
    for engine in Engine:
        if engine.value == value:
            return engine
    raise ValueError(f"Unsupported engine: {raw!r}")


LOCK_VERSION = 0
HASH_ALGORITHM = "blake2b-256"
CANONICALIZER = "matey-sql-v0"
LOCK_FILENAME = "schema.lock.toml"
SCHEMA_FILENAME = "schema.sql"
MIGRATIONS_DIRNAME = "migrations"
CHECKPOINTS_DIRNAME = "checkpoints"
CHAIN_PREFIX = "matey-lock-v0"


@dataclass(frozen=True)
class ConfigDefaults:
    dir: str = "db"
    url_env: str = "MATEY_URL"
    test_url_env: str = "MATEY_TEST_URL"


@dataclass(frozen=True)
class ConfigTarget:
    dir: str | None = None
    url_env: str | None = None
    test_url_env: str | None = None


@dataclass(frozen=True)
class MateyConfig:
    defaults: ConfigDefaults = ConfigDefaults()
    targets: dict[str, ConfigTarget] | None = None


@dataclass(frozen=True)
class ResolvedTargetConfig:
    name: str
    db_dir: Path
    url_env: str
    test_url_env: str


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
    from matey.domain.errors import TargetIdentityError

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
    from matey.domain.errors import TargetIdentityError

    derived = derive_target_key(repo_root=repo_root, db_dir=db_dir)
    if derived != target_key:
        raise TargetIdentityError(
            "Target key does not match target root identity. "
            f"expected={derived.value} got={target_key.value}"
        )


SqlOrigin = Literal["artifact", "scratch_dump", "live_dump"]


@dataclass(frozen=True)
class SqlSource:
    text: str
    origin: SqlOrigin
    context_url: str | None = None


@dataclass(frozen=True)
class PreparedSql:
    normalized: str
    digest: str


@dataclass(frozen=True)
class SqlComparison:
    expected: PreparedSql
    actual: PreparedSql
    equal: bool
    diff: str | None


@dataclass(frozen=True)
class SchemaOpContext:
    target_id: TargetId
    target_key: TargetKey
    target_paths: TargetPaths
    repo_root: Path
    base_ref: str | None
    replay_engine: Engine
    test_url: str | None
    clean: bool
    keep_scratch: bool
    run_nonce: str


@dataclass(frozen=True)
class DbOpContext:
    target_id: TargetId
    target_key: TargetKey
    target_paths: TargetPaths
    live_engine: Engine
    live_url: str
    test_url: str | None
    keep_scratch: bool
    run_nonce: str


@dataclass(frozen=True)
class SchemaStatusRow:
    marker: Literal["ok", "warn", "error"]
    migration_file: str
    status: str
    detail: str


@dataclass(frozen=True)
class SchemaStatusResult:
    up_to_date: bool
    stale: bool
    rows: tuple[SchemaStatusRow, ...]
    summary: tuple[str, ...]


@dataclass(frozen=True)
class SchemaPlanResult:
    comparison: SqlComparison
    replay_scratch_url: str
    down_checked: bool
    orphan_checkpoints: tuple[str, ...]


@dataclass(frozen=True)
class DbPlanResult:
    comparison: SqlComparison
    live_applied_index: int
