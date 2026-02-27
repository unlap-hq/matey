from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from matey.cli.common import resolve_lock_engine_for_sync
from matey.domain import LockfileError, ResolvedPaths
from matey.workflows.lockfile import (
    LockStep,
    SchemaLock,
    doctor_schema_lock,
    sync_schema_lock,
    write_schema_lock,
)
from matey.workflows.schema_diff import normalize_sql_text


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _digest_bytes(payload: bytes) -> str:
    return hashlib.blake2b(payload, digest_size=32).hexdigest()


def _digest_sql_text(sql: str) -> str:
    return _digest_bytes(normalize_sql_text(sql).encode("utf-8"))


def _create_basic_target(tmp_path: Path) -> ResolvedPaths:
    db_dir = tmp_path / "db"
    migrations_dir = db_dir / "migrations"
    schema_file = db_dir / "schema.sql"
    _write(
        migrations_dir / "202601010000_init.sql",
        "-- migrate:up\nCREATE TABLE widgets (id INT);\n-- migrate:down\nDROP TABLE widgets;\n",
    )
    _write(db_dir / "checkpoints" / "202601010000_init.sql", "CREATE TABLE widgets (id INT);\n")
    _write(schema_file, "CREATE TABLE widgets (id INT);\n")
    return ResolvedPaths(
        db_dir=db_dir,
        migrations_dir=migrations_dir,
        schema_file=schema_file,
    )


def test_lock_doctor_allows_new_migrations_not_yet_in_lock(tmp_path: Path) -> None:
    paths = _create_basic_target(tmp_path)
    sync_schema_lock(paths=paths, engine="postgres", target="default")

    _write(
        paths.migrations_dir / "202601010001_add_col.sql",
        "-- migrate:up\nALTER TABLE widgets ADD COLUMN payload TEXT;\n"
        "-- migrate:down\nALTER TABLE widgets DROP COLUMN payload;\n",
    )

    lock = doctor_schema_lock(paths=paths)
    assert lock.head_index == 1


def test_lock_doctor_rejects_relative_path_escape_in_step_files(tmp_path: Path) -> None:
    paths = _create_basic_target(tmp_path)
    sync_schema_lock(paths=paths, engine="postgres", target="default")

    external_sql = tmp_path / "outside.sql"
    _write(external_sql, "CREATE TABLE widgets (id INT);\n")
    migration_digest = _digest_bytes(external_sql.read_bytes())
    checkpoint_digest = _digest_bytes(external_sql.read_bytes())
    schema_digest = _digest_sql_text(external_sql.read_text(encoding="utf-8"))

    seed = _digest_bytes(b"matey-lock-v1|postgres|default")
    chain_hash = _digest_bytes(
        f"{seed}|202601010000|../outside.sql|{migration_digest}".encode()
    )

    malicious_lock = SchemaLock(
        lock_version=1,
        hash_algorithm="blake2b-256",
        canonicalizer="matey-sql-v1",
        engine="postgres",
        target="default",
        schema_file="schema.sql",
        head_index=1,
        head_chain_hash=chain_hash,
        head_schema_digest=schema_digest,
        steps=(
            LockStep(
                index=1,
                version="202601010000",
                migration_file="../outside.sql",
                migration_digest=migration_digest,
                chain_hash=chain_hash,
                checkpoint_file="../outside.sql",
                checkpoint_digest=checkpoint_digest,
                schema_digest=schema_digest,
            ),
        ),
    )
    write_schema_lock(paths.db_dir / "schema.lock.toml", malicious_lock)

    with pytest.raises(LockfileError, match="escapes target db dir"):
        doctor_schema_lock(paths=paths)


def test_lock_doctor_rejects_relative_path_escape_in_schema_file(tmp_path: Path) -> None:
    paths = _create_basic_target(tmp_path)
    lock = sync_schema_lock(paths=paths, engine="postgres", target="default")

    malicious_lock = SchemaLock(
        lock_version=lock.lock_version,
        hash_algorithm=lock.hash_algorithm,
        canonicalizer=lock.canonicalizer,
        engine=lock.engine,
        target=lock.target,
        schema_file="../schema.sql",
        head_index=lock.head_index,
        head_chain_hash=lock.head_chain_hash,
        head_schema_digest=lock.head_schema_digest,
        steps=lock.steps,
    )
    write_schema_lock(paths.db_dir / "schema.lock.toml", malicious_lock)

    with pytest.raises(LockfileError, match="schema_file"):
        doctor_schema_lock(paths=paths)


def test_lock_sync_engine_resolution_rejects_url_engine_mismatch(tmp_path: Path) -> None:
    paths = _create_basic_target(tmp_path)
    sync_schema_lock(paths=paths, engine="postgres", target="default")

    with pytest.raises(LockfileError, match="Engine mismatch"):
        resolve_lock_engine_for_sync(
            paths=paths,
            real_url=None,
            test_url="mysql://root:pass@localhost/db",
        )
