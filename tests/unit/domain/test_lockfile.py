import pytest

from matey.domain.errors import LockfileError
from matey.domain.lockfile import LockStep, SchemaLock, validate_lock_shape


def _make_lock() -> SchemaLock:
    return SchemaLock(
        lock_version=0,
        hash_algorithm="blake2b-256",
        canonicalizer="matey-sql-v0",
        engine="postgres",
        target="core",
        schema_file="schema.sql",
        migrations_dir="migrations",
        checkpoints_dir="checkpoints",
        head_index=1,
        head_chain_hash="abc",
        head_schema_digest="def",
        steps=(
            LockStep(
                index=1,
                version="202601010101",
                migration_file="migrations/202601010101_init.sql",
                migration_digest="aa",
                checkpoint_file="checkpoints/202601010101_init.sql",
                checkpoint_digest="bb",
                schema_digest="cc",
                chain_hash="dd",
            ),
        ),
    )


def test_validate_lock_shape_passes_for_valid_lock() -> None:
    validate_lock_shape(_make_lock())


def test_validate_lock_shape_rejects_path_traversal() -> None:
    lock = _make_lock()
    bad = SchemaLock(**{**lock.__dict__, "schema_file": "../schema.sql"})
    with pytest.raises(LockfileError):
        validate_lock_shape(bad)


def test_validate_lock_shape_rejects_head_index_mismatch() -> None:
    lock = _make_lock()
    bad = SchemaLock(**{**lock.__dict__, "head_index": 2})
    with pytest.raises(LockfileError):
        validate_lock_shape(bad)
