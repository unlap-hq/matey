from __future__ import annotations

from pathlib import Path
from typing import Any

from matey.domain import ResolvedPaths, SchemaValidationError
from matey.workflows.db_live import guarded_mutate_live_db
from matey.workflows.lockfile import SchemaLock


def _paths(tmp_path: Path) -> ResolvedPaths:
    db_dir = tmp_path / "db"
    migrations_dir = db_dir / "migrations"
    schema_file = db_dir / "schema.sql"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    schema_file.parent.mkdir(parents=True, exist_ok=True)
    schema_file.write_text("CREATE TABLE widgets (id INT);\n", encoding="utf-8")
    return ResolvedPaths(
        db_dir=db_dir,
        migrations_dir=migrations_dir,
        schema_file=schema_file,
    )


def _lock() -> SchemaLock:
    return SchemaLock(
        lock_version=1,
        hash_algorithm="blake2b-256",
        canonicalizer="matey-sql-v1",
        engine="postgres",
        target="default",
        schema_file="schema.sql",
        head_index=0,
        head_chain_hash="seed",
        head_schema_digest="digest",
        steps=(),
    )


def test_db_up_create_if_needed_recovers_status(monkeypatch, tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    verbs: list[str] = []
    status_calls = {"count": 0}

    def _fake_status(**kwargs: Any) -> int:
        status_calls["count"] += 1
        if status_calls["count"] == 1:
            raise SchemaValidationError("dbmate status failed on live target. database does not exist")
        return 0

    monkeypatch.setattr("matey.workflows.db_live.doctor_schema_lock", lambda **_: _lock())
    monkeypatch.setattr("matey.workflows.db_live._live_applied_index", _fake_status)
    monkeypatch.setattr("matey.workflows.db_live._expected_schema_for_index", lambda **_: None)

    def _fake_run_dbmate(**kwargs: Any) -> int:
        verbs.append(kwargs["verb"])
        return 0

    monkeypatch.setattr("matey.workflows.db_live.run_dbmate", _fake_run_dbmate)

    result = guarded_mutate_live_db(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        live_url="postgres://u:p@localhost/db",
        verb="up",
        down_steps=None,
        on_dbmate_result=None,
    )

    assert result.success is True
    assert verbs.count("create") == 1
    assert verbs.count("up") == 1


def test_db_up_attempts_create_when_status_fails_but_still_errors_if_unrecovered(
    monkeypatch,
    tmp_path: Path,
) -> None:
    paths = _paths(tmp_path)
    verbs: list[str] = []

    monkeypatch.setattr("matey.workflows.db_live.doctor_schema_lock", lambda **_: _lock())
    monkeypatch.setattr(
        "matey.workflows.db_live._live_applied_index",
        lambda **_: (_ for _ in ()).throw(
            SchemaValidationError("Live migration status does not match lockfile prefix.")
        ),
    )

    def _fake_run_dbmate(**kwargs: Any) -> int:
        verbs.append(kwargs["verb"])
        return 0

    monkeypatch.setattr("matey.workflows.db_live.run_dbmate", _fake_run_dbmate)

    result = guarded_mutate_live_db(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        live_url="postgres://u:p@localhost/db",
        verb="up",
        down_steps=None,
        on_dbmate_result=None,
    )

    assert result.success is False
    assert "lockfile prefix" in (result.error or "")
    assert verbs.count("create") == 1


def test_db_non_up_never_attempts_create_if_status_fails(monkeypatch, tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    verbs: list[str] = []

    monkeypatch.setattr("matey.workflows.db_live.doctor_schema_lock", lambda **_: _lock())
    monkeypatch.setattr(
        "matey.workflows.db_live._live_applied_index",
        lambda **_: (_ for _ in ()).throw(
            SchemaValidationError("dbmate status failed on live target. database does not exist")
        ),
    )

    def _fake_run_dbmate(**kwargs: Any) -> int:
        verbs.append(kwargs["verb"])
        return 0

    monkeypatch.setattr("matey.workflows.db_live.run_dbmate", _fake_run_dbmate)

    result = guarded_mutate_live_db(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        live_url="postgres://u:p@localhost/db",
        verb="rollback",
        down_steps=1,
        on_dbmate_result=None,
    )

    assert result.success is False
    assert "database does not exist" in (result.error or "")
    assert "create" not in verbs
