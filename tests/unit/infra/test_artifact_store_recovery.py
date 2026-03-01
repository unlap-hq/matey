from __future__ import annotations

import sqlite3
from pathlib import Path

from matey.app.protocols import ArtifactDelete, ArtifactWrite
from matey.domain.target import TargetKey
from matey.infra.artifact_store import SqliteArtifactStore


def _set_state(store: SqliteArtifactStore, txid: str, state: str) -> None:
    with sqlite3.connect(store._db_path) as conn:
        conn.execute("UPDATE tx SET state=? WHERE txid=?", (state, txid))


def _tx_count(store: SqliteArtifactStore) -> int:
    with sqlite3.connect(store._db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM tx").fetchone()
    assert row is not None
    return int(row[0])


def test_recover_prepared_restores_original_state(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    target = repo / "db"
    target.mkdir(parents=True)
    schema = target / "schema.sql"
    schema.write_text("old\n", encoding="utf-8")

    store = SqliteArtifactStore(repo_root=repo)
    key = TargetKey("target")
    txid = store.begin(
        target_key=key,
        target_root=target,
        writes=(ArtifactWrite(rel_path="schema.sql", content=b"new\n"),),
        deletes=(),
    )

    store.recover_pending(target_key=key, target_root=target)
    assert schema.read_text(encoding="utf-8") == "old\n"
    assert _tx_count(store) == 0
    assert not (repo / ".matey" / "tx" / txid).exists()


def test_recover_applying_restores_from_backups(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    target = repo / "db"
    target.mkdir(parents=True)
    schema = target / "schema.sql"
    doomed = target / "orphan.sql"
    schema.write_text("old\n", encoding="utf-8")
    doomed.write_text("to-delete\n", encoding="utf-8")

    store = SqliteArtifactStore(repo_root=repo)
    key = TargetKey("target")
    txid = store.begin(
        target_key=key,
        target_root=target,
        writes=(ArtifactWrite(rel_path="schema.sql", content=b"new\n"),),
        deletes=(ArtifactDelete(rel_path="orphan.sql"),),
    )

    schema.write_text("new\n", encoding="utf-8")
    doomed.unlink()
    _set_state(store, txid, "applying")

    store.recover_pending(target_key=key, target_root=target)
    assert schema.read_text(encoding="utf-8") == "old\n"
    assert doomed.read_text(encoding="utf-8") == "to-delete\n"
    assert _tx_count(store) == 0


def test_recover_committed_finalizes_without_rollback(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    target = repo / "db"
    target.mkdir(parents=True)
    schema = target / "schema.sql"
    schema.write_text("old\n", encoding="utf-8")

    store = SqliteArtifactStore(repo_root=repo)
    key = TargetKey("target")
    txid = store.begin(
        target_key=key,
        target_root=target,
        writes=(ArtifactWrite(rel_path="schema.sql", content=b"new\n"),),
        deletes=(),
    )
    store.apply(txid=txid)
    _set_state(store, txid, "committed")

    store.recover_pending(target_key=key, target_root=target)
    assert schema.read_text(encoding="utf-8") == "new\n"
    assert _tx_count(store) == 0
