from __future__ import annotations

import os
import shutil
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path

from matey.contracts import IArtifactStore
from matey.errors import ArtifactRecoveryError, ArtifactTransactionError
from matey.models import ArtifactDelete, ArtifactWrite, TargetKey
from matey.platform import resolve_inside


def _fsync_directory(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _fsync_file(path: Path) -> None:
    try:
        with path.open("rb") as handle:
            os.fsync(handle.fileno())
    except OSError:
        return


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    _fsync_file(dst)
    _fsync_directory(dst.parent)


def _write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp.{uuid.uuid4().hex}")
    with tmp.open("wb") as handle:
        handle.write(payload)
        handle.flush()
        os.fsync(handle.fileno())
    tmp.replace(path)
    _fsync_directory(path.parent)


def _raise_missing_staged(*, txid: str, rel_path: str) -> None:
    raise ArtifactTransactionError(f"Missing staged artifact for {txid}: {rel_path}")


@dataclass(frozen=True)
class _TxRow:
    txid: str
    target_key: str
    target_root: str
    state: str


@dataclass(frozen=True)
class _WriteRow:
    rel_path: str
    staged_path: str
    backup_path: str | None


@dataclass(frozen=True)
class _DeleteRow:
    rel_path: str
    backup_path: str | None


class SqliteArtifactStore(IArtifactStore):
    def __init__(self, *, repo_root: Path) -> None:
        self._repo_root = repo_root.resolve()
        self._meta_dir = self._repo_root / ".matey"
        self._tx_dir = self._meta_dir / "tx"
        self._db_path = self._meta_dir / "tx.db"
        self._meta_dir.mkdir(parents=True, exist_ok=True)
        self._tx_dir.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=FULL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS tx (
                    txid TEXT PRIMARY KEY,
                    target_key TEXT NOT NULL,
                    target_root TEXT NOT NULL,
                    state TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
                );

                CREATE TABLE IF NOT EXISTS tx_write (
                    txid TEXT NOT NULL,
                    rel_path TEXT NOT NULL,
                    staged_path TEXT NOT NULL,
                    backup_path TEXT,
                    PRIMARY KEY (txid, rel_path),
                    FOREIGN KEY (txid) REFERENCES tx(txid) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS tx_delete (
                    txid TEXT NOT NULL,
                    rel_path TEXT NOT NULL,
                    backup_path TEXT,
                    PRIMARY KEY (txid, rel_path),
                    FOREIGN KEY (txid) REFERENCES tx(txid) ON DELETE CASCADE
                );
                """
            )

    def recover_pending(self, *, target_key: TargetKey, target_root: Path) -> None:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT txid, target_key, target_root, state FROM tx WHERE target_key=?",
                    (target_key.value,),
                ).fetchall()
                for row in rows:
                    tx = _TxRow(
                        txid=str(row["txid"]),
                        target_key=str(row["target_key"]),
                        target_root=str(row["target_root"]),
                        state=str(row["state"]),
                    )
                    if Path(tx.target_root).resolve() != target_root.resolve():
                        continue
                    if tx.state == "committed":
                        self.finalize(txid=tx.txid)
                        continue
                    if tx.state in {"prepared", "applying"}:
                        self._rollback_tx(txid=tx.txid, target_root=target_root)
                        self.finalize(txid=tx.txid)
        except Exception as error:
            raise ArtifactRecoveryError(
                f"Failed artifact recovery for {target_root}: {error}"
            ) from error

    def begin(
        self,
        *,
        target_key: TargetKey,
        target_root: Path,
        writes: tuple[ArtifactWrite, ...],
        deletes: tuple[ArtifactDelete, ...],
    ) -> str:
        root = target_root.resolve()
        txid = uuid.uuid4().hex
        tx_root = self._tx_dir / txid
        staged_root = tx_root / "staged"
        backup_root = tx_root / "backup"
        staged_root.mkdir(parents=True, exist_ok=True)
        backup_root.mkdir(parents=True, exist_ok=True)
        _fsync_directory(staged_root)
        _fsync_directory(backup_root)

        write_rows: list[_WriteRow] = []
        delete_rows: list[_DeleteRow] = []

        for item in writes:
            target_path = resolve_inside(root, item.rel_path)
            staged_path = staged_root / item.rel_path
            _write_bytes(staged_path, item.content)
            backup_path: Path | None = None
            if target_path.exists() and target_path.is_file():
                backup_path = backup_root / item.rel_path
                _copy_file(target_path, backup_path)
            write_rows.append(
                _WriteRow(
                    rel_path=item.rel_path,
                    staged_path=str(staged_path),
                    backup_path=str(backup_path) if backup_path is not None else None,
                )
            )

        for item in deletes:
            target_path = resolve_inside(root, item.rel_path)
            backup_path = (
                backup_root / item.rel_path
                if target_path.exists() and target_path.is_file()
                else None
            )
            if backup_path is not None:
                _copy_file(target_path, backup_path)
            delete_rows.append(
                _DeleteRow(
                    rel_path=item.rel_path, backup_path=str(backup_path) if backup_path else None
                )
            )

        _fsync_directory(staged_root)
        _fsync_directory(backup_root)
        _fsync_directory(tx_root)
        _fsync_directory(self._tx_dir)

        try:
            with self._connect() as conn:
                conn.execute(
                    "INSERT INTO tx (txid, target_key, target_root, state) VALUES (?, ?, ?, 'prepared')",
                    (txid, target_key.value, str(root)),
                )
                conn.executemany(
                    "INSERT INTO tx_write (txid, rel_path, staged_path, backup_path) VALUES (?, ?, ?, ?)",
                    [(txid, row.rel_path, row.staged_path, row.backup_path) for row in write_rows],
                )
                conn.executemany(
                    "INSERT INTO tx_delete (txid, rel_path, backup_path) VALUES (?, ?, ?)",
                    [(txid, row.rel_path, row.backup_path) for row in delete_rows],
                )
        except Exception as error:
            self._cleanup_tx_dir(txid)
            raise ArtifactTransactionError(
                f"Failed to begin artifact transaction {txid}: {error}"
            ) from error
        return txid

    def _load_tx(self, txid: str) -> tuple[_TxRow, tuple[_WriteRow, ...], tuple[_DeleteRow, ...]]:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT txid, target_key, target_root, state FROM tx WHERE txid=?",
                (txid,),
            ).fetchone()
            if row is None:
                raise ArtifactTransactionError(f"Unknown artifact transaction: {txid}")
            writes = conn.execute(
                "SELECT rel_path, staged_path, backup_path FROM tx_write WHERE txid=? ORDER BY rel_path",
                (txid,),
            ).fetchall()
            deletes = conn.execute(
                "SELECT rel_path, backup_path FROM tx_delete WHERE txid=? ORDER BY rel_path",
                (txid,),
            ).fetchall()
        tx = _TxRow(
            txid=str(row["txid"]),
            target_key=str(row["target_key"]),
            target_root=str(row["target_root"]),
            state=str(row["state"]),
        )
        write_rows = tuple(
            _WriteRow(
                rel_path=str(w["rel_path"]),
                staged_path=str(w["staged_path"]),
                backup_path=w["backup_path"],
            )
            for w in writes
        )
        delete_rows = tuple(
            _DeleteRow(rel_path=str(d["rel_path"]), backup_path=d["backup_path"]) for d in deletes
        )
        return tx, write_rows, delete_rows

    def _set_state(self, txid: str, state: str) -> None:
        with self._connect() as conn:
            conn.execute("UPDATE tx SET state=? WHERE txid=?", (state, txid))

    def apply(self, *, txid: str) -> None:
        tx, writes, deletes = self._load_tx(txid)
        if tx.state == "committed":
            return
        if tx.state not in {"prepared", "applying"}:
            raise ArtifactTransactionError(f"Unsupported transaction state {tx.state!r} for {txid}")

        root = Path(tx.target_root).resolve()
        self._set_state(txid, "applying")
        try:
            for row in writes:
                target_path = resolve_inside(root, row.rel_path)
                staged_path = Path(row.staged_path)
                if not staged_path.exists():
                    _raise_missing_staged(txid=txid, rel_path=row.rel_path)
                _write_bytes(target_path, staged_path.read_bytes())

            for row in deletes:
                target_path = resolve_inside(root, row.rel_path)
                if target_path.exists():
                    target_path.unlink()
                    _fsync_directory(target_path.parent)

            self._set_state(txid, "committed")
        except Exception as error:
            raise ArtifactTransactionError(
                f"Failed to apply artifact transaction {txid}: {error}"
            ) from error

    def _rollback_tx(self, *, txid: str, target_root: Path) -> None:
        _tx, writes, deletes = self._load_tx(txid)
        root = target_root.resolve()

        for row in writes:
            target_path = resolve_inside(root, row.rel_path)
            if row.backup_path is not None:
                backup_path = Path(row.backup_path)
                if backup_path.exists():
                    _write_bytes(target_path, backup_path.read_bytes())
                elif target_path.exists():
                    target_path.unlink()
            elif target_path.exists():
                target_path.unlink()

        for row in deletes:
            if row.backup_path is None:
                continue
            target_path = resolve_inside(root, row.rel_path)
            backup_path = Path(row.backup_path)
            if backup_path.exists():
                _write_bytes(target_path, backup_path.read_bytes())

    def _cleanup_tx_dir(self, txid: str) -> None:
        tx_root = self._tx_dir / txid
        if tx_root.exists():
            shutil.rmtree(tx_root)
            _fsync_directory(self._tx_dir)

    def finalize(self, *, txid: str) -> None:
        try:
            with self._connect() as conn:
                conn.execute("DELETE FROM tx WHERE txid=?", (txid,))
            self._cleanup_tx_dir(txid)
        except Exception as error:
            raise ArtifactTransactionError(
                f"Failed to finalize artifact transaction {txid}: {error}"
            ) from error
