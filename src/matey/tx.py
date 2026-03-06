from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from boltons.fileutils import AtomicSaver
from portalocker import RLock

_STATE_PREPARED = "prepared"
_STATE_APPLYING = "applying"
_STATE_COMMITTED = "committed"
_KNOWN_STATES = {_STATE_PREPARED, _STATE_APPLYING, _STATE_COMMITTED}
_MANIFEST_FILE = "manifest.json"
_STATE_FILE = "state"
_STAGED_DIR = "staged"
_BACKUP_DIR = "backup"
_RESERVED_TX_PREFIX = (".matey", "tx")
_TARGET_LOCK_FILE = "tx.lock"
_RLOCKS_GUARD = threading.Lock()
_RLOCKS_BY_PATH: dict[Path, RLock] = {}


class TxError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class _TxManifest:
    version: int
    created_ns: int
    writes: tuple[str, ...]
    deletes: tuple[str, ...]


@contextmanager
def serialized_target(target_dir: Path) -> Iterator[None]:
    target_root = target_dir.resolve()
    lock_path = target_root / ".matey" / _TARGET_LOCK_FILE
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with _target_rlock(lock_path):
        yield


def _target_rlock(lock_path: Path) -> RLock:
    with _RLOCKS_GUARD:
        existing = _RLOCKS_BY_PATH.get(lock_path)
        if existing is not None:
            return existing
        created = RLock(str(lock_path), mode="a", timeout=None)
        _RLOCKS_BY_PATH[lock_path] = created
        return created


def recover_artifacts(target_dir: Path) -> None:
    target_root = target_dir.resolve()
    with serialized_target(target_root):
        _recover_artifacts_unlocked(target_root)


def _recover_artifacts_unlocked(target_root: Path) -> None:
    tx_root = _tx_root(target_root)
    if not tx_root.exists():
        return

    records: list[tuple[int, Path, _TxManifest, str]] = []
    for tx_dir in (entry for entry in tx_root.iterdir() if entry.is_dir()):
        manifest = _read_manifest(tx_dir)
        state = _read_state(tx_dir)
        created_ns = manifest.created_ns or _tx_dir_created_ns(tx_dir)
        records.append((created_ns, tx_dir, manifest, state))

    applying = sorted(
        (
            (created_ns, tx_dir, manifest, state)
            for created_ns, tx_dir, manifest, state in records
            if state == _STATE_APPLYING
        ),
        key=lambda row: (row[0], row[1].name),
    )
    applying_count = len(applying)
    if applying_count > 1:
        names = ", ".join(tx_dir.name for _, tx_dir, _, _ in applying)
        raise TxError(f"Multiple applying transactions found; recovery order is ambiguous: {names}")

    settled = sorted(
        (
            (created_ns, tx_dir, manifest, state)
            for created_ns, tx_dir, manifest, state in records
            if state != _STATE_APPLYING
        ),
        key=lambda row: (row[0], row[1].name),
    )
    for _, tx_dir, manifest, state in settled:
        _recover_tx(target_root=target_root, tx_dir=tx_dir, manifest=manifest, state=state)
    for _, tx_dir, manifest, state in applying:
        _recover_tx(target_root=target_root, tx_dir=tx_dir, manifest=manifest, state=state)

    if tx_root.exists() and not any(tx_root.iterdir()):
        shutil.rmtree(tx_root, ignore_errors=True)


def commit_artifacts(
    target_dir: Path,
    writes: Mapping[Path, bytes],
    deletes: tuple[Path, ...],
) -> tuple[Path, ...]:
    target_root = target_dir.resolve()
    with serialized_target(target_root):
        return _commit_artifacts_unlocked(target_root, writes=writes, deletes=deletes)


def _commit_artifacts_unlocked(
    target_root: Path,
    *,
    writes: Mapping[Path, bytes],
    deletes: tuple[Path, ...],
) -> tuple[Path, ...]:
    _recover_artifacts_unlocked(target_root)
    normalized_writes = _normalize_writes(target_root=target_root, writes=writes)
    normalized_deletes = _normalize_deletes(target_root=target_root, deletes=deletes)
    overlapping_paths = sorted(set(normalized_writes).intersection(normalized_deletes))
    if overlapping_paths:
        joined = ", ".join(overlapping_paths)
        raise TxError(
            "Paths cannot be in both writes and deletes in one transaction: "
            f"{joined}"
        )

    if not normalized_writes and not normalized_deletes:
        return ()

    tx_dir = _create_tx_dir(target_root)
    manifest = _TxManifest(
        version=1,
        created_ns=time.time_ns(),
        writes=tuple(sorted(normalized_writes.keys())),
        deletes=tuple(sorted(normalized_deletes)),
    )
    _prepare_tx(
        target_root=target_root,
        tx_dir=tx_dir,
        manifest=manifest,
        writes=normalized_writes,
        deletes=normalized_deletes,
    )
    # On any exception below, keep tx_dir intact for deterministic recovery.
    _write_state(tx_dir, _STATE_APPLYING)
    _apply_tx(
        target_root=target_root,
        tx_dir=tx_dir,
        manifest=manifest,
    )
    _write_state(tx_dir, _STATE_COMMITTED)
    shutil.rmtree(tx_dir, ignore_errors=True)

    changed = {
        _absolute_target_path(target_root, rel) for rel in (*manifest.writes, *manifest.deletes)
    }
    return tuple(sorted(changed, key=lambda p: p.as_posix()))


def _prepare_tx(
    *,
    target_root: Path,
    tx_dir: Path,
    manifest: _TxManifest,
    writes: Mapping[str, bytes],
    deletes: tuple[str, ...],
) -> None:
    for rel_path, payload in writes.items():
        staged_path = tx_dir / _STAGED_DIR / rel_path
        _atomic_write_bytes(staged_path, payload)

        target_path = _absolute_target_path(target_root, rel_path)
        if target_path.exists():
            backup_path = tx_dir / _BACKUP_DIR / rel_path
            _atomic_write_bytes(backup_path, target_path.read_bytes())

    for rel_path in deletes:
        target_path = _absolute_target_path(target_root, rel_path)
        if target_path.exists():
            backup_path = tx_dir / _BACKUP_DIR / rel_path
            _atomic_write_bytes(backup_path, target_path.read_bytes())

    _write_manifest(tx_dir, manifest)
    _write_state(tx_dir, _STATE_PREPARED)


def _apply_tx(
    *,
    target_root: Path,
    tx_dir: Path,
    manifest: _TxManifest,
) -> None:
    for rel_path in manifest.writes:
        staged_path = tx_dir / _STAGED_DIR / rel_path
        target_path = _absolute_target_path(target_root, rel_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.replace(target_path)
        _fsync_dir(target_path.parent)

    for rel_path in manifest.deletes:
        target_path = _absolute_target_path(target_root, rel_path)
        target_path.unlink(missing_ok=True)
        _fsync_dir(target_path.parent)


def _recover_tx(
    *,
    target_root: Path,
    tx_dir: Path,
    manifest: _TxManifest,
    state: str,
) -> None:
    if state == _STATE_COMMITTED:
        shutil.rmtree(tx_dir, ignore_errors=True)
        return
    if state == _STATE_PREPARED:
        shutil.rmtree(tx_dir, ignore_errors=True)
        return
    if state != _STATE_APPLYING:
        raise TxError(f"Unknown transaction state {state!r} in {tx_dir}.")

    touched = tuple(dict.fromkeys((*manifest.writes, *manifest.deletes)))
    for rel_path in touched:
        target_path = _absolute_target_path(target_root, rel_path)
        backup_path = tx_dir / _BACKUP_DIR / rel_path
        if backup_path.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.replace(target_path)
            _fsync_dir(target_path.parent)
            continue
        target_path.unlink(missing_ok=True)
        _fsync_dir(target_path.parent)

    shutil.rmtree(tx_dir, ignore_errors=True)


def _normalize_writes(*, target_root: Path, writes: Mapping[Path, bytes]) -> dict[str, bytes]:
    normalized_items = _normalize_path_items(
        target_root=target_root,
        items=writes.items(),
        reject_duplicates=True,
        duplicate_error_prefix="Duplicate write target after normalization",
    )
    return dict(normalized_items)


def _normalize_deletes(*, target_root: Path, deletes: tuple[Path, ...]) -> tuple[str, ...]:
    normalized_items = _normalize_path_items(
        target_root=target_root,
        items=((path, None) for path in deletes),
        reject_duplicates=False,
        duplicate_error_prefix="",
    )
    return tuple(rel for rel, _ in normalized_items)


def _normalize_path_items(
    *,
    target_root: Path,
    items: Iterator[tuple[Path, bytes | None]],
    reject_duplicates: bool,
    duplicate_error_prefix: str,
) -> tuple[tuple[str, bytes | None], ...]:
    rows: dict[str, bytes | None] = {}
    for raw_path, payload in items:
        absolute = raw_path.resolve()
        rel = _relative_target_path(target_root, absolute)
        if reject_duplicates and rel in rows:
            raise TxError(f"{duplicate_error_prefix}: {rel}")
        rows[rel] = payload
    return tuple(rows.items())


def _relative_target_path(target_root: Path, path: Path) -> str:
    if not path.is_relative_to(target_root):
        raise TxError(f"Path is outside target directory: {path}")
    rel = path.relative_to(target_root).as_posix()
    normalized = PurePosixPath(rel).as_posix()
    if not normalized or normalized == ".":
        raise TxError(f"Invalid target-relative path: {path}")
    if any(part in {"..", "."} for part in PurePosixPath(normalized).parts):
        raise TxError(f"Invalid normalized target-relative path: {normalized}")
    if _is_reserved_tx_path(PurePosixPath(normalized).parts):
        raise TxError(f"Path is reserved for tx journal internals: {normalized}")
    return normalized


def _absolute_target_path(target_root: Path, rel_path: str) -> Path:
    normalized = PurePosixPath(rel_path).as_posix()
    if not normalized or normalized == ".":
        raise TxError("Manifest contains empty relative path.")
    if PurePosixPath(normalized).is_absolute():
        raise TxError(f"Manifest path must be relative: {rel_path}")
    if any(part in {"..", "."} for part in PurePosixPath(normalized).parts):
        raise TxError(f"Manifest path contains invalid segment: {rel_path}")
    if _is_reserved_tx_path(PurePosixPath(normalized).parts):
        raise TxError(f"Manifest path is reserved for tx journal internals: {rel_path}")
    absolute = (target_root / Path(normalized)).resolve()
    if not absolute.is_relative_to(target_root):
        raise TxError(f"Resolved manifest path escapes target root: {rel_path}")
    return absolute


def _tx_root(target_root: Path) -> Path:
    return target_root / ".matey" / "tx"


def _is_reserved_tx_path(parts: tuple[str, ...]) -> bool:
    return len(parts) >= 2 and parts[0:2] == _RESERVED_TX_PREFIX


def _create_tx_dir(target_root: Path) -> Path:
    tx_root = _tx_root(target_root)
    tx_root.mkdir(parents=True, exist_ok=True)
    tx_dir = tx_root / f"{time.time_ns()}-{uuid.uuid4().hex}"
    tx_dir.mkdir(parents=False, exist_ok=False)
    _fsync_dir(tx_root)
    return tx_dir


def _write_manifest(tx_dir: Path, manifest: _TxManifest) -> None:
    payload = json.dumps(
        {
            "version": manifest.version,
            "created_ns": manifest.created_ns,
            "writes": list(manifest.writes),
            "deletes": list(manifest.deletes),
        },
        sort_keys=True,
        indent=2,
    ).encode("utf-8")
    _atomic_write_bytes(tx_dir / _MANIFEST_FILE, payload)


def _read_manifest(tx_dir: Path) -> _TxManifest:
    path = tx_dir / _MANIFEST_FILE
    if not path.exists():
        raise TxError(f"Missing transaction manifest: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise TxError(f"Invalid transaction manifest JSON: {path}") from error
    if not isinstance(data, dict):
        raise TxError(f"Invalid transaction manifest type: {path}")

    version = data.get("version")
    created_ns_raw = data.get("created_ns", 0)
    writes = data.get("writes")
    deletes = data.get("deletes")
    if not isinstance(version, int):
        raise TxError(f"Manifest has non-integer version: {path}")
    if not isinstance(created_ns_raw, int):
        raise TxError(f"Manifest has non-integer created_ns: {path}")
    if not isinstance(writes, list) or not all(isinstance(item, str) for item in writes):
        raise TxError(f"Manifest has invalid writes list: {path}")
    if not isinstance(deletes, list) or not all(isinstance(item, str) for item in deletes):
        raise TxError(f"Manifest has invalid deletes list: {path}")

    return _TxManifest(
        version=version,
        created_ns=created_ns_raw,
        writes=tuple(writes),
        deletes=tuple(deletes),
    )


def _tx_dir_created_ns(tx_dir: Path) -> int:
    name = tx_dir.name
    prefix = name.split("-", 1)[0]
    if prefix.isdigit():
        return int(prefix)
    try:
        return int(tx_dir.stat().st_mtime_ns)
    except OSError:
        return 0


def _write_state(tx_dir: Path, state: str) -> None:
    if state not in _KNOWN_STATES:
        raise TxError(f"Unknown transaction state: {state}")
    _atomic_write_bytes(tx_dir / _STATE_FILE, f"{state}\n".encode())


def _read_state(tx_dir: Path) -> str:
    path = tx_dir / _STATE_FILE
    if not path.exists():
        raise TxError(f"Missing transaction state file: {path}")
    state = path.read_text(encoding="utf-8").strip()
    if state not in _KNOWN_STATES:
        raise TxError(f"Invalid transaction state {state!r} in {path}")
    return state


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with AtomicSaver(str(path), overwrite=True, text_mode=False) as handle:
        handle.write(payload)
    _fsync_dir(path.parent)


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except Exception:  # pragma: no cover
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = [
    "TxError",
    "commit_artifacts",
    "recover_artifacts",
    "serialized_target",
]
