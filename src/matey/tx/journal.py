from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

MANIFEST_FILE = "manifest.json"
STATE_FILE = "state"
STAGED_DIR = "staged"
BACKUP_DIR = "backup"
RESERVED_TX_PREFIX = (".matey", "tx")
STATE_PREPARED = "prepared"
STATE_APPLYING = "applying"
STATE_COMMITTED = "committed"
KNOWN_STATES = {STATE_PREPARED, STATE_APPLYING, STATE_COMMITTED}


class TxError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class TxManifest:
    version: int
    created_ns: int
    writes: tuple[str, ...]
    deletes: tuple[str, ...]


def tx_root(target_root: Path) -> Path:
    return target_root / ".matey" / "tx"


def create_tx_dir(target_root: Path, *, fsync_dir: callable) -> Path:
    root = tx_root(target_root)
    root.mkdir(parents=True, exist_ok=True)
    tx_dir = root / f"{time.time_ns()}-{uuid.uuid4().hex}"
    tx_dir.mkdir(parents=False, exist_ok=False)
    fsync_dir(root)
    return tx_dir


def write_manifest(tx_dir: Path, manifest: TxManifest, *, write_bytes: callable) -> None:
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
    write_bytes(tx_dir / MANIFEST_FILE, payload)


def read_manifest(tx_dir: Path) -> TxManifest:
    path = tx_dir / MANIFEST_FILE
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

    return TxManifest(
        version=version,
        created_ns=created_ns_raw,
        writes=tuple(writes),
        deletes=tuple(deletes),
    )


def tx_dir_created_ns(tx_dir: Path) -> int:
    prefix = tx_dir.name.split("-", 1)[0]
    if prefix.isdigit():
        return int(prefix)
    try:
        return int(tx_dir.stat().st_mtime_ns)
    except OSError:
        return 0


def write_state(tx_dir: Path, state: str, *, write_bytes: callable) -> None:
    if state not in KNOWN_STATES:
        raise TxError(f"Unknown transaction state: {state}")
    write_bytes(tx_dir / STATE_FILE, f"{state}\n".encode())


def read_state(tx_dir: Path) -> str:
    path = tx_dir / STATE_FILE
    if not path.exists():
        raise TxError(f"Missing transaction state file: {path}")
    state = path.read_text(encoding="utf-8").strip()
    if state not in KNOWN_STATES:
        raise TxError(f"Invalid transaction state {state!r} in {path}")
    return state


def normalize_writes(*, target_root: Path, writes: Mapping[Path, bytes]) -> dict[str, bytes]:
    return dict(
        normalize_path_items(
            target_root=target_root,
            items=writes.items(),
            reject_duplicates=True,
            duplicate_error_prefix="Duplicate write target after normalization",
        )
    )


def normalize_deletes(*, target_root: Path, deletes: tuple[Path, ...]) -> tuple[str, ...]:
    return tuple(
        rel
        for rel, _ in normalize_path_items(
            target_root=target_root,
            items=((path, None) for path in deletes),
            reject_duplicates=False,
            duplicate_error_prefix="",
        )
    )


def normalize_path_items(
    *,
    target_root: Path,
    items: Iterator[tuple[Path, bytes | None]],
    reject_duplicates: bool,
    duplicate_error_prefix: str,
) -> tuple[tuple[str, bytes | None], ...]:
    rows: dict[str, bytes | None] = {}
    for raw_path, payload in items:
        rel = relative_target_path(target_root, raw_path.resolve())
        if reject_duplicates and rel in rows:
            raise TxError(f"{duplicate_error_prefix}: {rel}")
        rows[rel] = payload
    return tuple(rows.items())


def relative_target_path(target_root: Path, path: Path) -> str:
    if not path.is_relative_to(target_root):
        raise TxError(f"Path is outside target directory: {path}")
    rel = path.relative_to(target_root).as_posix()
    return _validated_relative_path(rel, source=f"path {path}")


def absolute_target_path(target_root: Path, rel_path: str) -> Path:
    normalized = _validated_relative_path(rel_path, source="manifest path")
    absolute = (target_root / Path(normalized)).resolve()
    if not absolute.is_relative_to(target_root):
        raise TxError(f"Resolved manifest path escapes target root: {rel_path}")
    return absolute


def is_reserved_tx_path(parts: tuple[str, ...]) -> bool:
    return len(parts) >= 2 and parts[0:2] == RESERVED_TX_PREFIX


def _validated_relative_path(rel_path: str, *, source: str) -> str:
    normalized = PurePosixPath(rel_path).as_posix()
    pure = PurePosixPath(normalized)
    if not normalized or normalized == ".":
        raise TxError(f"{source} is empty.")
    if pure.is_absolute():
        raise TxError(f"{source} must be relative: {rel_path}")
    if any(part in {"..", "."} for part in pure.parts):
        raise TxError(f"{source} contains invalid segment: {rel_path}")
    if is_reserved_tx_path(pure.parts):
        raise TxError(f"{source} is reserved for tx journal internals: {rel_path}")
    return normalized


__all__ = [
    "BACKUP_DIR",
    "STAGED_DIR",
    "STATE_APPLYING",
    "STATE_COMMITTED",
    "STATE_PREPARED",
    "TxError",
    "TxManifest",
    "absolute_target_path",
    "create_tx_dir",
    "normalize_deletes",
    "normalize_writes",
    "read_manifest",
    "read_state",
    "tx_dir_created_ns",
    "tx_root",
    "write_manifest",
    "write_state",
]
