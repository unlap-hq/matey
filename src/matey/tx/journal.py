from __future__ import annotations

import json
import time
import uuid
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from matey.paths import (
    PathBoundaryError,
    RelativePathError,
    describe_path_boundary_error,
    normalize_relative_posix_path,
    safe_descendant,
    safe_relative_descendant,
)

MANIFEST_FILE = "manifest.json"
STATE_FILE = "state"
STAGED_DIR = "staged"
BACKUP_DIR = "backup"
RESERVED_TX_PREFIX = (".matey", "tx")
RESERVED_TX_LOCK = (".matey", "tx.lock")
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


def ensure_safe_tx_root(target_root: Path) -> Path:
    meta_root = target_root / ".matey"
    root = tx_root(target_root)
    try:
        safe_descendant(
            root=target_root,
            candidate=meta_root,
            label="transaction metadata root",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
    except PathBoundaryError as error:
        if error.kind in {"symlink_leaf", "symlink_intermediate"}:
            raise TxError(f"Transaction metadata root is symlinked: {meta_root}") from error
        if error.kind == "not_dir":
            raise TxError(f"Transaction metadata root is not a directory: {meta_root}") from error
        raise TxError(describe_path_boundary_error(error)) from error

    try:
        safe_descendant(
            root=target_root,
            candidate=root,
            label="transaction root",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
    except PathBoundaryError as error:
        if error.kind in {"symlink_leaf", "symlink_intermediate"}:
            raise TxError(f"Transaction root is symlinked: {root}") from error
        if error.kind == "not_dir":
            raise TxError(f"Transaction root is not a directory: {root}") from error
        raise TxError(describe_path_boundary_error(error)) from error
    return root


def create_tx_dir(target_root: Path, *, fsync_dir: callable) -> Path:
    root = ensure_safe_tx_root(target_root)
    root.parent.mkdir(parents=True, exist_ok=True)
    root.mkdir(parents=False, exist_ok=True)
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
    ensure_regular_journal_file(tx_dir, path, label="transaction manifest")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise TxError(f"Unable to read transaction manifest: {path}") from error
    except UnicodeDecodeError as error:
        raise TxError(f"Transaction manifest is not valid UTF-8: {path}") from error
    except json.JSONDecodeError as error:
        raise TxError(f"Invalid transaction manifest JSON: {path}") from error
    if not isinstance(data, dict):
        raise TxError(f"Invalid transaction manifest type: {path}")

    version = data.get("version")
    created_ns_raw = data.get("created_ns")
    writes = data.get("writes")
    deletes = data.get("deletes")
    if not isinstance(version, int):
        raise TxError(f"Manifest has non-integer version: {path}")
    if not isinstance(created_ns_raw, int):
        raise TxError(f"Manifest has non-integer created_ns: {path}")
    if created_ns_raw <= 0:
        raise TxError(f"Manifest has invalid created_ns: {path}")
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


def write_state(tx_dir: Path, state: str, *, write_bytes: callable) -> None:
    if state not in KNOWN_STATES:
        raise TxError(f"Unknown transaction state: {state}")
    write_bytes(tx_dir / STATE_FILE, f"{state}\n".encode())


def read_state(tx_dir: Path) -> str:
    path = tx_dir / STATE_FILE
    ensure_regular_journal_file(tx_dir, path, label="transaction state file")
    try:
        state = path.read_text(encoding="utf-8").strip()
    except OSError as error:
        raise TxError(f"Unable to read transaction state file: {path}") from error
    except UnicodeDecodeError as error:
        raise TxError(f"Transaction state file is not valid UTF-8: {path}") from error
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
        rel = normalize_target_input_path(target_root, raw_path)
        if reject_duplicates and rel in rows:
            raise TxError(f"{duplicate_error_prefix}: {rel}")
        rows[rel] = payload
    return tuple(rows.items())


def normalize_target_input_path(target_root: Path, raw_path: Path) -> str:
    candidate = raw_path if raw_path.is_absolute() else (target_root / raw_path)
    try:
        rel_path = safe_relative_descendant(
            root=target_root,
            candidate=candidate,
            label=f"path {candidate}",
            allow_missing_leaf=True,
        )
    except PathBoundaryError as error:
        raise TxError(describe_path_boundary_error(error)) from error
    return _validate_tx_relative_path(rel_path, source=f"path {candidate}")


def relative_target_path(target_root: Path, path: Path) -> str:
    try:
        rel = safe_relative_descendant(
            root=target_root,
            candidate=path,
            label=f"path {path}",
            allow_missing_leaf=True,
        )
    except PathBoundaryError as error:
        raise TxError(describe_path_boundary_error(error)) from error
    return _validate_tx_relative_path(rel, source=f"path {path}")


def absolute_target_path(target_root: Path, rel_path: str) -> Path:
    normalized = _validate_tx_relative_path(rel_path, source="manifest path")
    candidate = target_root / Path(normalized)
    try:
        return safe_descendant(
            root=target_root,
            candidate=candidate,
            label=f"manifest path {rel_path}",
            allow_missing_leaf=True,
        )
    except PathBoundaryError as error:
        raise TxError(str(error)) from error


def is_reserved_tx_path(parts: tuple[str, ...]) -> bool:
    return (len(parts) >= 2 and parts[0:2] == RESERVED_TX_PREFIX) or parts == RESERVED_TX_LOCK


def _validate_tx_relative_path(rel_path: str, *, source: str) -> str:
    try:
        normalized = normalize_relative_posix_path(rel_path, label=source)
    except RelativePathError as error:
        raise TxError(str(error)) from error
    pure = PurePosixPath(normalized)
    if is_reserved_tx_path(pure.parts):
        raise TxError(f"{source} is reserved for tx journal internals: {rel_path}")
    return normalized


def ensure_regular_journal_file(
    tx_dir: Path,
    path: Path,
    *,
    label: str,
    allow_missing: bool = False,
) -> bool:
    try:
        safe_path = safe_descendant(
            root=tx_dir,
            candidate=path,
            label=label,
            allow_missing_leaf=True,
            expected_kind="file",
        )
    except PathBoundaryError as error:
        raise TxError(
            describe_path_boundary_error(
                error,
                path=path,
                symlink_message=f"{label} uses a symlinked journal path",
            )
        ) from error

    if not safe_path.exists():
        if allow_missing:
            return False
        raise TxError(f"Missing {label}: {path}")
    return True


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
    "ensure_regular_journal_file",
    "ensure_safe_tx_root",
    "normalize_deletes",
    "normalize_writes",
    "read_manifest",
    "read_state",
    "tx_root",
    "write_manifest",
    "write_state",
]
