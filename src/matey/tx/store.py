from __future__ import annotations

import os
import shutil
import time
from pathlib import Path

from boltons.fileutils import AtomicSaver

from .journal import (
    BACKUP_DIR,
    STAGED_DIR,
    STATE_APPLYING,
    STATE_COMMITTED,
    STATE_PREPARED,
    TxError,
    TxManifest,
    absolute_target_path,
    create_tx_dir,
    normalize_deletes,
    normalize_writes,
    read_manifest,
    read_state,
    tx_dir_created_ns,
    tx_root,
    write_manifest,
    write_state,
)
from .locking import serialized_target


def recover_artifacts(target_dir: Path) -> None:
    target_root = target_dir.resolve()
    with serialized_target(target_root):
        recover_artifacts_unlocked(target_root)


def commit_artifacts(
    target_dir: Path,
    writes: dict[Path, bytes],
    deletes: tuple[Path, ...],
) -> tuple[Path, ...]:
    target_root = target_dir.resolve()
    with serialized_target(target_root):
        return commit_artifacts_unlocked(target_root, writes=writes, deletes=deletes)


def recover_artifacts_unlocked(target_root: Path) -> None:
    root = tx_root(target_root)
    if not root.exists():
        return

    records: list[tuple[int, Path, TxManifest, str]] = []
    for tx_dir in (entry for entry in root.iterdir() if entry.is_dir()):
        manifest = read_manifest(tx_dir)
        state = read_state(tx_dir)
        created_ns = manifest.created_ns or tx_dir_created_ns(tx_dir)
        records.append((created_ns, tx_dir, manifest, state))

    applying = sorted(
        (
            (created_ns, tx_dir, manifest, state)
            for created_ns, tx_dir, manifest, state in records
            if state == STATE_APPLYING
        ),
        key=lambda row: (row[0], row[1].name),
    )
    if len(applying) > 1:
        names = ", ".join(tx_dir.name for _, tx_dir, _, _ in applying)
        raise TxError(f"Multiple applying transactions found; recovery order is ambiguous: {names}")

    settled = sorted(
        (
            (created_ns, tx_dir, manifest, state)
            for created_ns, tx_dir, manifest, state in records
            if state != STATE_APPLYING
        ),
        key=lambda row: (row[0], row[1].name),
    )
    for _, tx_dir, manifest, state in settled:
        recover_tx(target_root=target_root, tx_dir=tx_dir, manifest=manifest, state=state)
    for _, tx_dir, manifest, state in applying:
        recover_tx(target_root=target_root, tx_dir=tx_dir, manifest=manifest, state=state)

    if root.exists() and not any(root.iterdir()):
        shutil.rmtree(root, ignore_errors=True)


def commit_artifacts_unlocked(
    target_root: Path,
    *,
    writes: dict[Path, bytes],
    deletes: tuple[Path, ...],
) -> tuple[Path, ...]:
    recover_artifacts_unlocked(target_root)
    normalized_writes = normalize_writes(target_root=target_root, writes=writes)
    normalized_deletes = normalize_deletes(target_root=target_root, deletes=deletes)
    overlapping_paths = sorted(set(normalized_writes).intersection(normalized_deletes))
    if overlapping_paths:
        raise TxError(
            "Paths cannot be in both writes and deletes in one transaction: "
            + ", ".join(overlapping_paths)
        )
    if not normalized_writes and not normalized_deletes:
        return ()

    tx_dir = create_tx_dir(target_root, fsync_dir=fsync_dir)
    manifest = TxManifest(
        version=1,
        created_ns=time.time_ns(),
        writes=tuple(sorted(normalized_writes.keys())),
        deletes=tuple(sorted(normalized_deletes)),
    )
    prepare_tx(
        target_root=target_root,
        tx_dir=tx_dir,
        manifest=manifest,
        writes=normalized_writes,
        deletes=normalized_deletes,
    )
    write_state(tx_dir, STATE_APPLYING, write_bytes=atomic_write_bytes)
    apply_tx(target_root=target_root, tx_dir=tx_dir, manifest=manifest)
    write_state(tx_dir, STATE_COMMITTED, write_bytes=atomic_write_bytes)
    shutil.rmtree(tx_dir, ignore_errors=True)

    changed = {
        absolute_target_path(target_root, rel_path)
        for rel_path in (*manifest.writes, *manifest.deletes)
    }
    return tuple(sorted(changed, key=lambda path: path.as_posix()))


def prepare_tx(
    *,
    target_root: Path,
    tx_dir: Path,
    manifest: TxManifest,
    writes: dict[str, bytes],
    deletes: tuple[str, ...],
) -> None:
    for rel_path, payload in writes.items():
        atomic_write_bytes(tx_dir / STAGED_DIR / rel_path, payload)
        target_path = absolute_target_path(target_root, rel_path)
        if target_path.exists():
            atomic_write_bytes(tx_dir / BACKUP_DIR / rel_path, target_path.read_bytes())

    for rel_path in deletes:
        target_path = absolute_target_path(target_root, rel_path)
        if target_path.exists():
            atomic_write_bytes(tx_dir / BACKUP_DIR / rel_path, target_path.read_bytes())

    write_manifest(tx_dir, manifest, write_bytes=atomic_write_bytes)
    write_state(tx_dir, STATE_PREPARED, write_bytes=atomic_write_bytes)


def apply_tx(*, target_root: Path, tx_dir: Path, manifest: TxManifest) -> None:
    for rel_path in manifest.writes:
        staged_path = tx_dir / STAGED_DIR / rel_path
        target_path = absolute_target_path(target_root, rel_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        staged_path.replace(target_path)
        fsync_dir(target_path.parent)

    for rel_path in manifest.deletes:
        target_path = absolute_target_path(target_root, rel_path)
        target_path.unlink(missing_ok=True)
        fsync_dir(target_path.parent)


def recover_tx(*, target_root: Path, tx_dir: Path, manifest: TxManifest, state: str) -> None:
    if state == STATE_COMMITTED:
        shutil.rmtree(tx_dir, ignore_errors=True)
        return
    if state == STATE_PREPARED:
        shutil.rmtree(tx_dir, ignore_errors=True)
        return
    if state != STATE_APPLYING:
        raise TxError(f"Unknown transaction state {state!r} in {tx_dir}.")

    touched = tuple(dict.fromkeys((*manifest.writes, *manifest.deletes)))
    for rel_path in touched:
        target_path = absolute_target_path(target_root, rel_path)
        backup_path = tx_dir / BACKUP_DIR / rel_path
        if backup_path.exists():
            target_path.parent.mkdir(parents=True, exist_ok=True)
            backup_path.replace(target_path)
            fsync_dir(target_path.parent)
            continue
        target_path.unlink(missing_ok=True)
        fsync_dir(target_path.parent)

    shutil.rmtree(tx_dir, ignore_errors=True)


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with AtomicSaver(str(path), overwrite=True, text_mode=False) as handle:
        handle.write(payload)
    fsync_dir(path.parent)


def fsync_dir(path: Path) -> None:
    flags = os.O_RDONLY
    if hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    try:
        fd = os.open(path, flags)
    except OSError as error:
        raise TxError(f"Failed to open directory for fsync: {path}") from error
    try:
        os.fsync(fd)
    except OSError as error:
        raise TxError(f"Failed to fsync directory: {path}") from error
    finally:
        os.close(fd)


__all__ = ["commit_artifacts", "recover_artifacts"]
