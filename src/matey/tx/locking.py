from __future__ import annotations

import hashlib
import tempfile
import threading
import weakref
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from portalocker import RLock

from .journal import ensure_tx_target_dir

_TARGET_LOCK_FILE = "tx.lock"
_RLOCKS_GUARD = threading.Lock()
_RLOCKS_BY_PATH: weakref.WeakValueDictionary[Path, RLock] = weakref.WeakValueDictionary()


@contextmanager
def serialized_target(target_dir: Path) -> Iterator[None]:
    target_root = ensure_tx_target_dir(target_dir)
    lock_path = target_lock_path(target_root)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with target_rlock(lock_path):
        yield


def target_lock_path(target_root: Path) -> Path:
    digest = hashlib.blake2b(
        target_root.as_posix().encode("utf-8"),
        digest_size=16,
    ).hexdigest()
    lock_root = Path(tempfile.gettempdir()) / "matey-locks"
    return lock_root / f"{digest}-{_TARGET_LOCK_FILE}"


def target_rlock(lock_path: Path) -> RLock:
    with _RLOCKS_GUARD:
        existing = _RLOCKS_BY_PATH.get(lock_path)
        if existing is not None:
            return existing
        created = RLock(str(lock_path), mode="a", timeout=None)
        _RLOCKS_BY_PATH[lock_path] = created
        return created


__all__ = ["serialized_target", "target_lock_path"]
