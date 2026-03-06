from __future__ import annotations

import threading
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from portalocker import RLock

_TARGET_LOCK_FILE = "tx.lock"
_RLOCKS_GUARD = threading.Lock()
_RLOCKS_BY_PATH: dict[Path, RLock] = {}


@contextmanager
def serialized_target(target_dir: Path) -> Iterator[None]:
    target_root = target_dir.resolve()
    lock_path = target_root / ".matey" / _TARGET_LOCK_FILE
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with target_rlock(lock_path):
        yield


def target_rlock(lock_path: Path) -> RLock:
    with _RLOCKS_GUARD:
        existing = _RLOCKS_BY_PATH.get(lock_path)
        if existing is not None:
            return existing
        created = RLock(str(lock_path), mode="a", timeout=None)
        _RLOCKS_BY_PATH[lock_path] = created
        return created


__all__ = ["serialized_target"]
