from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

import portalocker

from matey.domain.target import TargetKey


@dataclass
class _HeldLock:
    portalock: portalocker.Lock
    file_handle: object
    depth: int


class ReentrantLockManager:
    """Cross-process file lock with in-process reentrant depth tracking."""

    def __init__(self, *, lock_root: Path, timeout_seconds: int = 300) -> None:
        self._lock_root = lock_root
        self._timeout_seconds = timeout_seconds
        self._mutex = RLock()
        self._held: dict[str, _HeldLock] = {}

    def _lock_path(self, target_key: TargetKey) -> Path:
        self._lock_root.mkdir(parents=True, exist_ok=True)
        return self._lock_root / f"{target_key.value}.lock"

    @contextmanager
    def hold(self, *, target_key: TargetKey):
        key = target_key.value
        with self._mutex:
            existing = self._held.get(key)
            if existing is not None:
                existing.depth += 1
            else:
                lock_path = self._lock_path(target_key)
                plock = portalocker.Lock(str(lock_path), mode="a+", timeout=self._timeout_seconds)
                handle = plock.acquire()
                self._held[key] = _HeldLock(portalock=plock, file_handle=handle, depth=1)

        try:
            yield
        finally:
            with self._mutex:
                state = self._held.get(key)
                if state is not None:
                    state.depth -= 1
                    if state.depth <= 0:
                        try:
                            state.portalock.release()
                        finally:
                            self._held.pop(key, None)
