from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from matey.app.protocols import IArtifactStore, ICommandScope
from matey.domain.target import TargetKey, assert_target_identity
from matey.infra.locking import ReentrantLockManager


class CommandScope(ICommandScope):
    def __init__(
        self,
        *,
        repo_root: Path,
        lock_manager: ReentrantLockManager,
        artifact_store: IArtifactStore,
    ) -> None:
        self._repo_root = repo_root
        self._lock_manager = lock_manager
        self._artifact_store = artifact_store

    @contextmanager
    def open(self, *, target_key: TargetKey, target_root: Path):
        assert_target_identity(repo_root=self._repo_root, db_dir=target_root, target_key=target_key)
        with self._lock_manager.hold(target_key=target_key):
            self._artifact_store.recover_pending(target_key=target_key, target_root=target_root)
            yield
