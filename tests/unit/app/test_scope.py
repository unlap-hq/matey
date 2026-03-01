from pathlib import Path

import pytest

from matey.app.scope import CommandScope
from matey.domain.errors import TargetIdentityError
from matey.domain.target import TargetKey, derive_target_key
from matey.infra.artifact_store import SqliteArtifactStore
from matey.infra.locking import ReentrantLockManager


def test_command_scope_checks_target_identity(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    db = repo / "db"
    db.mkdir(parents=True)

    store = SqliteArtifactStore(repo_root=repo)
    lock = ReentrantLockManager(lock_root=repo / ".matey" / "locks")
    scope = CommandScope(repo_root=repo, lock_manager=lock, artifact_store=store)

    good_key = derive_target_key(repo_root=repo, db_dir=db)
    with scope.open(target_key=good_key, target_root=db):
        pass

    with pytest.raises(TargetIdentityError), scope.open(target_key=TargetKey("wrong"), target_root=db):
        pass
