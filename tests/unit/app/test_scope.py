from pathlib import Path

import pytest

from matey.artifacts import SqliteArtifactStore
from matey.errors import TargetIdentityError
from matey.models import TargetKey, derive_target_key
from matey.platform import ReentrantLockManager
from matey.runtime import CommandScope


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
