from __future__ import annotations

from pathlib import Path

import pygit2
import pytest

from matey.config import TargetConfig
from matey.repo import Snapshot, SnapshotError


def _init_repo(path: Path) -> pygit2.Repository:
    repo = pygit2.init_repository(str(path), initial_head="main")
    signature = pygit2.Signature("Matey Test", "matey-test@example.com")

    index = repo.index
    index.add_all()
    index.write()
    tree_id = index.write_tree()
    repo.create_commit("HEAD", signature, signature, "initial", tree_id, [])
    return repo


def test_snapshot_from_worktree_reads_expected_files(tmp_path: Path) -> None:
    target_dir = tmp_path / "db" / "core"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "schema.sql").write_text("CREATE TABLE a(id INTEGER);\n", encoding="utf-8")
    (target_dir / "schema.lock.toml").write_text("lock_version = 0\n", encoding="utf-8")
    (target_dir / "migrations").mkdir(parents=True, exist_ok=True)
    (target_dir / "migrations" / "001_init.sql").write_text("-- up\n", encoding="utf-8")
    (target_dir / "migrations" / "nested").mkdir(parents=True, exist_ok=True)
    (target_dir / "migrations" / "nested" / "002_next.sql").write_text("-- up\n", encoding="utf-8")
    (target_dir / "checkpoints").mkdir(parents=True, exist_ok=True)
    (target_dir / "checkpoints" / "001_init.sql").write_text(
        "CREATE TABLE a(id INTEGER);\n", encoding="utf-8"
    )

    target = TargetConfig(
        name="core",
        dir=target_dir.resolve(),
        url_env="CORE_DATABASE_URL",
        test_url_env="CORE_TEST_DATABASE_URL",
    )
    snapshot = Snapshot.from_worktree(target)

    assert snapshot.target_name == "core"
    assert snapshot.schema_sql is not None
    assert snapshot.lock_toml is not None
    assert set(snapshot.migrations.keys()) == {
        "migrations/001_init.sql",
        "migrations/nested/002_next.sql",
    }
    assert set(snapshot.checkpoints.keys()) == {"checkpoints/001_init.sql"}


def test_snapshot_from_tree_reads_committed_state(tmp_path: Path) -> None:
    target_dir = tmp_path / "db" / "core"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "schema.sql").write_text("CREATE TABLE a(id INTEGER);\n", encoding="utf-8")
    (target_dir / "schema.lock.toml").write_text("lock_version = 0\n", encoding="utf-8")
    (target_dir / "migrations").mkdir(parents=True, exist_ok=True)
    (target_dir / "migrations" / "001_init.sql").write_text("-- up v1\n", encoding="utf-8")
    (target_dir / "checkpoints").mkdir(parents=True, exist_ok=True)
    (target_dir / "checkpoints" / "001_init.sql").write_text(
        "CREATE TABLE a(id INTEGER);\n", encoding="utf-8"
    )

    repo = _init_repo(tmp_path)
    head_tree = repo.revparse_single("HEAD").peel(pygit2.Commit).tree

    # mutate worktree after commit; from_tree must still read committed bytes
    (target_dir / "migrations" / "001_init.sql").write_text("-- up v2\n", encoding="utf-8")

    snapshot = Snapshot.from_tree(
        target_name="core",
        target_rel_dir="db/core",
        root_tree=head_tree,
    )

    assert snapshot.target_name == "core"
    assert snapshot.migrations["migrations/001_init.sql"] == b"-- up v1\n"


def test_snapshot_from_tree_returns_empty_for_missing_target(tmp_path: Path) -> None:
    repo = _init_repo(tmp_path)
    head_tree = repo.revparse_single("HEAD").peel(pygit2.Commit).tree

    snapshot = Snapshot.from_tree(
        target_name="core",
        target_rel_dir="db/core",
        root_tree=head_tree,
    )

    assert snapshot.schema_sql is None
    assert snapshot.lock_toml is None
    assert snapshot.migrations == {}
    assert snapshot.checkpoints == {}


def test_snapshot_from_tree_rejects_non_tree_target_dir(tmp_path: Path) -> None:
    target_path = tmp_path / "db" / "core"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text("not a directory\n", encoding="utf-8")

    repo = _init_repo(tmp_path)
    head_tree = repo.revparse_single("HEAD").peel(pygit2.Commit).tree

    with pytest.raises(SnapshotError, match="Expected tree but found non-tree object"):
        Snapshot.from_tree(
            target_name="core",
            target_rel_dir="db/core",
            root_tree=head_tree,
        )


def test_snapshot_from_tree_rejects_non_tree_intermediate_component(tmp_path: Path) -> None:
    db_path = tmp_path / "db"
    db_path.write_text("not a directory\n", encoding="utf-8")

    repo = _init_repo(tmp_path)
    head_tree = repo.revparse_single("HEAD").peel(pygit2.Commit).tree

    with pytest.raises(SnapshotError, match="Expected tree but found non-tree object at 'db'"):
        Snapshot.from_tree(
            target_name="core",
            target_rel_dir="db/core",
            root_tree=head_tree,
        )


def test_snapshot_from_tree_rejects_non_tree_migrations_dir(tmp_path: Path) -> None:
    target_dir = tmp_path / "db" / "core"
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / "schema.sql").write_text("CREATE TABLE a(id INTEGER);\n", encoding="utf-8")
    (target_dir / "schema.lock.toml").write_text("lock_version = 0\n", encoding="utf-8")
    (target_dir / "migrations").write_text("not a directory\n", encoding="utf-8")

    repo = _init_repo(tmp_path)
    head_tree = repo.revparse_single("HEAD").peel(pygit2.Commit).tree

    with pytest.raises(SnapshotError, match="Expected tree but found non-tree object at 'migrations'"):
        Snapshot.from_tree(
            target_name="core",
            target_rel_dir="db/core",
            root_tree=head_tree,
        )


def test_snapshot_from_tree_rejects_non_blob_schema_file(tmp_path: Path) -> None:
    target_dir = tmp_path / "db" / "core"
    (target_dir / "schema.sql").mkdir(parents=True, exist_ok=True)
    (target_dir / "schema.sql" / "nested.txt").write_text("not a blob schema\n", encoding="utf-8")
    (target_dir / "migrations").mkdir(parents=True, exist_ok=True)
    (target_dir / "checkpoints").mkdir(parents=True, exist_ok=True)

    repo = _init_repo(tmp_path)
    head_tree = repo.revparse_single("HEAD").peel(pygit2.Commit).tree

    with pytest.raises(SnapshotError, match=r"Expected blob but found non-blob object at 'schema\.sql'"):
        Snapshot.from_tree(
            target_name="core",
            target_rel_dir="db/core",
            root_tree=head_tree,
        )
