from __future__ import annotations

from pathlib import Path

import pygit2
import pytest

from matey.git_repo import GitRepo, NotGitRepositoryError, UnknownBaseRefError


def _commit_all(repo: pygit2.Repository, message: str) -> pygit2.Oid:
    signature = pygit2.Signature("Matey Test", "matey-test@example.com")
    index = repo.index
    index.add_all()
    index.write()
    tree_id = index.write_tree()
    head = repo.head.target if not repo.head_is_unborn else None
    parents = [head] if head is not None else []
    return repo.create_commit("HEAD", signature, signature, message, tree_id, parents)


def test_open_raises_for_non_git_directory(tmp_path: Path) -> None:
    with pytest.raises(NotGitRepositoryError):
        GitRepo.open(tmp_path)


def test_resolve_merge_base_and_tree_for(tmp_path: Path) -> None:
    repo = pygit2.init_repository(str(tmp_path), initial_head="main")
    (tmp_path / "file.txt").write_text("base\n", encoding="utf-8")
    base_oid = _commit_all(repo, "base")

    base_commit = repo[base_oid]
    repo.create_branch("feature", base_commit)
    repo.set_head("refs/heads/feature")
    repo.checkout_tree(base_commit.tree, strategy=pygit2.GIT_CHECKOUT_FORCE)
    (tmp_path / "file.txt").write_text("feature\n", encoding="utf-8")
    _commit_all(repo, "feature")

    git_repo = GitRepo.open(tmp_path)
    merge = git_repo.resolve_merge_base("refs/heads/main")
    tree = git_repo.tree_for(merge.merge_base_oid)

    assert merge.base_ref == "refs/heads/main"
    assert merge.base_oid == base_oid
    assert merge.merge_base_oid == base_oid
    assert isinstance(tree, pygit2.Tree)


def test_resolve_merge_base_unknown_ref_raises(tmp_path: Path) -> None:
    repo = pygit2.init_repository(str(tmp_path), initial_head="main")
    (tmp_path / "file.txt").write_text("base\n", encoding="utf-8")
    _commit_all(repo, "base")

    git_repo = GitRepo.open(tmp_path)
    with pytest.raises(UnknownBaseRefError):
        git_repo.resolve_merge_base("refs/heads/does-not-exist")
