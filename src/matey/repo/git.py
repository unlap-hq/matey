from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pygit2


class GitRepoError(RuntimeError):
    pass


class NotGitRepositoryError(GitRepoError):
    pass


class UnknownBaseRefError(GitRepoError):
    pass


@dataclass(frozen=True, slots=True)
class MergeBase:
    base_ref: str
    head_oid: pygit2.Oid
    base_oid: pygit2.Oid
    merge_base_oid: pygit2.Oid


class GitRepo:
    def __init__(self, repository: pygit2.Repository) -> None:
        self._repo = repository

    @classmethod
    def open(cls, start: Path) -> GitRepo:
        discovered = pygit2.discover_repository(str(start.resolve()))
        if discovered is None:
            raise NotGitRepositoryError(f"Path is not inside a git repository: {start}")
        return cls(pygit2.Repository(discovered))

    @property
    def repo_root(self) -> Path:
        if self._repo.workdir is None:
            raise GitRepoError("Repository has no working tree (bare repository).")
        return Path(self._repo.workdir).resolve()

    def resolve_merge_base(self, base_ref: str) -> MergeBase:
        head_commit = self._resolve_commit("HEAD")
        base_commit = self._resolve_commit(base_ref)

        merge_base_oid = self._repo.merge_base(head_commit.id, base_commit.id)
        if merge_base_oid is None:
            raise GitRepoError(
                f"No merge base between HEAD ({head_commit.id}) and {base_ref} ({base_commit.id})."
            )

        return MergeBase(
            base_ref=base_ref,
            head_oid=head_commit.id,
            base_oid=base_commit.id,
            merge_base_oid=merge_base_oid,
        )

    def tree_for(self, oid: pygit2.Oid) -> pygit2.Tree:
        try:
            obj = self._repo[oid]
        except (KeyError, ValueError) as error:
            raise GitRepoError(f"Unknown git object id {oid}.") from error

        if isinstance(obj, pygit2.Tree):
            return obj
        if isinstance(obj, pygit2.Commit):
            return obj.tree
        raise GitRepoError(f"Object {oid} is not a commit/tree (got {type(obj).__name__}).")

    def _resolve_commit(self, ref: str) -> pygit2.Commit:
        try:
            obj = self._repo.revparse_single(ref)
        except KeyError as error:
            raise UnknownBaseRefError(f"Unknown git ref {ref!r}.") from error
        except (ValueError, pygit2.GitError) as error:
            raise GitRepoError(f"Unable to resolve ref {ref!r}: {error}") from error

        try:
            commit = obj.peel(pygit2.Commit)
        except (ValueError, pygit2.GitError) as error:
            raise GitRepoError(f"Reference {ref!r} does not resolve to a commit.") from error
        return commit


__all__ = [
    "GitRepo",
    "GitRepoError",
    "MergeBase",
    "NotGitRepositoryError",
    "UnknownBaseRefError",
]
