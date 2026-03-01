from __future__ import annotations

from pathlib import Path, PurePosixPath

import pygit2

from matey.app.protocols import IGitRepo, WorktreeChange
from matey.domain.errors import ReplayError


class PygitRepository(IGitRepo):
    def __init__(self, *, cwd: Path | None = None) -> None:
        start = (cwd or Path.cwd()).resolve()
        discovered = pygit2.discover_repository(str(start))
        if discovered is None:
            raise ReplayError(f"Unable to discover git repository from {start}")
        self._repo = pygit2.Repository(discovered)
        workdir = self._repo.workdir
        if workdir is None:
            raise ReplayError("Bare repositories are not supported.")
        self._root = Path(workdir).resolve()

    def repo_root(self) -> Path:
        return self._root

    def head_commit(self) -> str:
        return str(self._repo.head.target)

    def resolve_ref(self, ref: str) -> str:
        try:
            obj = self._repo.revparse_single(ref)
        except KeyError as error:
            raise ReplayError(f"Unable to resolve git ref: {ref}") from error
        return str(obj.id)

    def merge_base(self, left_ref: str, right_ref: str) -> str:
        left = self.resolve_ref(left_ref)
        right = self.resolve_ref(right_ref)
        merged = self._repo.merge_base(pygit2.Oid(hex=left), pygit2.Oid(hex=right))
        if merged is None:
            raise ReplayError(f"Unable to compute merge base for refs: {left_ref}, {right_ref}")
        return str(merged)

    def _commit_tree(self, commit: str) -> pygit2.Tree:
        obj = self._repo.get(pygit2.Oid(hex=commit))
        if obj is None:
            raise ReplayError(f"Unknown commit oid: {commit}")
        if isinstance(obj, pygit2.Commit):
            return obj.tree
        if isinstance(obj, pygit2.Tag):
            target = self._repo.get(obj.target)
            if isinstance(target, pygit2.Commit):
                return target.tree
        raise ReplayError(f"Object is not a commit-like reference: {commit}")

    def read_blob_bytes(self, commit: str, rel_path: Path) -> bytes | None:
        tree = self._commit_tree(commit)
        key = PurePosixPath(rel_path.as_posix()).as_posix()
        try:
            entry = tree[key]
        except KeyError:
            return None
        obj = self._repo.get(entry.id)
        if not isinstance(obj, pygit2.Blob):
            return None
        return bytes(obj.data)

    def list_tree_paths(self, commit: str, rel_dir: Path) -> tuple[Path, ...]:
        tree = self._commit_tree(commit)
        key = PurePosixPath(rel_dir.as_posix()).as_posix().strip("/")
        subtree: pygit2.Tree = tree
        if key:
            try:
                entry = tree[key]
            except KeyError:
                return ()
            obj = self._repo.get(entry.id)
            if not isinstance(obj, pygit2.Tree):
                return ()
            subtree = obj

        rows: list[Path] = []

        def walk(current_tree: pygit2.Tree, prefix: PurePosixPath) -> None:
            for node in current_tree:
                node_path = prefix / node.name
                obj = self._repo.get(node.id)
                if isinstance(obj, pygit2.Tree):
                    walk(obj, node_path)
                elif isinstance(obj, pygit2.Blob):
                    rows.append(Path(node_path.as_posix()))

        walk(subtree, PurePosixPath(key) if key else PurePosixPath(""))
        rows.sort(key=lambda p: p.as_posix())
        return tuple(rows)

    def has_local_changes(self, *, rel_paths: tuple[Path, ...]) -> bool:
        return bool(self.list_local_changes(rel_paths=rel_paths))

    def list_local_changes(self, *, rel_paths: tuple[Path, ...]) -> tuple[WorktreeChange, ...]:
        statuses = self._repo.status(untracked_files="normal")
        rows: list[WorktreeChange] = []
        filters = tuple(path.as_posix().strip("/") for path in rel_paths)
        for rel, flags in statuses.items():
            candidate = rel.strip("/")
            if filters and not any(
                candidate == prefix or candidate.startswith(f"{prefix}/") for prefix in filters
            ):
                continue
            staged = bool(
                flags
                & (
                    pygit2.GIT_STATUS_INDEX_NEW
                    | pygit2.GIT_STATUS_INDEX_MODIFIED
                    | pygit2.GIT_STATUS_INDEX_DELETED
                    | pygit2.GIT_STATUS_INDEX_RENAMED
                    | pygit2.GIT_STATUS_INDEX_TYPECHANGE
                )
            )
            unstaged = bool(
                flags
                & (
                    pygit2.GIT_STATUS_WT_MODIFIED
                    | pygit2.GIT_STATUS_WT_DELETED
                    | pygit2.GIT_STATUS_WT_RENAMED
                    | pygit2.GIT_STATUS_WT_TYPECHANGE
                )
            )
            untracked = bool(flags & pygit2.GIT_STATUS_WT_NEW)
            if staged or unstaged or untracked:
                rows.append(
                    WorktreeChange(
                        rel_path=candidate,
                        staged=staged,
                        unstaged=unstaged,
                        untracked=untracked,
                    )
                )
        rows.sort(key=lambda row: row.rel_path)
        return tuple(rows)
