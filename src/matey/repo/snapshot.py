from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import pygit2

from matey.paths import PathBoundaryError, describe_path_boundary_error, safe_descendant
from matey.project import TargetConfig


class SnapshotError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class Snapshot:
    target_name: str
    schema_sql: bytes | None
    lock_toml: bytes | None
    migrations: Mapping[str, bytes]
    checkpoints: Mapping[str, bytes]

    @classmethod
    def from_worktree(cls, target: TargetConfig) -> Snapshot:
        target_root = safe_descendant(
            root=target.root,
            candidate=target.root,
            label=f"target {target.name} directory",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
        return cls(
            target_name=target.name,
            schema_sql=_read_optional_file(target_root, target.schema),
            lock_toml=_read_optional_file(target_root, target.lockfile),
            migrations=_read_sql_dir(target_root, target.migrations, prefix="migrations"),
            checkpoints=_read_sql_dir(target_root, target.checkpoints, prefix="checkpoints"),
        )

    @classmethod
    def from_tree(
        cls,
        *,
        target_name: str,
        target_rel_dir: str,
        root_tree: pygit2.Tree,
    ) -> Snapshot:
        target_tree = _tree_at_path(root_tree, target_rel_dir)
        if target_tree is None:
            return cls(
                target_name=target_name,
                schema_sql=None,
                lock_toml=None,
                migrations={},
                checkpoints={},
            )

        schema_sql = _blob_at_path(target_tree, "schema.sql")
        lock_toml = _blob_at_path(target_tree, "schema.lock.toml")
        migrations = _read_sql_tree(target_tree, root_dir="migrations")
        checkpoints = _read_sql_tree(target_tree, root_dir="checkpoints")

        return cls(
            target_name=target_name,
            schema_sql=schema_sql,
            lock_toml=lock_toml,
            migrations=migrations,
            checkpoints=checkpoints,
        )


def _read_optional_file(root: Path, path: Path) -> bytes | None:
    try:
        safe_path = safe_descendant(
            root=root,
            candidate=path,
            label=f"artifact file {path}",
            allow_missing_leaf=True,
            expected_kind="file",
        )
    except PathBoundaryError as error:
        raise SnapshotError(
            describe_path_boundary_error(
                error,
                path=path,
                symlink_message="Refusing to read symlinked file",
            )
        ) from error
    if not safe_path.exists():
        return None
    return safe_path.read_bytes()


def _read_sql_dir(root: Path, directory: Path, *, prefix: str) -> dict[str, bytes]:
    try:
        safe_dir = safe_descendant(
            root=root,
            candidate=directory,
            label=f"artifact dir {directory}",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
    except PathBoundaryError as error:
        raise SnapshotError(
            describe_path_boundary_error(
                error,
                path=directory,
                symlink_message="Refusing to traverse symlinked directory",
            )
        ) from error
    if not safe_dir.exists():
        return {}

    rows: dict[str, bytes] = {}
    for walk_root, dirnames, filenames in os.walk(safe_dir, followlinks=False):
        root_path = Path(walk_root)
        for dirname in list(dirnames):
            child = root_path / dirname
            try:
                safe_descendant(
                    root=root,
                    candidate=child,
                    label=f"artifact dir {child}",
                    allow_missing_leaf=False,
                    expected_kind="dir",
                )
            except PathBoundaryError as error:
                raise SnapshotError(
                    describe_path_boundary_error(
                        error,
                        path=child,
                        symlink_message="Refusing to traverse symlinked directory",
                    )
                ) from error
        for filename in sorted(filenames):
            if not filename.endswith(".sql"):
                continue
            file_path = root_path / filename
            try:
                safe_file = safe_descendant(
                    root=root,
                    candidate=file_path,
                    label=f"artifact file {file_path}",
                    allow_missing_leaf=False,
                    expected_kind="file",
                )
            except PathBoundaryError as error:
                raise SnapshotError(
                    describe_path_boundary_error(
                        error,
                        path=file_path,
                        symlink_message="Refusing to read symlinked file",
                    )
                ) from error
            rel_path = safe_file.relative_to(safe_dir).as_posix()
            key = str(PurePosixPath(prefix) / rel_path)
            rows[key] = safe_file.read_bytes()
    return rows


def _tree_at_path(root: pygit2.Tree, rel_path: str) -> pygit2.Tree | None:
    obj = _resolve_tree_object(root, rel_path)
    if _is_symlink_object(obj):
        raise SnapshotError(f"Refusing to read symlinked tree object at {rel_path!r}.")
    if obj is not None and not isinstance(obj, pygit2.Tree):
        raise SnapshotError(f"Expected tree but found non-tree object at {rel_path!r}.")
    if isinstance(obj, pygit2.Tree):
        return obj
    return None


def _blob_at_path(root: pygit2.Tree, rel_path: str) -> bytes | None:
    obj = _resolve_tree_object(root, rel_path)
    if _is_symlink_object(obj):
        raise SnapshotError(f"Refusing to read symlinked blob object at {rel_path!r}.")
    if obj is not None and not isinstance(obj, pygit2.Blob):
        raise SnapshotError(f"Expected blob but found non-blob object at {rel_path!r}.")
    if isinstance(obj, pygit2.Blob):
        return bytes(obj.data)
    return None


def _resolve_tree_object(root: pygit2.Tree, rel_path: str) -> pygit2.Object | None:
    if not rel_path or rel_path == ".":
        return root

    parts = PurePosixPath(rel_path).parts
    current: pygit2.Object = root
    for idx, part in enumerate(parts):
        if not isinstance(current, pygit2.Tree):
            traversed = PurePosixPath(*parts[:idx]).as_posix()
            raise SnapshotError(f"Expected tree while traversing {traversed!r}.")
        next_obj = _tree_object(current, part)
        if next_obj is None:
            return None
        current = next_obj
        if idx < len(parts) - 1 and not isinstance(current, pygit2.Tree):
            traversed = PurePosixPath(*parts[: idx + 1]).as_posix()
            raise SnapshotError(f"Expected tree but found non-tree object at {traversed!r}.")
    return current


def _read_sql_tree(tree: pygit2.Tree, *, root_dir: str) -> dict[str, bytes]:
    base_obj = _tree_object(tree, root_dir)
    if base_obj is None:
        return {}
    if not isinstance(base_obj, pygit2.Tree):
        raise SnapshotError(f"Expected tree but found non-tree object at {root_dir!r}.")

    rows: dict[str, bytes] = {}
    _collect_tree_sql_rows(base_obj, prefix=PurePosixPath(root_dir), rows=rows)
    return rows


def _collect_tree_sql_rows(
    tree: pygit2.Tree,
    *,
    prefix: PurePosixPath,
    rows: dict[str, bytes],
) -> None:
    for obj in tree:
        entry_path = prefix / obj.name
        if _is_symlink_object(obj):
            raise SnapshotError(f"Refusing to read symlinked tree object at {entry_path.as_posix()!r}.")
        if isinstance(obj, pygit2.Tree):
            _collect_tree_sql_rows(obj, prefix=entry_path, rows=rows)
            continue
        if not isinstance(obj, pygit2.Blob):
            continue
        if entry_path.suffix != ".sql":
            continue
        rows[entry_path.as_posix()] = bytes(obj.data)


def _tree_object(tree: pygit2.Tree, name: str) -> pygit2.Object | None:
    try:
        return tree[name]
    except KeyError:
        return None


def _is_symlink_object(obj: pygit2.Object | None) -> bool:
    return getattr(obj, "filemode", None) == 0o120000


__all__ = ["Snapshot", "SnapshotError"]
