from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

import pygit2

from matey.config import TargetConfig


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
        return cls(
            target_name=target.name,
            schema_sql=_read_optional_file(target.schema),
            lock_toml=_read_optional_file(target.lockfile),
            migrations=_read_sql_dir(target.migrations, prefix="migrations"),
            checkpoints=_read_sql_dir(target.checkpoints, prefix="checkpoints"),
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


def _read_optional_file(path: Path) -> bytes | None:
    if not path.exists():
        return None
    if not path.is_file():
        raise SnapshotError(f"Expected file but found non-file path: {path}")
    return path.read_bytes()


def _read_sql_dir(directory: Path, *, prefix: str) -> dict[str, bytes]:
    if not directory.exists():
        return {}
    if not directory.is_dir():
        raise SnapshotError(f"Expected directory but found non-directory path: {directory}")

    rows: dict[str, bytes] = {}
    for file_path in sorted(directory.rglob("*.sql")):
        rel_path = file_path.relative_to(directory).as_posix()
        key = str(PurePosixPath(prefix) / rel_path)
        rows[key] = file_path.read_bytes()
    return rows


def _tree_at_path(root: pygit2.Tree, rel_path: str) -> pygit2.Tree | None:
    obj = _resolve_tree_object(root, rel_path)
    if isinstance(obj, pygit2.Tree):
        return obj
    return None


def _blob_at_path(root: pygit2.Tree, rel_path: str) -> bytes | None:
    obj = _resolve_tree_object(root, rel_path)
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
            return None
        next_obj = _tree_object(current, part)
        if next_obj is None:
            return None
        current = next_obj
        if idx < len(parts) - 1 and not isinstance(current, pygit2.Tree):
            return None
    return current


def _read_sql_tree(tree: pygit2.Tree, *, root_dir: str) -> dict[str, bytes]:
    base_obj = _tree_object(tree, root_dir)
    if base_obj is None:
        return {}
    if not isinstance(base_obj, pygit2.Tree):
        return {}

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


__all__ = ["Snapshot", "SnapshotError"]
