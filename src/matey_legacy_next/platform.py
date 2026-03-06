from __future__ import annotations

import os
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from threading import RLock

import portalocker
import pygit2
import typed_settings

from matey.contracts import IEnvProvider, IFileSystem, IGitRepo, IProcessRunner
from matey.errors import ReplayError
from matey.models import CmdResult, TargetKey, WorktreeChange


def normalized_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def resolve_inside(root: Path, rel: str) -> Path:
    candidate = (root / rel).resolve()
    root_resolved = root.resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as error:
        raise ValueError(f"Path escapes root: {rel!r}") from error
    return candidate


@dataclass(frozen=True)
class RuntimeSettings:
    matey_dbmate_bin: str = ""
    matey_dbmate_wait_timeout: int = 60
    github_base_ref: str = ""
    ci_merge_request_target_branch_name: str = ""
    buildkite_pull_request_base_branch: str = ""


class TypedSettingsEnvProvider(IEnvProvider):
    def __init__(self) -> None:
        self._settings = typed_settings.load(
            RuntimeSettings,
            "matey",
            config_files=(),
            env_prefix=None,
        )

    @property
    def settings(self) -> RuntimeSettings:
        return self._settings

    def get(self, key: str, default: str | None = None) -> str | None:
        return os.environ.get(key, default)

    def require(self, key: str) -> str:
        value = self.get(key)
        if value is None or not value.strip():
            raise KeyError(f"Required environment variable is missing: {key}")
        return value.strip()


class LocalFileSystem(IFileSystem):
    def read_bytes(self, path: Path) -> bytes:
        return path.read_bytes()

    def read_text(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def write_bytes_atomic(self, path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
        )
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def write_text_atomic(self, path: Path, data: str) -> None:
        self.write_bytes_atomic(path, data.encode("utf-8"))

    def exists(self, path: Path) -> bool:
        return path.exists()

    def mkdir(self, path: Path, parents: bool = False) -> None:
        path.mkdir(parents=parents, exist_ok=True)

    def list_files(self, path: Path) -> tuple[Path, ...]:
        if not path.exists():
            return ()
        return tuple(sorted((p for p in path.iterdir() if p.is_file()), key=lambda p: p.name))


class SubprocessRunner(IProcessRunner):
    def run(self, argv: tuple[str, ...], cwd: Path | None = None) -> CmdResult:
        import subprocess

        completed = subprocess.run(
            list(argv),
            cwd=str(cwd) if cwd is not None else None,
            check=False,
            capture_output=True,
            text=True,
        )
        return CmdResult(
            argv=argv,
            exit_code=int(completed.returncode),
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )


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
