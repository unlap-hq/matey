from __future__ import annotations

import os
import subprocess
import tempfile
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from threading import RLock

import portalocker
import typed_settings

from matey.app.protocols import CmdResult, IEnvProvider, IFileSystem, IProcessRunner
from matey.domain.model import TargetKey


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
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
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
