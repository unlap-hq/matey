from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from matey.domain.dbmate_output import DbmateOutput
from matey.domain.engine import Engine
from matey.domain.sql import PreparedSql, SqlComparison, SqlSource
from matey.domain.target import TargetKey


@dataclass(frozen=True)
class CmdResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class ArtifactWrite:
    rel_path: str
    content: bytes


@dataclass(frozen=True)
class ArtifactDelete:
    rel_path: str


@dataclass(frozen=True)
class WorktreeChange:
    rel_path: str
    staged: bool
    unstaged: bool
    untracked: bool


@dataclass(frozen=True)
class ScratchHandle:
    engine: Engine
    url: str
    scratch_name: str
    purpose: str
    auto_provisioned: bool
    cleanup_required: bool


class IProcessRunner(Protocol):
    def run(self, argv: tuple[str, ...], cwd: Path | None = None) -> CmdResult: ...


class IFileSystem(Protocol):
    def read_bytes(self, path: Path) -> bytes: ...
    def read_text(self, path: Path) -> str: ...
    def write_bytes_atomic(self, path: Path, data: bytes) -> None: ...
    def write_text_atomic(self, path: Path, data: str) -> None: ...
    def exists(self, path: Path) -> bool: ...
    def mkdir(self, path: Path, parents: bool = False) -> None: ...
    def list_files(self, path: Path) -> tuple[Path, ...]: ...


class IEnvProvider(Protocol):
    def get(self, key: str, default: str | None = None) -> str | None: ...
    def require(self, key: str) -> str: ...


class IGitRepo(Protocol):
    def repo_root(self) -> Path: ...
    def head_commit(self) -> str: ...
    def resolve_ref(self, ref: str) -> str: ...
    def merge_base(self, left_ref: str, right_ref: str) -> str: ...
    def read_blob_bytes(self, commit: str, rel_path: Path) -> bytes | None: ...
    def list_tree_paths(self, commit: str, rel_dir: Path) -> tuple[Path, ...]: ...
    def has_local_changes(self, *, rel_paths: tuple[Path, ...]) -> bool: ...
    def list_local_changes(self, *, rel_paths: tuple[Path, ...]) -> tuple[WorktreeChange, ...]: ...


class ISqlPipeline(Protocol):
    def prepare(self, *, engine: Engine, source: SqlSource) -> PreparedSql: ...
    def compare(self, *, engine: Engine, expected: SqlSource, actual: SqlSource) -> SqlComparison: ...


@dataclass(frozen=True)
class EngineClassifierPolicy:
    missing_db_positive: tuple[str, ...]
    missing_db_negative: tuple[str, ...]
    create_exists: tuple[str, ...]
    create_fatal: tuple[str, ...]


@dataclass(frozen=True)
class EnginePolicy:
    wait_required: bool
    requires_test_url_for_index0: bool
    build_scratch_url: Callable[[str, str], str]
    classifier: EngineClassifierPolicy


class IEnginePolicyRegistry(Protocol):
    def get(self, engine: Engine) -> EnginePolicy: ...


class IScratchManager(Protocol):
    def prepare(
        self,
        *,
        engine: Engine,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        keep: bool,
    ) -> ScratchHandle: ...

    def cleanup(self, handle: ScratchHandle) -> None: ...


class IDbmateGateway(Protocol):
    def new(self, name: str, migrations_dir: Path) -> CmdResult: ...
    def wait(self, url: str, timeout_seconds: int) -> CmdResult: ...
    def create(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def drop(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def up(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult: ...
    def migrate(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult: ...
    def rollback(self, url: str, migrations_dir: Path, steps: int, no_dump_schema: bool = True) -> CmdResult: ...
    def load_schema(
        self,
        url: str,
        schema_path: Path,
        migrations_dir: Path,
        no_dump_schema: bool = True,
    ) -> CmdResult: ...
    def dump(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def status(self, url: str, migrations_dir: Path) -> CmdResult: ...
    def raw(self, argv_suffix: tuple[str, ...], url: str, migrations_dir: Path) -> CmdResult: ...

    @staticmethod
    def to_output(result: CmdResult) -> DbmateOutput:
        return DbmateOutput(exit_code=result.exit_code, stdout=result.stdout, stderr=result.stderr)


class IArtifactStore(Protocol):
    def recover_pending(self, *, target_key: TargetKey, target_root: Path) -> None: ...

    def begin(
        self,
        *,
        target_key: TargetKey,
        target_root: Path,
        writes: tuple[ArtifactWrite, ...],
        deletes: tuple[ArtifactDelete, ...],
    ) -> str: ...

    def apply(self, *, txid: str) -> None: ...
    def finalize(self, *, txid: str) -> None: ...


class ICommandScope(Protocol):
    def open(self, *, target_key: TargetKey, target_root: Path) -> AbstractContextManager[None]: ...
