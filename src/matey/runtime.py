from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from matey.artifacts import SqliteArtifactStore
from matey.contracts import (
    IArtifactStore,
    ICommandScope,
    IDbmateGateway,
    IEnginePolicyRegistry,
    IEnvProvider,
    IFileSystem,
    IGitRepo,
    IProcessRunner,
    IScratchManager,
    ISqlPipeline,
)
from matey.dbmate import DbmateGateway
from matey.engine import EnginePolicyRegistry, ScratchManager
from matey.models import TargetKey, assert_target_identity
from matey.platform import (
    LocalFileSystem,
    PygitRepository,
    ReentrantLockManager,
    SubprocessRunner,
    TypedSettingsEnvProvider,
)
from matey.sql import SqlPipeline


@dataclass(frozen=True)
class AppContext:
    fs: IFileSystem
    proc: IProcessRunner
    env: IEnvProvider
    git: IGitRepo
    dbmate: IDbmateGateway
    sql_pipeline: ISqlPipeline
    engine_policies: IEnginePolicyRegistry
    scratch: IScratchManager
    artifact_store: IArtifactStore
    scope: ICommandScope


class CommandScope(ICommandScope):
    def __init__(
        self,
        *,
        repo_root: Path,
        lock_manager: ReentrantLockManager,
        artifact_store: IArtifactStore,
    ) -> None:
        self._repo_root = repo_root
        self._lock_manager = lock_manager
        self._artifact_store = artifact_store

    @contextmanager
    def open(self, *, target_key: TargetKey, target_root: Path):
        assert_target_identity(repo_root=self._repo_root, db_dir=target_root, target_key=target_key)
        with self._lock_manager.hold(target_key=target_key):
            self._artifact_store.recover_pending(target_key=target_key, target_root=target_root)
            yield


def build_context(*, cwd: Path | None = None, dbmate_bin: Path | None = None) -> AppContext:
    fs = LocalFileSystem()
    proc = SubprocessRunner()
    env = TypedSettingsEnvProvider()
    git = PygitRepository(cwd=cwd)
    engine_policies = EnginePolicyRegistry()
    dbmate = DbmateGateway(runner=proc, env=env, dbmate_binary=dbmate_bin)
    sql_pipeline = SqlPipeline()
    artifact_store = SqliteArtifactStore(repo_root=git.repo_root())
    lock_manager = ReentrantLockManager(lock_root=git.repo_root() / ".matey" / "locks")
    scope = CommandScope(
        repo_root=git.repo_root(),
        lock_manager=lock_manager,
        artifact_store=artifact_store,
    )
    scratch = ScratchManager(engine_policies=engine_policies)
    return AppContext(
        fs=fs,
        proc=proc,
        env=env,
        git=git,
        dbmate=dbmate,
        sql_pipeline=sql_pipeline,
        engine_policies=engine_policies,
        scratch=scratch,
        artifact_store=artifact_store,
        scope=scope,
    )
