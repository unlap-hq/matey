from __future__ import annotations

from pathlib import Path

from matey.app.context import AppContext
from matey.app.scope import CommandScope
from matey.infra.artifact_store import SqliteArtifactStore
from matey.infra.dbmate import DbmateGateway
from matey.infra.engine_policy import EnginePolicyRegistry
from matey.infra.env import TypedSettingsEnvProvider
from matey.infra.fs import LocalFileSystem
from matey.infra.git import PygitRepository
from matey.infra.locking import ReentrantLockManager
from matey.infra.proc import SubprocessRunner
from matey.infra.scratch.factory import ScratchManager
from matey.infra.sql_pipeline import SqlPipeline


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
