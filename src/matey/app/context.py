from __future__ import annotations

from dataclasses import dataclass

from matey.app.protocols import (
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
