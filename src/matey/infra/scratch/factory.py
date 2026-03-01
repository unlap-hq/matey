from __future__ import annotations

from matey.app.protocols import IEnginePolicyRegistry, IScratchManager, ScratchHandle
from matey.domain.engine import Engine
from matey.domain.errors import ReplayError
from matey.infra.scratch.bigquery import BigQueryScratchManager
from matey.infra.scratch.containerized import ContainerizedScratchManager
from matey.infra.scratch.sqlite import SqliteScratchManager


class ScratchManager(IScratchManager):
    def __init__(self, *, engine_policies: IEnginePolicyRegistry) -> None:
        self._engine_policies = engine_policies
        self._sqlite = SqliteScratchManager()
        self._containerized = ContainerizedScratchManager()
        self._bigquery = BigQueryScratchManager()

    def prepare(
        self,
        *,
        engine: Engine,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        keep: bool,
    ) -> ScratchHandle:
        policy = self._engine_policies.get(engine)
        if engine is Engine.SQLITE:
            return self._sqlite.prepare(
                scratch_name=scratch_name,
                purpose=purpose,
                test_base_url=test_base_url,
                build_scratch_url=policy.build_scratch_url,
            )

        if engine is Engine.BIGQUERY:
            return self._bigquery.prepare(
                scratch_name=scratch_name,
                purpose=purpose,
                test_base_url=test_base_url,
                build_scratch_url=policy.build_scratch_url,
            )

        if engine in {Engine.POSTGRES, Engine.MYSQL, Engine.CLICKHOUSE}:
            return self._containerized.prepare(
                engine=engine,
                scratch_name=scratch_name,
                purpose=purpose,
                test_base_url=test_base_url,
                build_scratch_url=policy.build_scratch_url,
            )

        raise ReplayError(f"Unsupported scratch engine: {engine.value}")

    def cleanup(self, handle: ScratchHandle) -> None:
        if handle.engine is Engine.SQLITE:
            self._sqlite.cleanup(handle)
            return
        if handle.engine is Engine.BIGQUERY:
            self._bigquery.cleanup(handle)
            return
        if handle.engine in {Engine.POSTGRES, Engine.MYSQL, Engine.CLICKHOUSE}:
            self._containerized.cleanup(handle)
            return
