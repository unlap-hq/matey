from __future__ import annotations

import tempfile
from dataclasses import dataclass
from pathlib import Path

from matey.app.protocols import IEnginePolicyRegistry, IScratchManager, ScratchHandle
from matey.domain.errors import ReplayError
from matey.domain.model import Engine


class SqliteScratchManager:
    def __init__(self) -> None:
        self._files: dict[str, Path] = {}

    def prepare(
        self,
        *,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        build_scratch_url,
    ) -> ScratchHandle:
        if test_base_url and test_base_url.strip():
            url = build_scratch_url(test_base_url.strip(), scratch_name)
            return ScratchHandle(
                engine=Engine.SQLITE,
                url=url,
                scratch_name=scratch_name,
                purpose=purpose,
                auto_provisioned=False,
                cleanup_required=False,
            )

        file_path = Path(tempfile.gettempdir()) / f"{scratch_name}.sqlite3"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        self._files[scratch_name] = file_path
        return ScratchHandle(
            engine=Engine.SQLITE,
            url=f"sqlite3:{file_path.as_posix()}",
            scratch_name=scratch_name,
            purpose=purpose,
            auto_provisioned=True,
            cleanup_required=False,
        )

    def cleanup(self, handle: ScratchHandle) -> None:
        file_path = self._files.pop(handle.scratch_name, None)
        if file_path is not None and file_path.exists():
            file_path.unlink(missing_ok=True)


class BigQueryScratchManager:
    def prepare(
        self,
        *,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        build_scratch_url,
    ) -> ScratchHandle:
        if not test_base_url or not test_base_url.strip():
            raise ReplayError(
                "BigQuery scratch requires a non-empty --test-url or resolved test_url_env value."
            )

        url = build_scratch_url(test_base_url.strip(), scratch_name)
        return ScratchHandle(
            engine=Engine.BIGQUERY,
            url=url,
            scratch_name=scratch_name,
            purpose=purpose,
            auto_provisioned=False,
            cleanup_required=True,
        )

    def cleanup(self, handle: ScratchHandle) -> None:
        # BigQuery dataset cleanup is handled by dbmate drop in command engines.
        return


@dataclass
class _RunningContainer:
    engine: Engine
    container: object


class ContainerizedScratchManager:
    def __init__(self) -> None:
        self._active: dict[str, _RunningContainer] = {}

    def prepare(
        self,
        *,
        engine: Engine,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        build_scratch_url,
    ) -> ScratchHandle:
        if test_base_url and test_base_url.strip():
            return ScratchHandle(
                engine=engine,
                url=build_scratch_url(test_base_url.strip(), scratch_name),
                scratch_name=scratch_name,
                purpose=purpose,
                auto_provisioned=False,
                cleanup_required=False,
            )

        container, url = self._start_container(engine=engine, database_name=scratch_name)
        self._active[scratch_name] = _RunningContainer(engine=engine, container=container)
        return ScratchHandle(
            engine=engine,
            url=url,
            scratch_name=scratch_name,
            purpose=purpose,
            auto_provisioned=True,
            cleanup_required=False,
        )

    def cleanup(self, handle: ScratchHandle) -> None:
        running = self._active.pop(handle.scratch_name, None)
        if running is None:
            return
        stop = getattr(running.container, "stop", None)
        if callable(stop):
            stop()

    def _start_container(self, *, engine: Engine, database_name: str) -> tuple[object, str]:
        if engine is Engine.POSTGRES:
            from testcontainers.postgres import PostgresContainer

            container = PostgresContainer(
                image="postgres:16-alpine",
                username="postgres",
                password="postgres",
                dbname=database_name,
            )
            container.start()
            return container, str(container.get_connection_url())

        if engine is Engine.MYSQL:
            from testcontainers.mysql import MySqlContainer

            container = MySqlContainer(
                image="mysql:8.4",
                username="root",
                password="root",
                dbname=database_name,
            )
            container.start()
            return container, str(container.get_connection_url())

        if engine is Engine.CLICKHOUSE:
            from testcontainers.clickhouse import ClickHouseContainer

            container = ClickHouseContainer(image="clickhouse/clickhouse-server:24.8")
            container.start()
            base_url = str(container.get_connection_url())
            if "/" in base_url.rsplit("@", 1)[-1]:
                url = base_url.rsplit("/", 1)[0] + f"/{database_name}"
            else:
                url = base_url + f"/{database_name}"
            return container, url

        raise ReplayError(f"Containerized scratch is not supported for engine: {engine.value}")


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
        del keep
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
