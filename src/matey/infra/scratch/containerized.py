from __future__ import annotations

from dataclasses import dataclass

from matey.app.protocols import ScratchHandle
from matey.domain.engine import Engine
from matey.domain.errors import ReplayError


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
            # ClickHouse container URL often has a default db path; rewrite to scratch db.
            if "/" in base_url.rsplit("@", 1)[-1]:
                url = base_url.rsplit("/", 1)[0] + f"/{database_name}"
            else:
                url = base_url + f"/{database_name}"
            return container, url

        raise ReplayError(f"Containerized scratch is not supported for engine: {engine.value}")
