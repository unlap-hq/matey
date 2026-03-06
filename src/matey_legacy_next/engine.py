from __future__ import annotations

import re
import tempfile
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

from matey.contracts import IEnginePolicyRegistry, IScratchManager
from matey.errors import EngineInferenceError, ReplayError
from matey.models import Engine, EngineClassifierPolicy, EnginePolicy, ScratchHandle

_BIGQUERY_MULTI_REGION = {"us", "eu"}


class EnginePolicyRegistry(IEnginePolicyRegistry):
    def __init__(self) -> None:
        self._policies: dict[Engine, EnginePolicy] = {
            Engine.POSTGRES: EnginePolicy(
                wait_required=True,
                requires_test_url_for_index0=False,
                build_scratch_url=_replace_path_segment,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(r"does not exist", r"3d000"),
                    missing_db_negative=(
                        r"connection refused",
                        r"i/o timeout",
                        r"no such host",
                        r"28p01",
                    ),
                    create_exists=(),
                    create_fatal=(r"permission denied", r"access denied"),
                ),
            ),
            Engine.MYSQL: EnginePolicy(
                wait_required=True,
                requires_test_url_for_index0=False,
                build_scratch_url=_replace_path_segment,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(r"unknown database", r"\b1049\b"),
                    missing_db_negative=(
                        r"connection refused",
                        r"i/o timeout",
                        r"no such host",
                        r"\b1045\b",
                    ),
                    create_exists=(),
                    create_fatal=(r"permission denied", r"access denied"),
                ),
            ),
            Engine.SQLITE: EnginePolicy(
                wait_required=False,
                requires_test_url_for_index0=False,
                build_scratch_url=_sqlite_scratch_url,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(
                        r"unable to open database file",
                        r"cannot open database file",
                        r"no such file or directory",
                    ),
                    missing_db_negative=(r"permission denied",),
                    create_exists=(),
                    create_fatal=(r"permission denied",),
                ),
            ),
            Engine.CLICKHOUSE: EnginePolicy(
                wait_required=True,
                requires_test_url_for_index0=False,
                build_scratch_url=_replace_path_segment,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(r"database .* does not exist", r"code:\s*81"),
                    missing_db_negative=(
                        r"connection refused",
                        r"i/o timeout",
                        r"no such host",
                        r"code:\s*516",
                    ),
                    create_exists=(),
                    create_fatal=(r"permission denied", r"access denied"),
                ),
            ),
            Engine.BIGQUERY: EnginePolicy(
                wait_required=False,
                requires_test_url_for_index0=True,
                build_scratch_url=_bigquery_scratch_url,
                classifier=EngineClassifierPolicy(
                    missing_db_positive=(),
                    missing_db_negative=(),
                    create_exists=(r"already exists", r"alreadyexists"),
                    create_fatal=(
                        r"permission denied",
                        r"access denied",
                        r"project .* not found",
                        r"notfound",
                        r"location",
                        r"quota",
                        r"ratelimit",
                        r"invalid",
                        r"badrequest",
                    ),
                ),
            ),
        }

    def get(self, engine: Engine) -> EnginePolicy:
        return self._policies[engine]


def detect_engine_from_url(url: str) -> Engine:
    scheme = urlsplit(url).scheme.lower()
    if scheme in {"postgres", "postgresql"}:
        return Engine.POSTGRES
    if scheme == "mysql":
        return Engine.MYSQL
    if scheme in {"sqlite", "sqlite3"}:
        return Engine.SQLITE
    if scheme.startswith("clickhouse"):
        return Engine.CLICKHOUSE
    if scheme == "bigquery":
        return Engine.BIGQUERY
    raise EngineInferenceError(f"Unsupported database URL scheme: {scheme}")


def classify_missing_db(policy: EnginePolicy, detail_text: str) -> bool:
    text = detail_text.lower()
    if any(re.search(pattern, text) for pattern in policy.classifier.missing_db_negative):
        return False
    return any(re.search(pattern, text) for pattern in policy.classifier.missing_db_positive)


def classify_create_outcome(policy: EnginePolicy, detail_text: str) -> str:
    text = detail_text.lower()
    if any(re.search(pattern, text) for pattern in policy.classifier.create_fatal):
        return "fatal"
    if any(re.search(pattern, text) for pattern in policy.classifier.create_exists):
        return "exists"
    return "ok"


def _replace_path_segment(base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    rebuilt = SplitResult(
        scheme=parsed.scheme,
        netloc=parsed.netloc,
        path=f"/{scratch_name}",
        query=parsed.query,
        fragment=parsed.fragment,
    )
    return urlunsplit(rebuilt)


def _sqlite_scratch_url(base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    path = parsed.path or parsed.netloc
    if not path:
        return f"sqlite3:{scratch_name}.sqlite3"
    base = Path(path)
    parent = base.parent if base.suffix else base
    scratch = parent / f"{scratch_name}.sqlite3"
    return f"sqlite3:{scratch.as_posix()}"


def _is_location_like(token: str) -> bool:
    lowered = token.lower()
    return lowered in _BIGQUERY_MULTI_REGION or "-" in lowered


def _bigquery_scratch_url(base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    if not parsed.netloc:
        raise EngineInferenceError("BigQuery scratch base URL must include project host")

    segments = [seg for seg in parsed.path.split("/") if seg]
    if len(segments) > 2:
        raise EngineInferenceError(
            "BigQuery scratch base URL must be one of: "
            "bigquery://<project>, bigquery://<project>/<location>, "
            "bigquery://<project>/<location>/<dataset> or bigquery://<project>/<dataset>."
        )

    if len(segments) == 0:
        scratch_segments = [scratch_name]
    elif len(segments) == 1:
        if _is_location_like(segments[0]):
            scratch_segments = [segments[0], scratch_name]
        else:
            scratch_segments = [scratch_name]
    else:
        scratch_segments = [segments[0], scratch_name]

    rebuilt = SplitResult(
        scheme=parsed.scheme,
        netloc=parsed.netloc,
        path=f"/{'/'.join(scratch_segments)}",
        query=parsed.query,
        fragment=parsed.fragment,
    )
    return urlunsplit(rebuilt)


class _SqliteScratchManager:
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


class _BigQueryScratchManager:
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
        del handle


@dataclass
class _RunningContainer:
    engine: Engine
    container: object


class _ContainerizedScratchManager:
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
        self._sqlite = _SqliteScratchManager()
        self._containerized = _ContainerizedScratchManager()
        self._bigquery = _BigQueryScratchManager()

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
