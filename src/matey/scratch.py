from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

from matey.sql import engine_from_url as sql_engine_from_url

_BIGQUERY_MULTI_REGION = {"us", "eu"}
_DEFAULT_POSTGRES_IMAGE = "postgres:16-alpine"
_DEFAULT_MYSQL_IMAGE = "mysql:8.4"


class Engine(StrEnum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"


@dataclass(frozen=True, slots=True)
class ScratchLease:
    engine: Engine
    scratch_name: str
    url: str
    auto_provisioned: bool


class ScratchError(RuntimeError):
    pass


class ScratchConfigError(ScratchError):
    pass


class Scratch:
    def __init__(self, *, temp_root: Path | None = None) -> None:
        self._temp_root = (
            temp_root.resolve() if temp_root is not None else Path(tempfile.gettempdir())
        )

    @contextmanager
    def lease(
        self,
        *,
        engine: Engine,
        scratch_name: str,
        test_base_url: str | None,
        keep: bool = False,
    ) -> Iterator[ScratchLease]:
        base_url = (test_base_url or "").strip()
        cleanup: Callable[[], None] | None = None

        if base_url:
            lease = ScratchLease(
                engine=engine,
                scratch_name=scratch_name,
                url=_build_scratch_url(engine=engine, base_url=base_url, scratch_name=scratch_name),
                auto_provisioned=False,
            )
        else:
            lease, cleanup = self._provision(engine=engine, scratch_name=scratch_name)

        try:
            yield lease
        finally:
            if not keep and lease.auto_provisioned and cleanup is not None:
                cleanup()

    def _provision(
        self,
        *,
        engine: Engine,
        scratch_name: str,
    ) -> tuple[ScratchLease, Callable[[], None]]:
        if engine is Engine.SQLITE:
            self._temp_root.mkdir(parents=True, exist_ok=True)
            file_path = self._temp_root / f"{scratch_name}.sqlite3"
            file_path.touch(exist_ok=True)
            lease = ScratchLease(
                engine=engine,
                scratch_name=scratch_name,
                url=f"sqlite3:{file_path.as_posix()}",
                auto_provisioned=True,
            )
            return lease, lambda: file_path.unlink(missing_ok=True)

        if engine is Engine.BIGQUERY:
            raise ScratchConfigError(
                "BigQuery scratch requires a non-empty test_base_url (--test-url or test_url_env)."
            )

        if engine is Engine.POSTGRES:
            from testcontainers.postgres import PostgresContainer

            image = _postgres_image_for_local_pg_client()
            container = PostgresContainer(
                image=image,
                username="postgres",
                password="postgres",
                dbname=scratch_name,
            )
            container.start()
            lease = ScratchLease(
                engine=engine,
                scratch_name=scratch_name,
                url=str(container.get_connection_url()),
                auto_provisioned=True,
            )
            return lease, container.stop

        if engine is Engine.MYSQL:
            from testcontainers.mysql import MySqlContainer

            image = _mysql_image_for_local_dump_client()
            container = MySqlContainer(
                image=image,
                username="root",
                password="root",
                dbname=scratch_name,
            )
            container.start()
            lease = ScratchLease(
                engine=engine,
                scratch_name=scratch_name,
                url=str(container.get_connection_url()),
                auto_provisioned=True,
            )
            return lease, container.stop

        if engine is Engine.CLICKHOUSE:
            from testcontainers.clickhouse import ClickHouseContainer

            container = ClickHouseContainer(image="clickhouse/clickhouse-server:24.8")
            container.start()
            base_url = str(container.get_connection_url())
            lease = ScratchLease(
                engine=engine,
                scratch_name=scratch_name,
                url=_replace_path_segment(base_url=base_url, scratch_name=scratch_name),
                auto_provisioned=True,
            )
            return lease, container.stop

        raise ScratchError(f"Unsupported scratch engine: {engine.value}")


def engine_from_url(url: str) -> Engine:
    match sql_engine_from_url(url):
        case "postgres" | "postgresql":
            return Engine.POSTGRES
        case "mysql":
            return Engine.MYSQL
        case "sqlite":
            return Engine.SQLITE
        case "clickhouse":
            return Engine.CLICKHOUSE
        case "bigquery":
            return Engine.BIGQUERY
        case _:
            raise ScratchError(f"Unsupported URL scheme for engine inference: {url!r}.")


def _build_scratch_url(*, engine: Engine, base_url: str, scratch_name: str) -> str:
    if engine in {Engine.POSTGRES, Engine.MYSQL, Engine.CLICKHOUSE}:
        return _replace_path_segment(base_url=base_url, scratch_name=scratch_name)
    if engine is Engine.SQLITE:
        return _sqlite_scratch_url(base_url=base_url, scratch_name=scratch_name)
    if engine is Engine.BIGQUERY:
        return _bigquery_scratch_url(base_url=base_url, scratch_name=scratch_name)
    raise ScratchError(f"Unsupported scratch engine for URL build: {engine.value}")


def _replace_path_segment(*, base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    return urlunsplit(
        SplitResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=f"/{scratch_name}",
            query=parsed.query,
            fragment=parsed.fragment,
        )
    )


def _sqlite_scratch_url(*, base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    path = parsed.path or parsed.netloc
    if not path:
        return urlunsplit(
            SplitResult(
                scheme=parsed.scheme or "sqlite3",
                netloc=parsed.netloc,
                path=f"{scratch_name}.sqlite3",
                query=parsed.query,
                fragment=parsed.fragment,
            )
        )
    base = Path(path)
    parent = base.parent if base.suffix else base
    scratch = parent / f"{scratch_name}.sqlite3"
    return urlunsplit(
        SplitResult(
            scheme=parsed.scheme or "sqlite3",
            netloc=parsed.netloc,
            path=scratch.as_posix(),
            query=parsed.query,
            fragment=parsed.fragment,
        )
    )


def _bigquery_scratch_url(*, base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    if not parsed.netloc:
        raise ScratchConfigError("BigQuery scratch base URL must include project host.")

    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) > 2:
        raise ScratchConfigError(
            "BigQuery scratch base URL must be one of: "
            "bigquery://<project>, bigquery://<project>/<dataset>, "
            "bigquery://<project>/<location>/<dataset>."
        )

    if len(segments) == 0:
        scratch_segments = [scratch_name]
    elif len(segments) == 1:
        if _is_location_like(segments[0]):
            raise ScratchConfigError(
                "Ambiguous BigQuery scratch base URL. Use an explicit dataset "
                "(bigquery://<project>/<dataset>) or explicit location+dataset "
                "(bigquery://<project>/<location>/<dataset>)."
            )
        scratch_segments = [scratch_name]
    else:
        scratch_segments = [segments[0], scratch_name]

    return urlunsplit(
        SplitResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=f"/{'/'.join(scratch_segments)}",
            query=parsed.query,
            fragment=parsed.fragment,
        )
    )


def _is_location_like(token: str) -> bool:
    lowered = token.lower()
    return lowered in _BIGQUERY_MULTI_REGION or "-" in lowered


def _postgres_image_for_local_pg_client() -> str:
    major = _detect_client_major(
        binary_name="pg_dump",
        version_flag="--version",
        pattern=r"(\d+)(?:\.\d+)?",
    )
    if major is None:
        return _DEFAULT_POSTGRES_IMAGE
    return f"postgres:{major}-alpine"


def _mysql_image_for_local_dump_client() -> str:
    major = _detect_client_major(
        binary_name="mysqldump",
        version_flag="--version",
        pattern=r"(?:Ver|Distrib)\s+(\d+)(?:\.\d+)?",
        reject_substring="mariadb",
        valid_range=range(5, 11),
    )
    if major is None:
        return _DEFAULT_MYSQL_IMAGE
    return f"mysql:{major}"


def _detect_client_major(
    *,
    binary_name: str,
    version_flag: str,
    pattern: str,
    reject_substring: str | None = None,
    valid_range: range | None = None,
) -> int | None:
    binary_path = shutil.which(binary_name)
    if binary_path is None:
        return None
    try:
        completed = subprocess.run(
            [binary_path, version_flag],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError:
        return None
    if completed.returncode != 0:
        return None
    text = (completed.stdout or completed.stderr).strip()
    if reject_substring is not None and reject_substring.lower() in text.lower():
        return None
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match is None:
        return None
    try:
        major = int(match.group(1))
    except ValueError:
        return None
    if valid_range is not None and major not in valid_range:
        return None
    return major


__all__ = [
    "Engine",
    "Scratch",
    "ScratchConfigError",
    "ScratchError",
    "ScratchLease",
    "engine_from_url",
]
