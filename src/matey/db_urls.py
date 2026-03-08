from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import SplitResult, parse_qsl, urlencode, urlsplit, urlunsplit

import ibis
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

import matey.bqemu as bqemu
from matey import Engine


@dataclass(frozen=True, slots=True)
class SqlAlchemyTarget:
    url: str
    env: dict[str, str]
    engine_kwargs: dict[str, Any]


@dataclass(frozen=True, slots=True)
class IbisTarget:
    kind: str
    backend: Any
    database: str | tuple[str, str] | None


def sqlalchemy_target(*, engine: Engine, url: str) -> SqlAlchemyTarget:
    parsed = urlsplit(url)

    if engine is Engine.SQLITE:
        path = url.removeprefix("sqlite3:")
        return SqlAlchemyTarget(f"sqlite:///{path}", {}, {})

    if engine is Engine.POSTGRES:
        query = parsed.query
        if "sslmode=" not in query:
            query = f"{query}&sslmode=disable" if query else "sslmode=disable"
        return SqlAlchemyTarget(
            url=urlunsplit(
                SplitResult(
                    scheme="postgresql+psycopg",
                    netloc=parsed.netloc,
                    path=parsed.path,
                    query=query,
                    fragment=parsed.fragment,
                )
            ),
            env={},
            engine_kwargs={},
        )

    if engine is Engine.MYSQL:
        return SqlAlchemyTarget(
            url=urlunsplit(
                SplitResult(
                    scheme="mysql+pymysql",
                    netloc=parsed.netloc,
                    path=parsed.path,
                    query=parsed.query,
                    fragment=parsed.fragment,
                )
            ),
            env={},
            engine_kwargs={},
        )

    if engine is Engine.CLICKHOUSE:
        return SqlAlchemyTarget(
            url=urlunsplit(
                SplitResult(
                    scheme="clickhouse+native",
                    netloc=parsed.netloc,
                    path=parsed.path,
                    query=parsed.query,
                    fragment=parsed.fragment,
                )
            ),
            env={},
            engine_kwargs={},
        )

    if engine is Engine.BIGQUERY:
        project, location, dataset = parse_bigquery_url(url)
        return SqlAlchemyTarget(
            url=f"bigquery://{project}/{dataset}",
            env={},
            engine_kwargs={"location": location} if location is not None else {},
        )

    if engine is Engine.BIGQUERY_EMULATOR:
        hostport, project, location, dataset = bqemu.parse_bigquery_emulator_url(url)
        return SqlAlchemyTarget(
            url=f"bigquery://{project}/{dataset}",
            env={"BIGQUERY_EMULATOR_HOST": f"http://{hostport}"},
            engine_kwargs={"location": location} if location is not None else {},
        )

    raise ValueError(f"Unsupported engine for SQLAlchemy target: {engine.value}")


def ibis_target(*, engine: Engine, url: str) -> IbisTarget:
    parsed = urlsplit(url)

    if engine is Engine.SQLITE:
        return IbisTarget("ibis", ibis.sqlite.connect(url.removeprefix("sqlite3:")), None)

    if engine is Engine.POSTGRES:
        database = parsed.path.lstrip("/")
        return IbisTarget(
            "ibis",
            ibis.postgres.connect(
                host=parsed.hostname,
                user=parsed.username,
                password=parsed.password,
                port=parsed.port or 5432,
                database=database,
            ),
            None,
        )

    if engine is Engine.MYSQL:
        database = parsed.path.lstrip("/")
        return IbisTarget(
            "ibis",
            ibis.mysql.connect(
                host=parsed.hostname or "localhost",
                user=parsed.username,
                password=parsed.password,
                port=parsed.port or 3306,
                database=database,
            ),
            None,
        )

    if engine is Engine.CLICKHOUSE:
        database = parsed.path.lstrip("/") or "default"
        return IbisTarget(
            "ibis",
            ibis.clickhouse.connect(
                host=parsed.hostname or "localhost",
                port=clickhouse_http_port(url),
                database=database,
                user=parsed.username or "default",
                password=parsed.password or "",
            ),
            None,
        )

    if engine is Engine.BIGQUERY:
        project, location, dataset = parse_bigquery_url(url)
        backend = ibis.bigquery.connect(
            project_id=project,
            dataset_id=dataset,
            location=location,
        )
        return IbisTarget("ibis", backend, (project, dataset))

    if engine is Engine.BIGQUERY_EMULATOR:
        hostport, project, location, dataset = bqemu.parse_bigquery_emulator_url(url)
        client = bigquery.Client(
            project=project,
            credentials=AnonymousCredentials(),
            client_options={"api_endpoint": f"http://{hostport}"},
        )
        return IbisTarget("bigquery-emulator-client", client, (project, dataset))

    raise ValueError(f"Unsupported engine for Ibis target: {engine.value}")


def parse_bigquery_url(url: str) -> tuple[str, str | None, str]:
    parsed = urlsplit(url)
    project = parsed.netloc
    parts = [segment for segment in parsed.path.split("/") if segment]
    if len(parts) == 2:
        location, dataset = parts
    elif len(parts) == 1:
        location, dataset = None, parts[0]
    else:
        raise ValueError(f"Unsupported BigQuery URL: {url}")
    return project, location, dataset


def clickhouse_http_port(url: str) -> int:
    parsed = urlsplit(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    value = params.get("http_port")
    if value is None:
        raise ValueError(
            "ClickHouse URL is missing required http_port query parameter for Ibis-backed operations."
        )
    return int(value)


def with_clickhouse_http_port(url: str, http_port: int) -> str:
    return set_query_param(url=url, key="http_port", value=str(http_port))


def set_query_param(*, url: str, key: str, value: str) -> str:
    parsed = urlsplit(url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    params[key] = value
    return urlunsplit(
        SplitResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=parsed.path,
            query=urlencode(params),
            fragment=parsed.fragment,
        )
    )


def dbmate_target(url: str) -> str:
    if bqemu.is_bigquery_emulator_url(url):
        return bqemu.to_dbmate_bigquery_url(url)
    parsed = urlsplit(url)
    if parsed.scheme == "clickhouse":
        params = dict(parse_qsl(parsed.query, keep_blank_values=True))
        params.pop("http_port", None)
        return urlunsplit(
            SplitResult(
                scheme=parsed.scheme,
                netloc=parsed.netloc,
                path=parsed.path,
                query=urlencode(params),
                fragment=parsed.fragment,
            )
        )
    return url
