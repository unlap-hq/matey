from __future__ import annotations

from enum import StrEnum


class Engine(StrEnum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"


def parse_engine(raw: str) -> Engine:
    value = raw.strip().lower()
    for engine in Engine:
        if engine.value == value:
            return engine
    raise ValueError(f"Unsupported engine: {raw!r}")
