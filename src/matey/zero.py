from __future__ import annotations

from matey.scratch import Engine


def zero_schema_sql(*, engine: Engine) -> str:
    if engine is Engine.CLICKHOUSE:
        return "CREATE DATABASE IF NOT EXISTS __db__"
    return ""


__all__ = ["zero_schema_sql"]
