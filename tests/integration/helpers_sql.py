from __future__ import annotations

from matey.scratch import Engine


def migration_sql(*, engine: Engine, table: str) -> str:
    return (
        "-- migrate:up\n"
        f"{create_table_sql(engine=engine, table=table)}\n"
        "\n"
        "-- migrate:down\n"
        f"{drop_table_sql(table=table)}\n"
    )


def create_table_sql(*, engine: Engine, table: str) -> str:
    match engine:
        case Engine.CLICKHOUSE:
            return f"CREATE TABLE {table} (id Int64) ENGINE = MergeTree ORDER BY tuple();"
        case Engine.BIGQUERY | Engine.BIGQUERY_EMULATOR:
            return f"CREATE TABLE {table} (id INT64);"
        case _:
            return f"CREATE TABLE {table} (id BIGINT);"


def drop_table_sql(*, table: str) -> str:
    return f"DROP TABLE {table};"
