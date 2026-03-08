from __future__ import annotations

import json
from pathlib import Path

import pytest

from matey import Engine
from matey.data import apply as apply_data
from matey.data import export as export_data
from matey.db import up
from matey.dbmate import Dbmate
from matey.project import TargetConfig
from matey.schema import apply
from matey.schema import init_target as init_schema_target

from .conftest import IntegrationRuntime

pytestmark = pytest.mark.integration


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _table_sql(engine: Engine, table: str) -> str:
    match engine:
        case Engine.CLICKHOUSE:
            return (
                f"CREATE TABLE {table} (id Int64, name String) "
                "ENGINE = MergeTree ORDER BY tuple();"
            )
        case Engine.BIGQUERY | Engine.BIGQUERY_EMULATOR:
            return f"CREATE TABLE {table} (id INT64, name STRING);"
        case Engine.SQLITE:
            return f"CREATE TABLE {table} (id INTEGER PRIMARY KEY, name TEXT);"
        case _:
            return f"CREATE TABLE {table} (id BIGINT PRIMARY KEY, name VARCHAR(255));"


def _migration_sql(engine: Engine, table: str) -> str:
    return (
        "-- migrate:up\n"
        f"{_table_sql(engine, table)}\n\n"
        "-- migrate:down\n"
        f"DROP TABLE {table};\n"
    )


def _insert_sql(table: str, rows: list[tuple[int, str]]) -> str:
    values = ", ".join(f"({row_id}, '{name}')" for row_id, name in rows)
    return f"INSERT INTO {table} (id, name) VALUES {values};\n"


def _bootstrap_target_artifacts(runtime: IntegrationRuntime, target: TargetConfig) -> None:
    _ = apply(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )


def test_data_export_and_apply_roundtrip(
    runtime: IntegrationRuntime,
    target: TargetConfig,
    live_url: str,
) -> None:
    _ = init_schema_target(target, engine=runtime.engine.value)
    _write(
        target.migrations / "001_roles.sql",
        _migration_sql(runtime.engine, "roles"),
    )
    _bootstrap_target_artifacts(runtime, target)
    _ = up(target, url=live_url, dbmate_bin=runtime.dbmate_bin)

    _write(
        target.data_manifest,
        """
[core]
files = [
  { name = "roles", table = "roles", mode = "replace" },
]
""".strip()
        + "\n",
    )

    dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=runtime.dbmate_bin)
    load_result = dbmate.database(live_url).load(
        _insert_sql("roles", [(2, "viewer"), (1, "admin")])
    )
    assert load_result.exit_code == 0, load_result.stderr or load_result.stdout

    export_result = export_data(target=target, url=live_url, set_name="core")
    assert export_result.set_name == "core"
    assert export_result.files[0].rows == 2

    exported_rows = [
        json.loads(line)
        for line in (target.data_dir / "roles.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert exported_rows == [
        {"id": 1, "name": "admin"},
        {"id": 2, "name": "viewer"},
    ]

    _write(
        target.data_dir / "roles.jsonl",
        '\n'.join(
            [
                json.dumps({"id": 3, "name": "owner"}),
                json.dumps({"id": 4, "name": "member"}),
            ]
        )
        + "\n",
    )

    apply_result = apply_data(target=target, url=live_url, set_name="core")
    assert apply_result.set_name == "core"
    assert apply_result.files[0].rows == 2

    export_result = export_data(target=target, url=live_url, set_name="core")
    assert export_result.files[0].rows == 2
    exported_rows = [
        json.loads(line)
        for line in (target.data_dir / "roles.jsonl").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert exported_rows == [
        {"id": 3, "name": "owner"},
        {"id": 4, "name": "member"},
    ]
