from __future__ import annotations

import pytest

from matey.app.schema_engine import SchemaEngine
from tests.integration.conftest import live_container, write_migration


@pytest.mark.integration
def test_schema_apply_plan_status_sqlite(app_context, runtime, defaults) -> None:
    write_migration(
        runtime=runtime,
        version="001",
        name="init",
        up_sql="CREATE TABLE t(id INTEGER);",
        down_sql="DROP TABLE t;",
    )
    engine = SchemaEngine(context=app_context)

    engine.schema_apply(
        runtime=runtime,
        defaults=defaults,
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override="sqlite3:/tmp/matey-integration-live.sqlite3",
        test_url_override=None,
    )

    status = engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None)
    plan = engine.schema_plan(
        runtime=runtime,
        defaults=defaults,
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override="sqlite3:/tmp/matey-integration-live.sqlite3",
        test_url_override=None,
    )
    sql_text = engine.schema_plan_sql(
        runtime=runtime,
        defaults=defaults,
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override="sqlite3:/tmp/matey-integration-live.sqlite3",
        test_url_override=None,
    )

    assert runtime.paths.schema_file.exists()
    assert runtime.paths.lock_file.exists()
    assert runtime.paths.checkpoints_dir.joinpath("001_init.sql").exists()
    assert status.up_to_date is True
    assert plan.comparison.equal is True
    assert sql_text.strip() != ""


@pytest.mark.integration
@pytest.mark.parametrize("engine_name", ["postgres", "mysql", "clickhouse"])
def test_schema_apply_with_containerized_engines(
    app_context,
    runtime,
    defaults,
    has_docker: bool,
    engine_name: str,
) -> None:
    if not has_docker:
        pytest.skip("Docker is required for containerized integration tests")

    up_sql_by_engine = {
        "postgres": "CREATE TABLE t(id INTEGER);",
        "mysql": "CREATE TABLE t(id INT);",
        "clickhouse": "CREATE TABLE t(id Int32) ENGINE = MergeTree ORDER BY tuple();",
    }
    down_sql_by_engine = {
        "postgres": "DROP TABLE t;",
        "mysql": "DROP TABLE t;",
        "clickhouse": "DROP TABLE t;",
    }

    write_migration(
        runtime=runtime,
        version="001",
        name=f"{engine_name}_init",
        up_sql=up_sql_by_engine[engine_name],
        down_sql=down_sql_by_engine[engine_name],
    )
    engine = SchemaEngine(context=app_context)

    with live_container(engine_name, db_name=f"matey_{engine_name}_it") as live:
        engine.schema_apply(
            runtime=runtime,
            defaults=defaults,
            base_ref=None,
            clean=True,
            keep_scratch=False,
            url_override=live.url,
            test_url_override=live.url,
        )

    status = engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None)
    assert status.up_to_date is True


@pytest.mark.integration
def test_schema_apply_bigquery_env_gated(app_context, runtime, defaults, bigquery_urls) -> None:
    write_migration(
        runtime=runtime,
        version="001",
        name="bq_init",
        up_sql="-- migrate:up transaction:false\nCREATE TABLE t(id INT64);",
        down_sql="-- migrate:down transaction:false\nDROP TABLE t;",
    )
    engine = SchemaEngine(context=app_context)
    engine.schema_apply(
        runtime=runtime,
        defaults=defaults,
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override=bigquery_urls.url,
        test_url_override=bigquery_urls.test_url,
    )
    status = engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None)
    assert status.up_to_date is True
