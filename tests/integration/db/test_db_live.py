from __future__ import annotations

import pytest

from matey.db import DbEngine
from matey.schema import SchemaEngine
from tests.integration.conftest import write_migration


@pytest.mark.integration
def test_db_guarded_workflow_sqlite(app_context, runtime, defaults) -> None:
    write_migration(
        runtime=runtime,
        version="001",
        name="first",
        up_sql="CREATE TABLE t1(id INTEGER);",
        down_sql="DROP TABLE t1;",
    )
    schema_engine = SchemaEngine(context=app_context)
    db_engine = DbEngine(context=app_context, schema_engine=schema_engine)
    live_url = f"sqlite3:{(runtime.paths.db_dir / 'live.sqlite3').as_posix()}"

    schema_engine.schema_apply(
        runtime=runtime,
        defaults=defaults,
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override=live_url,
        test_url_override=None,
    )

    db_engine.db_up(
        runtime=runtime,
        defaults=defaults,
        url_override=live_url,
        test_url_override=None,
        keep_scratch=False,
    )

    write_migration(
        runtime=runtime,
        version="002",
        name="second",
        up_sql="CREATE TABLE t2(id INTEGER);",
        down_sql="DROP TABLE t2;",
    )
    schema_engine.schema_apply(
        runtime=runtime,
        defaults=defaults,
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override=live_url,
        test_url_override=None,
    )

    db_engine.db_migrate(
        runtime=runtime,
        defaults=defaults,
        url_override=live_url,
        test_url_override=None,
        keep_scratch=False,
    )

    drift = db_engine.db_drift(
        runtime=runtime,
        defaults=defaults,
        url_override=live_url,
        test_url_override=None,
        keep_scratch=False,
    )
    plan = db_engine.db_plan(
        runtime=runtime,
        defaults=defaults,
        url_override=live_url,
        test_url_override=None,
        keep_scratch=False,
    )
    planned_sql = db_engine.db_plan_sql(runtime=runtime, defaults=defaults)
    status_text = db_engine.db_status(runtime=runtime, url_override=live_url)

    assert drift.result.comparison.equal is True
    assert plan.result.comparison.equal is True
    assert planned_sql == runtime.paths.schema_file.read_text(encoding="utf-8")
    assert "applied" in status_text.lower()

    db_engine.db_down(
        runtime=runtime,
        defaults=defaults,
        steps=1,
        url_override=live_url,
        test_url_override=None,
        keep_scratch=False,
    )
    db_engine.db_drift(
        runtime=runtime,
        defaults=defaults,
        url_override=live_url,
        test_url_override=None,
        keep_scratch=False,
    )
    plan_after_down = db_engine.db_plan(
        runtime=runtime,
        defaults=defaults,
        url_override=live_url,
        test_url_override=None,
        keep_scratch=False,
    )
    assert plan_after_down.result.comparison.equal is False
