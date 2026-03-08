from __future__ import annotations

from pathlib import Path

import pytest

from matey.config import TargetConfig
from matey.db import (
    DbError,
    bootstrap,
    down,
    drift,
    migrate,
    plan,
    plan_diff,
    plan_sql,
    status_raw,
    up,
)
from matey.dbmate import Dbmate
from matey.schema import apply
from matey.schema import init_target as init_schema_target
from matey.scratch import Engine

from .conftest import IntegrationRuntime
from .helpers_sql import create_table_sql, migration_sql

pytestmark = pytest.mark.integration


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _bootstrap_target_artifacts(runtime: IntegrationRuntime, target: TargetConfig) -> None:
    _ = apply(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )


def test_db_matrix_up_down_drift_plan_cycle(
    runtime: IntegrationRuntime,
    target: TargetConfig,
    live_url: str,
) -> None:
    _write(
        target.migrations / "001_init.sql",
        migration_sql(engine=runtime.engine, table="it_db_one"),
    )
    _write(
        target.migrations / "002_next.sql",
        migration_sql(engine=runtime.engine, table="it_db_two"),
    )
    _bootstrap_target_artifacts(runtime, target)

    up_result = up(
        target,
        url=live_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert up_result.before_index == 0
    assert up_result.after_index == 2

    status_result = status_raw(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert status_result.exit_code == 0
    assert "001_init.sql" in status_result.stdout
    assert "002_next.sql" in status_result.stdout

    plan_after_up = plan(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert plan_after_up.applied_index == 2
    assert plan_after_up.target_index == 2
    assert plan_after_up.matches is True

    drift_after_up = drift(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert drift_after_up.applied_index == 2
    assert drift_after_up.drifted is False

    down_result = down(target, steps=1, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert down_result.before_index == 2
    assert down_result.after_index == 1

    plan_after_down = plan(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert plan_after_down.applied_index == 1
    assert plan_after_down.target_index == 2
    assert plan_after_down.matches is False

    drift_after_down = drift(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert drift_after_down.applied_index == 1
    assert drift_after_down.drifted is False

    migrate_result = migrate(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert migrate_result.before_index == 1
    assert migrate_result.after_index == 2

    final_plan = plan(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert final_plan.matches is True


def test_db_bootstrap_loads_head_schema(
    runtime: IntegrationRuntime,
    target: TargetConfig,
    live_url: str,
) -> None:
    _write(
        target.migrations / "001_init.sql",
        migration_sql(engine=runtime.engine, table="it_db_bootstrap_one"),
    )
    _write(
        target.migrations / "002_next.sql",
        migration_sql(engine=runtime.engine, table="it_db_bootstrap_two"),
    )
    _bootstrap_target_artifacts(runtime, target)

    bootstrap_result = bootstrap(
        target,
        url=live_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert bootstrap_result.before_index == 0
    assert bootstrap_result.after_index == 2

    status_result = status_raw(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert status_result.exit_code == 0
    assert "Applied: 2" in status_result.stdout
    assert "Pending: 0" in status_result.stdout

    plan_result = plan(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert plan_result.matches is True


def test_db_down_to_zero_verifies_zero_baseline(
    runtime: IntegrationRuntime,
    target: TargetConfig,
    live_url: str,
) -> None:
    _ = init_schema_target(target, engine=runtime.engine.value)
    _write(
        target.migrations / "001_init.sql",
        migration_sql(engine=runtime.engine, table="it_db_zero"),
    )
    _bootstrap_target_artifacts(runtime, target)

    up_result = up(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert up_result.after_index == 1

    down_result = down(target, steps=1, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert down_result.before_index == 1
    assert down_result.after_index == 0

    status_result = status_raw(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert status_result.exit_code == 0
    assert "Applied: 0" in status_result.stdout

    drift_result = drift(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert drift_result.applied_index == 0
    assert drift_result.drifted is False

    plan_result = plan(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert plan_result.applied_index == 0
    assert plan_result.target_index == 1
    assert plan_result.matches is False


def test_db_migrate_requires_existing_database(
    runtime: IntegrationRuntime,
    target: TargetConfig,
    live_url: str,
) -> None:
    _write(
        target.migrations / "001_init.sql",
        migration_sql(engine=runtime.engine, table="it_db_migrate_only"),
    )
    _bootstrap_target_artifacts(runtime, target)

    if runtime.engine is Engine.SQLITE:
        migrate_result = migrate(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
        assert migrate_result.before_index == 0
        assert migrate_result.after_index == 1
        return

    with pytest.raises(DbError, match="db migrate pre-status failed"):
        migrate(target, url=live_url, dbmate_bin=runtime.dbmate_bin)

    dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=runtime.dbmate_bin)
    created = dbmate.database(live_url).create()
    assert created.exit_code == 0

    migrate_result = migrate(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert migrate_result.before_index == 0
    assert migrate_result.after_index == 1


def test_db_plan_sql_and_diff_against_worktree_target(
    runtime: IntegrationRuntime,
    target: TargetConfig,
    live_url: str,
) -> None:
    _write(
        target.migrations / "001_init.sql",
        migration_sql(engine=runtime.engine, table="it_db_plan_base"),
    )
    _bootstrap_target_artifacts(runtime, target)
    _ = up(target, url=live_url, dbmate_bin=runtime.dbmate_bin)

    expected_sql = plan_sql(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert expected_sql.strip()

    extra_schema_sql = create_table_sql(engine=runtime.engine, table="it_db_extra") + "\n"
    dbmate = Dbmate(migrations_dir=target.migrations, dbmate_bin=runtime.dbmate_bin)
    load_result = dbmate.database(live_url).load(extra_schema_sql)
    assert load_result.exit_code == 0, load_result.stderr or load_result.stdout

    drift_result = drift(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert drift_result.drifted is True

    plan_result = plan(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert plan_result.matches is False
    assert plan_result.target_index == 1

    diff_text = plan_diff(target, url=live_url, dbmate_bin=runtime.dbmate_bin)
    assert "--- live/schema.sql" in diff_text
    assert "+++ expected/worktree.sql" in diff_text
