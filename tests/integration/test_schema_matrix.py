from __future__ import annotations

from pathlib import Path

import pytest

from matey.config import TargetConfig
from matey.schema import apply, plan, plan_diff, plan_sql, status

from .conftest import IntegrationRuntime
from .helpers_sql import migration_sql

pytestmark = pytest.mark.integration


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_schema_matrix_plan_apply_cycle(runtime: IntegrationRuntime, target: TargetConfig) -> None:
    _write(
        target.migrations / "001_init.sql",
        migration_sql(engine=runtime.engine, table="it_schema_one"),
    )
    _write(
        target.migrations / "002_next.sql",
        migration_sql(engine=runtime.engine, table="it_schema_two"),
    )

    before = plan(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert before.tail_count == 2
    assert before.matches is False

    first_apply = apply(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert first_apply.wrote is True

    state_after_first_apply = status(target)
    assert state_after_first_apply.is_clean is True

    after_first_apply = plan(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert after_first_apply.tail_count == 0
    assert after_first_apply.matches is True

    _write(
        target.migrations / "003_tail.sql",
        migration_sql(engine=runtime.engine, table="it_schema_three"),
    )

    changed_plan = plan(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert changed_plan.tail_count == 1
    assert changed_plan.matches is False

    expected_sql = plan_sql(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert "CREATE TABLE" in expected_sql.upper()

    diff_text = plan_diff(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert "--- worktree/schema.sql" in diff_text
    assert "+++ replay/schema.sql" in diff_text

    second_apply = apply(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert second_apply.wrote is True

    final_plan = plan(
        target,
        test_base_url=runtime.test_base_url,
        dbmate_bin=runtime.dbmate_bin,
    )
    assert final_plan.tail_count == 0
    assert final_plan.matches is True
