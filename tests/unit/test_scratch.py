from __future__ import annotations

from pathlib import Path

import pytest

from matey.domain import ScratchProvisionError
from matey.drivers.scratch import build_scratch_url, detect_engine, plan_scratch_target


def test_detect_engine_by_url_scheme() -> None:
    assert detect_engine("postgres://u:p@localhost/db") == "postgres"
    assert detect_engine("mysql://u:p@localhost/db") == "mysql"
    assert detect_engine("sqlite:/tmp/db.sqlite3") == "sqlite"
    assert detect_engine("clickhouse://localhost/default") == "clickhouse"
    assert detect_engine("bigquery://my-project/us/my_dataset") == "bigquery"


def test_detect_engine_rejects_unsupported_scheme() -> None:
    with pytest.raises(ScratchProvisionError, match="Unsupported database URL scheme"):
        detect_engine("mongodb://localhost")


def test_detect_engine_rejects_spanner_for_schema_workflows() -> None:
    with pytest.raises(ScratchProvisionError, match="Spanner scratch is not supported"):
        detect_engine("spanner-postgres://127.0.0.1:5432/app")


def test_build_scratch_url_replaces_database_name_for_server_dbs() -> None:
    assert (
        build_scratch_url(
            "postgres",
            "postgres://u:p@127.0.0.1:5432/postgres?sslmode=disable",
            "scratch_db",
        )
        == "postgres://u:p@127.0.0.1:5432/scratch_db?sslmode=disable"
    )
    assert (
        build_scratch_url("mysql", "mysql://u:p@127.0.0.1:3306/mysql", "scratch_db")
        == "mysql://u:p@127.0.0.1:3306/scratch_db"
    )


def test_build_scratch_url_for_sqlite_uses_same_parent_directory() -> None:
    scratch_url = build_scratch_url("sqlite", "sqlite:/tmp/base.sqlite3", "scratch_sqlite")
    assert scratch_url.endswith("/tmp/scratch_sqlite.sqlite3")


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        ("bigquery://my-project", "bigquery://my-project/scratch_ds"),
        ("bigquery://my-project/us", "bigquery://my-project/us/scratch_ds"),
        ("bigquery://my-project/my_dataset", "bigquery://my-project/scratch_ds"),
        ("bigquery://my-project/us/my_dataset", "bigquery://my-project/us/scratch_ds"),
    ],
)
def test_build_scratch_url_for_bigquery_base_and_placeholder_forms(
    base_url: str,
    expected: str,
) -> None:
    assert build_scratch_url("bigquery", base_url, "scratch_ds") == expected


def test_build_scratch_url_for_bigquery_preserves_query_params() -> None:
    base_url = (
        "bigquery://my-project/us/my_dataset"
        "?disable_auth=true&endpoint=http%3A%2F%2F127.0.0.1%3A9050"
    )
    result = build_scratch_url("bigquery", base_url, "scratch_ds")
    assert (
        result
        == "bigquery://my-project/us/scratch_ds"
        "?disable_auth=true&endpoint=http%3A%2F%2F127.0.0.1%3A9050"
    )


def test_build_scratch_url_for_bigquery_rejects_invalid_base_shape() -> None:
    with pytest.raises(ScratchProvisionError, match="must be one of"):
        build_scratch_url("bigquery", "bigquery://my-project/us/dataset/extra", "scratch_ds")


def test_plan_scratch_target_uses_given_test_url_without_docker() -> None:
    planned = plan_scratch_target(
        engine="postgres",
        scratch_name="scratch_db",
        test_url="postgres://u:p@127.0.0.1:5432/postgres?sslmode=disable",
    )
    assert planned.target.scratch_url.endswith("/scratch_db?sslmode=disable")
    assert planned.target.auto_provisioned is False


def test_plan_scratch_target_sqlite_without_test_url_creates_tempdir() -> None:
    planned = plan_scratch_target(engine="sqlite", scratch_name="scratch_db", test_url=None)
    try:
        assert planned.target.auto_provisioned is True
        assert planned.target.cleanup_required is False
        assert "scratch_db.sqlite3" in planned.target.scratch_url
        sqlite_path = planned.target.scratch_url.removeprefix("sqlite3:")
        assert Path(sqlite_path).parent.exists()
    finally:
        planned.cleanup()


def test_plan_scratch_target_uses_docker_when_no_test_url(monkeypatch) -> None:
    cleanup_called = {"called": False}

    def _fake_start(engine: str) -> tuple[str, object]:
        del engine

        def _cleanup() -> None:
            cleanup_called["called"] = True

        return "postgres://u:p@127.0.0.1:5432/postgres?sslmode=disable", _cleanup

    monkeypatch.setattr("matey.drivers.scratch._start_docker_server", _fake_start)

    planned = plan_scratch_target(engine="postgres", scratch_name="scratch_db", test_url=None)
    assert planned.target.auto_provisioned is True
    assert planned.target.cleanup_required is True
    assert planned.target.scratch_url.endswith("/scratch_db?sslmode=disable")
    planned.cleanup()
    assert cleanup_called["called"] is True


def test_plan_scratch_target_bigquery_requires_explicit_test_url() -> None:
    with pytest.raises(ScratchProvisionError, match="BigQuery scratch requires --test-url"):
        plan_scratch_target(engine="bigquery", scratch_name="scratch_db", test_url=None)
