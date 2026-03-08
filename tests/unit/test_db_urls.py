from __future__ import annotations

import pytest

from matey.bqemu import (
    is_bigquery_emulator_url,
    parse_bigquery_emulator_url,
    to_dbmate_bigquery_url,
)
from matey.db_urls import with_clickhouse_http_port


def test_parse_bigquery_emulator_url_with_location() -> None:
    assert parse_bigquery_emulator_url(
        "bigquery-emulator://127.0.0.1:9050/matey/us/scratch_ds"
    ) == ("127.0.0.1:9050", "matey", "us", "scratch_ds")


def test_parse_bigquery_emulator_url_without_location() -> None:
    assert parse_bigquery_emulator_url(
        "bigquery-emulator://127.0.0.1:9050/matey/scratch_ds"
    ) == ("127.0.0.1:9050", "matey", None, "scratch_ds")


def test_parse_bigquery_emulator_url_rejects_invalid_shape() -> None:
    with pytest.raises(ValueError, match="BigQuery emulator URL must be one of"):
        parse_bigquery_emulator_url("bigquery-emulator://127.0.0.1:9050/matey")


def test_to_dbmate_bigquery_url_is_identity_for_non_emulator() -> None:
    url = "postgres://u:p@127.0.0.1:5432/app"
    assert to_dbmate_bigquery_url(url) == url


def test_is_bigquery_emulator_url_matches_scheme() -> None:
    assert is_bigquery_emulator_url("bigquery-emulator://127.0.0.1:9050/matey/scratch_ds")
    assert not is_bigquery_emulator_url("bigquery://matey/us/scratch_ds")


def test_with_clickhouse_http_port_sets_query_param() -> None:
    assert with_clickhouse_http_port(
        "clickhouse://test:test@localhost:9000/testdb",
        8123,
    ) == "clickhouse://test:test@localhost:9000/testdb?http_port=8123"
