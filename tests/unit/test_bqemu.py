from __future__ import annotations

import pytest

from matey.bqemu import (
    DEFAULT_BIGQUERY_EMULATOR_LOCATION,
    BigQueryEmulatorUrlError,
    build_bigquery_emulator_url,
    is_bigquery_emulator_url,
    parse_bigquery_emulator_url,
    rewrite_bigquery_emulator_url,
    to_dbmate_bigquery_url,
)


def test_parse_bigquery_emulator_url_with_location() -> None:
    assert parse_bigquery_emulator_url(
        "bigquery-emulator://127.0.0.1:9050/matey/us/scratch_ds"
    ) == ("127.0.0.1:9050", "matey", "us", "scratch_ds")


def test_parse_bigquery_emulator_url_without_location() -> None:
    assert parse_bigquery_emulator_url(
        "bigquery-emulator://127.0.0.1:9050/matey/scratch_ds"
    ) == ("127.0.0.1:9050", "matey", None, "scratch_ds")


def test_parse_bigquery_emulator_url_rejects_invalid_shape() -> None:
    with pytest.raises(BigQueryEmulatorUrlError, match="BigQuery emulator URL must be one of"):
        parse_bigquery_emulator_url("bigquery-emulator://127.0.0.1:9050/matey")


def test_rewrite_bigquery_emulator_url_preserves_query_and_fragment() -> None:
    assert rewrite_bigquery_emulator_url(
        base_url="bigquery-emulator://127.0.0.1:9050/matey/us/original?x=1#frag",
        scratch_name="scratch_ds",
    ) == "bigquery-emulator://127.0.0.1:9050/matey/us/scratch_ds?x=1#frag"


def test_build_bigquery_emulator_url_defaults_to_locationless() -> None:
    assert build_bigquery_emulator_url(
        hostport="127.0.0.1:9050",
        project="matey",
        dataset="scratch_ds",
    ) == "bigquery-emulator://127.0.0.1:9050/matey/scratch_ds"


def test_build_bigquery_emulator_url_accepts_explicit_default_location() -> None:
    assert build_bigquery_emulator_url(
        hostport="127.0.0.1:9050",
        project="matey",
        dataset="scratch_ds",
        location=DEFAULT_BIGQUERY_EMULATOR_LOCATION,
    ) == "bigquery-emulator://127.0.0.1:9050/matey/us/scratch_ds"


def test_to_dbmate_bigquery_url_is_identity_for_non_emulator() -> None:
    url = "postgres://u:p@127.0.0.1:5432/app"
    assert to_dbmate_bigquery_url(url) == url


def test_is_bigquery_emulator_url_matches_scheme() -> None:
    assert is_bigquery_emulator_url("bigquery-emulator://127.0.0.1:9050/matey/scratch_ds")
    assert not is_bigquery_emulator_url("bigquery://matey/us/scratch_ds")
