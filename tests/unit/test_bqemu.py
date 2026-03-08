from __future__ import annotations

from matey.bqemu import (
    DEFAULT_BIGQUERY_EMULATOR_LOCATION,
    build_bigquery_emulator_url,
    rewrite_bigquery_emulator_url,
)


def test_rewrite_bigquery_emulator_url_preserves_query_and_fragment() -> None:
    assert (
        rewrite_bigquery_emulator_url(
            base_url="bigquery-emulator://127.0.0.1:9050/matey/us/original?x=1#frag",
            scratch_name="scratch_ds",
        )
        == "bigquery-emulator://127.0.0.1:9050/matey/us/scratch_ds?x=1#frag"
    )


def test_build_bigquery_emulator_url_defaults_to_locationless() -> None:
    assert (
        build_bigquery_emulator_url(
            hostport="127.0.0.1:9050",
            project="matey",
            dataset="scratch_ds",
        )
        == "bigquery-emulator://127.0.0.1:9050/matey/scratch_ds"
    )


def test_build_bigquery_emulator_url_accepts_explicit_default_location() -> None:
    assert (
        build_bigquery_emulator_url(
            hostport="127.0.0.1:9050",
            project="matey",
            dataset="scratch_ds",
            location=DEFAULT_BIGQUERY_EMULATOR_LOCATION,
        )
        == "bigquery-emulator://127.0.0.1:9050/matey/us/scratch_ds"
    )
