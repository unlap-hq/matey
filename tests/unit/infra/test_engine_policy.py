from matey.domain.engine import Engine
from matey.infra.engine_policy import (
    EnginePolicyRegistry,
    classify_create_outcome,
    classify_missing_db,
    detect_engine_from_url,
)


def test_detect_engine_from_url() -> None:
    assert detect_engine_from_url("postgres://u:p@h/db") is Engine.POSTGRES
    assert detect_engine_from_url("mysql://u:p@h/db") is Engine.MYSQL
    assert detect_engine_from_url("sqlite3:/tmp/a.db") is Engine.SQLITE
    assert detect_engine_from_url("clickhouse://u:p@h/db") is Engine.CLICKHOUSE
    assert detect_engine_from_url("bigquery://project/us/ds") is Engine.BIGQUERY


def test_missing_db_classifier_uses_positive_and_negative_tables() -> None:
    policy = EnginePolicyRegistry().get(Engine.POSTGRES)
    assert classify_missing_db(policy, "database does not exist") is True
    assert classify_missing_db(policy, "connection refused") is False


def test_bigquery_create_classifier_distinguishes_exists_and_fatal() -> None:
    policy = EnginePolicyRegistry().get(Engine.BIGQUERY)
    assert classify_create_outcome(policy, "already exists") == "exists"
    assert classify_create_outcome(policy, "permission denied") == "fatal"
    assert classify_create_outcome(policy, "ok") == "ok"
