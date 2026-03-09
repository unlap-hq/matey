from __future__ import annotations

from matey import Engine
from matey.db_urls import sqlalchemy_target


def test_sqlalchemy_target_sqlite() -> None:
    target = sqlalchemy_target(
        engine=Engine.SQLITE,
        url="sqlite3:/tmp/codegen.sqlite3",
    )
    assert target.url == "sqlite:////tmp/codegen.sqlite3"
    assert target.connect_args == {}
    assert target.engine_kwargs == {}


def test_sqlalchemy_target_postgres() -> None:
    target = sqlalchemy_target(
        engine=Engine.POSTGRES,
        url="postgresql+psycopg2://user:pass@localhost:5432/testdb",
    )
    assert target.url == "postgresql+psycopg://user:pass@localhost:5432/testdb?sslmode=disable"
    assert target.connect_args == {}
    assert target.engine_kwargs == {}


def test_sqlalchemy_target_mysql() -> None:
    target = sqlalchemy_target(
        engine=Engine.MYSQL,
        url="mysql://root:root@localhost:3306/testdb",
    )
    assert target.url == "mysql+pymysql://root:root@localhost:3306/testdb"
    assert target.connect_args == {}
    assert target.engine_kwargs == {}


def test_sqlalchemy_target_clickhouse() -> None:
    target = sqlalchemy_target(
        engine=Engine.CLICKHOUSE,
        url="clickhouse://test:test@localhost:9000/testdb?http_port=8123",
    )
    assert target.url == "clickhouse+native://test:test@localhost:9000/testdb?http_port=8123"
    assert target.connect_args == {}
    assert target.engine_kwargs == {}


def test_sqlalchemy_target_bigquery() -> None:
    target = sqlalchemy_target(
        engine=Engine.BIGQUERY,
        url="bigquery://example-project/us/testds",
    )
    assert target.url == "bigquery://example-project/testds"
    assert target.connect_args == {}
    assert target.engine_kwargs == {"location": "us"}


def test_sqlalchemy_target_bigquery_emulator() -> None:
    target = sqlalchemy_target(
        engine=Engine.BIGQUERY_EMULATOR,
        url="bigquery-emulator://127.0.0.1:9050/matey/us/testds",
    )
    assert target.url == "bigquery://matey/testds?user_supplied_client=true"
    assert "client" in target.connect_args
    assert target.engine_kwargs == {"location": "us"}
