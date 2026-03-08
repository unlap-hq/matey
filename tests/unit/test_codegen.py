from __future__ import annotations

from matey.schema.codegen import sqlalchemy_target
from matey.scratch import Engine


def test_sqlalchemy_target_sqlite() -> None:
    url, env, kwargs = sqlalchemy_target(
        engine=Engine.SQLITE,
        url="sqlite3:/tmp/codegen.sqlite3",
    )
    assert url == "sqlite:////tmp/codegen.sqlite3"
    assert env == {}
    assert kwargs == {}


def test_sqlalchemy_target_postgres() -> None:
    url, env, kwargs = sqlalchemy_target(
        engine=Engine.POSTGRES,
        url="postgresql+psycopg2://user:pass@localhost:5432/testdb",
    )
    assert url == "postgresql+psycopg://user:pass@localhost:5432/testdb?sslmode=disable"
    assert env == {}
    assert kwargs == {}


def test_sqlalchemy_target_mysql() -> None:
    url, env, kwargs = sqlalchemy_target(
        engine=Engine.MYSQL,
        url="mysql://root:root@localhost:3306/testdb",
    )
    assert url == "mysql+pymysql://root:root@localhost:3306/testdb"
    assert env == {}
    assert kwargs == {}


def test_sqlalchemy_target_clickhouse() -> None:
    url, env, kwargs = sqlalchemy_target(
        engine=Engine.CLICKHOUSE,
        url="clickhouse://test:test@localhost:9000/testdb",
    )
    assert url == "clickhouse+native://test:test@localhost:9000/testdb"
    assert env == {}
    assert kwargs == {}


def test_sqlalchemy_target_bigquery() -> None:
    url, env, kwargs = sqlalchemy_target(
        engine=Engine.BIGQUERY,
        url="bigquery://example-project/us/testds",
    )
    assert url == "bigquery://example-project/testds"
    assert env == {}
    assert kwargs == {"location": "us"}


def test_sqlalchemy_target_bigquery_emulator() -> None:
    url, env, kwargs = sqlalchemy_target(
        engine=Engine.BIGQUERY_EMULATOR,
        url="bigquery-emulator://127.0.0.1:9050/matey/us/testds",
    )
    assert url == "bigquery://matey/testds"
    assert env == {"BIGQUERY_EMULATOR_HOST": "http://127.0.0.1:9050"}
    assert kwargs == {"location": "us"}
