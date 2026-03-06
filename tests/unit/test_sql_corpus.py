from __future__ import annotations

import pytest

from matey.sql import SqlError, SqlProgram, has_executable_sql, split_migration_sections


def test_split_migration_sections_only_uses_directive_lines() -> None:
    migration_sql = (
        "-- migrate:up\n"
        "SELECT '-- migrate:down' AS marker;\n"
        "/* -- migrate:down */\n"
        "CREATE TABLE events (id INT64);\n"
        "-- migrate:down\n"
        "DROP TABLE events;\n"
    )

    up_sql, down_sql = split_migration_sections(migration_sql)

    assert "SELECT '-- migrate:down' AS marker;" in up_sql
    assert "/* -- migrate:down */" in up_sql
    assert "DROP TABLE events;" in down_sql


def test_has_executable_sql_handles_postgres_dollar_quoted_body() -> None:
    sql = (
        "CREATE FUNCTION f() RETURNS void AS $$\n"
        "BEGIN\n"
        "  PERFORM 1;\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;\n"
    )

    assert has_executable_sql(sql, engine="postgres") is True


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT '/* not comment */';",
        "SELECT '-- not comment';",
        "CREATE TABLE t (v TEXT DEFAULT 'a;b');",
    ],
)
def test_has_executable_sql_handles_comment_like_tokens_in_literals(sql: str) -> None:
    assert has_executable_sql(sql, engine="postgres") is True


@pytest.mark.parametrize(
    ("engine", "sql", "reason"),
    [
        ("mysql", "REPLACE INTO other_db.events(id) VALUES (1);", "unsupported mutating syntax"),
        ("mysql", "RENAME TABLE other_db.a TO other_db.b;", "unsupported mutating syntax"),
        (
            "mysql",
            "CREATE INDEX idx ON other_db.events(id);",
            "qualified write target",
        ),
        (
            "bigquery",
            "CREATE TABLE analytics.events (id INT64);",
            "qualified write target",
        ),
    ],
)
def test_guarded_write_corpus(engine: str, sql: str, reason: str) -> None:
    violations = SqlProgram(sql, engine=engine).section_write_violations("up")

    assert len(violations) == 1
    assert violations[0].reason == reason


def test_anchor_statements_postgres_validate_function_body() -> None:
    sql = (
        "CREATE FUNCTION f() RETURNS void AS $$\n"
        "BEGIN\n"
        "  PERFORM 1;\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;\n"
        "CREATE TABLE widgets (id bigint);\n"
    )

    statements = SqlProgram(sql, engine="postgres").anchor_statements(
        target_url="postgresql://u:p@host:5432/app_db?sslmode=disable"
    )

    assert len(statements) == 2
    assert statements[0].startswith("CREATE FUNCTION f()")
    assert statements[1] == "CREATE TABLE widgets (id bigint)"


def test_anchor_statements_bigquery_retargets_target_writes_and_keeps_foreign_reads() -> None:
    sql = (
        "CREATE TABLE `example-project.old_ds.events` AS "
        "SELECT * FROM `example-project.analytics.users`;"
    )

    statements = SqlProgram(sql, engine="bigquery").anchor_statements(
        target_url="bigquery://example-project/us/new_ds"
    )

    assert statements == (
        "CREATE TABLE `example-project.new_ds.events` AS SELECT * FROM `example-project.analytics.users`",
    )


@pytest.mark.parametrize(
    "sql",
    [
        "CREATE TABLE broken (",
        "CREATE VIEW x AS SELECT FROM ;",
    ],
)
def test_schema_fingerprint_parse_failures_raise(sql: str) -> None:
    with pytest.raises(SqlError):
        SqlProgram(sql, engine="postgres").schema_fingerprint(
            context_url="postgresql://u:p@host:5432/app_db?sslmode=disable"
        )
