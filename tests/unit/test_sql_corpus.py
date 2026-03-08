from __future__ import annotations

import importlib

import pytest

from matey.sql import (
    MigrationSqlError,
    SqlError,
    SqlProgram,
    first_migration_violation_message,
    has_executable_sql,
    split_migration_sections,
)

sql_source_mod = importlib.import_module("matey.sql.source")


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


def test_split_migration_sections_accepts_dbmate_directive_suffixes() -> None:
    migration_sql = (
        "-- migrate:up transaction:false\n"
        "CREATE TABLE events (id INT64);\n"
        "-- migrate:down transaction:false\n"
        "DROP TABLE events;\n"
    )

    up_sql, down_sql = split_migration_sections(migration_sql)

    assert up_sql == "CREATE TABLE events (id INT64);\n"
    assert down_sql == "DROP TABLE events;\n"


def test_has_executable_sql_handles_postgres_dollar_quoted_body() -> None:
    sql = (
        "CREATE FUNCTION f() RETURNS void AS $$\nBEGIN\n  PERFORM 1;\nEND;\n$$ LANGUAGE plpgsql;\n"
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


def test_anchor_statements_postgres_keep_semicolons_inside_comments_and_bodies() -> None:
    sql = (
        "CREATE FUNCTION f() RETURNS void AS $$\n"
        "BEGIN\n"
        "  PERFORM 1; -- keep ; inside body\n"
        "  /* comment ; inside body */\n"
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


def test_anchor_statements_postgres_keep_semicolons_inside_block_comments() -> None:
    sql = (
        "CREATE TABLE widgets (id bigint) /* trailing ; comment */;\n"
        "CREATE TABLE logs (id bigint);\n"
    )

    statements = SqlProgram(sql, engine="postgres").anchor_statements(
        target_url="postgresql://u:p@host:5432/app_db?sslmode=disable"
    )

    assert statements == (
        "CREATE TABLE widgets (id bigint) /* trailing ; comment */",
        "CREATE TABLE logs (id bigint)",
    )


def test_anchor_statements_postgres_allow_nested_block_comments() -> None:
    sql = "/* outer /* inner */ still; comment */\nSELECT 1;\nSELECT 2;\n"

    statements = SqlProgram(sql, engine="postgres").anchor_statements(
        target_url="postgresql://u:p@host:5432/app_db?sslmode=disable"
    )

    assert statements == (
        "/* outer /* inner */ still; comment */\nSELECT 1",
        "SELECT 2",
    )


def test_anchor_statements_postgres_ignore_comment_only_fragments_between_statements() -> None:
    sql = (
        "CREATE TABLE widgets (id bigint);\n"
        "/* comment only */;\n"
        "-- comment only\n"
        ";\n"
        "CREATE TABLE logs (id bigint);\n"
    )

    statements = SqlProgram(sql, engine="postgres").anchor_statements(
        target_url="postgresql://u:p@host:5432/app_db?sslmode=disable"
    )

    assert statements == (
        "CREATE TABLE widgets (id bigint)",
        "CREATE TABLE logs (id bigint)",
    )


def test_anchor_statements_sqlite_trigger_body_fails_closed() -> None:
    sql = (
        "CREATE TABLE events(id INTEGER);\n"
        "CREATE TRIGGER t AFTER INSERT ON events\n"
        "BEGIN\n"
        "  INSERT INTO events(id) VALUES (NEW.id + 1);\n"
        "  INSERT INTO events(id) VALUES (NEW.id + 2);\n"
        "END;\n"
    )

    with pytest.raises(SqlError, match="sqlite trigger bodies are not supported safely"):
        SqlProgram(sql, engine="sqlite").anchor_statements(target_url="sqlite3:/tmp/test.sqlite3")


def test_anchor_statements_sqlite_comment_mention_of_trigger_is_allowed() -> None:
    sql = (
        "CREATE TABLE events(id INTEGER);\n"
        "-- CREATE TRIGGER is only mentioned here\n"
        "CREATE TABLE logs(id INTEGER);\n"
    )

    statements = SqlProgram(sql, engine="sqlite").anchor_statements(
        target_url="sqlite3:/tmp/test.sqlite3"
    )

    assert statements == (
        "CREATE TABLE events(id INTEGER)",
        "-- CREATE TRIGGER is only mentioned here\nCREATE TABLE logs(id INTEGER)",
    )


def test_source_anchor_statements_alignment_mismatch_raises() -> None:
    with pytest.raises(sql_source_mod.SqlTextDecodeError, match="could not be aligned safely"):
        sql_source_mod.aligned_source_statements(
            "CREATE TABLE a(id INTEGER);",
            expected_count=2,
            label="sqlite",
        )


def test_source_anchor_statements_handles_plain_string_backslashes_before_closing_quote() -> None:
    statements = sql_source_mod.aligned_source_statements(
        "SELECT 'a\\\\';\nSELECT 1;",
        expected_count=2,
        label="postgres",
    )

    assert statements == ("SELECT 'a\\\\'", "SELECT 1")


def test_anchor_statements_postgres_identifier_with_dollar_tag_fragment() -> None:
    sql = "CREATE TABLE foo$tag$bar (id bigint);\nSELECT 1;\n"

    statements = SqlProgram(sql, engine="postgres").anchor_statements(
        target_url="postgresql://u:p@host:5432/app_db?sslmode=disable"
    )

    assert statements == (
        "CREATE TABLE foo$tag$bar (id bigint)",
        "SELECT 1",
    )


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


def test_first_migration_violation_message_stops_at_first_violation() -> None:
    message = first_migration_violation_message(
        entries=(
            ("migrations/001_bad.sql", b"CREATE TABLE other_db.events (id BIGINT);"),
            ("migrations/002_later.sql", b"\xff\xfe\x00"),
        ),
        engine="mysql",
        section="up",
    )

    assert message is not None
    assert "migrations/001_bad.sql" in message
    assert "qualified mysql write target" in message


def test_first_migration_violation_message_attributes_decode_failure() -> None:
    with pytest.raises(
        MigrationSqlError, match=r"Unable to decode migration migrations/002_bad\.sql as UTF-8"
    ):
        first_migration_violation_message(
            entries=(
                ("migrations/001_ok.sql", b"CREATE TABLE events (id BIGINT);"),
                ("migrations/002_bad.sql", b"\xff\xfe\x00"),
            ),
            engine="mysql",
            section="up",
        )
