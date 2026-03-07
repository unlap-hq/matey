from __future__ import annotations

import pytest

from matey.sql import (
    SqlError,
    SqlProgram,
    has_executable_sql,
    split_migration_sections,
)


def test_schema_fingerprint_postgres_strips_set_noise_only() -> None:
    dump = """
SET statement_timeout = 0;
SET transaction_timeout = 0;
CREATE TABLE widgets (id bigint);
"""
    expected = "CREATE TABLE widgets (id bigint);"
    normalized_dump = SqlProgram(dump, engine="postgres").schema_fingerprint(
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    )
    normalized_expected = SqlProgram(expected, engine="postgres").schema_fingerprint(
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    )
    assert normalized_dump == normalized_expected


def test_schema_fingerprint_postgres_preserves_schema_qualifier() -> None:
    public_sql = "CREATE TABLE public.widgets (id bigint);"
    audit_sql = "CREATE TABLE audit.widgets (id bigint);"

    public_fingerprint = SqlProgram(public_sql, engine="postgres").schema_fingerprint(
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    )
    audit_fingerprint = SqlProgram(audit_sql, engine="postgres").schema_fingerprint(
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    )

    assert public_fingerprint != audit_fingerprint


def test_schema_fingerprint_preserves_literal_whitespace() -> None:
    left = SqlProgram(
        "CREATE VIEW widgets_v AS SELECT 'a  b' AS label;",
        engine="postgres",
    )
    right = SqlProgram(
        "CREATE VIEW widgets_v AS SELECT 'a b' AS label;",
        engine="postgres",
    )

    assert left.schema_fingerprint(
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    ) != right.schema_fingerprint(
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    )


def test_schema_fingerprint_mysql_strips_dump_noise() -> None:
    dump = """
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
CREATE TABLE `db_name`.`items` (
  `id` bigint NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
LOCK TABLES `items` WRITE;
UNLOCK TABLES;
"""
    expected = "CREATE TABLE items (id bigint NOT NULL);"
    normalized_dump = SqlProgram(dump, engine="mysql").schema_fingerprint(
        context_url="mysql://u:p@127.0.0.1:3306/db_name",
    )
    normalized_expected = SqlProgram(expected, engine="mysql").schema_fingerprint(
        context_url="mysql://u:p@127.0.0.1:3306/db_name",
    )
    assert normalized_dump == normalized_expected


def test_schema_fingerprint_mysql_normalizes_innodb_case() -> None:
    left = SqlProgram(
        "CREATE TABLE items (id bigint NOT NULL) ENGINE=InnoDB;",
        engine="mysql",
    )
    right = SqlProgram(
        "CREATE TABLE items (id bigint NOT NULL) ENGINE=innodb;",
        engine="mysql",
    )

    assert left.schema_fingerprint(context_url="mysql://u:p@127.0.0.1:3306/db_name") == right.schema_fingerprint(
        context_url="mysql://u:p@127.0.0.1:3306/db_name",
    )


def test_schema_fingerprint_clickhouse_strips_settings_and_qualifier() -> None:
    dump = """
SET allow_experimental_analyzer=1;
CREATE TABLE `analytics`.`events` (`id` Int64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
"""
    expected = "CREATE TABLE events (id Int64) ENGINE = MergeTree ORDER BY tuple();"
    normalized_dump = SqlProgram(dump, engine="clickhouse").schema_fingerprint(
        context_url="clickhouse://u:p@127.0.0.1:9000/analytics",
    )
    normalized_expected = SqlProgram(expected, engine="clickhouse").schema_fingerprint(
        context_url="clickhouse://u:p@127.0.0.1:9000/analytics",
    )
    assert normalized_dump == normalized_expected


def test_schema_fingerprint_bigquery_normalizes_only_current_target_dataset() -> None:
    dump = """
CREATE DATABASE IF NOT EXISTS `example-project.matey_ds` OPTIONS(location="US");
CREATE TABLE `example-project.matey_ds.events` (id INT64);
INSERT INTO `example-project.matey_ds.schema_migrations` (version) VALUES ('001');
"""
    expected = """
CREATE SCHEMA IF NOT EXISTS `example-project.other_ds`;
CREATE TABLE `example-project.other_ds.events` (id INT64);
INSERT INTO `example-project.other_ds.schema_migrations` (version) VALUES ('001');
"""
    normalized_dump = SqlProgram(dump, engine="bigquery").schema_fingerprint(
        context_url="bigquery://example-project/us/matey_ds",
    )
    normalized_expected = SqlProgram(expected, engine="bigquery").schema_fingerprint(
        context_url="bigquery://example-project/us/other_ds",
    )
    assert normalized_dump == normalized_expected


def test_schema_fingerprint_bigquery_preserves_foreign_dataset_references() -> None:
    left = """
CREATE VIEW `example-project.matey_ds.events_view` AS
SELECT * FROM `example-project.analytics.users`;
"""
    right = """
CREATE VIEW `example-project.matey_ds.events_view` AS
SELECT * FROM `example-project.staging.users`;
"""

    normalized_left = SqlProgram(left, engine="bigquery").schema_fingerprint(
        context_url="bigquery://example-project/us/matey_ds",
    )
    normalized_right = SqlProgram(right, engine="bigquery").schema_fingerprint(
        context_url="bigquery://example-project/us/matey_ds",
    )

    assert normalized_left != normalized_right


def test_migration_sections_split_up_and_down() -> None:
    migration_sql = (
        "-- migrate:up\n"
        "CREATE TABLE events (id INT64);\n"
        "\n"
        "-- migrate:down\n"
        "DROP TABLE events;\n"
    )

    up_sql, down_sql = split_migration_sections(migration_sql)

    assert "CREATE TABLE events" in up_sql
    assert "DROP TABLE events" in down_sql


def test_has_executable_sql_ignores_comments_only() -> None:
    assert has_executable_sql("\n-- comment\n/* block */\n;\n", engine="postgres") is False
    assert has_executable_sql("\n-- comment\nDROP TABLE events;\n", engine="postgres") is True


def test_schema_fingerprint_raises_on_unparseable_sql() -> None:
    with pytest.raises(SqlError):
        SqlProgram("CREATE TABLE broken (", engine="postgres").schema_fingerprint(
            context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
        )


def test_qualified_write_targets_allow_unqualified_bigquery_write() -> None:
    violations = SqlProgram(
        "CREATE TABLE events (id INT64);",
        engine="bigquery",
    ).section_write_violations("up")

    assert violations == ()


def test_qualified_write_targets_allow_foreign_bigquery_read() -> None:
    violations = SqlProgram(
        "CREATE VIEW events AS SELECT * FROM other_ds.users;",
        engine="bigquery",
    ).section_write_violations("up")

    assert violations == ()


@pytest.mark.parametrize(
    ("engine", "sql", "expected_target"),
    [
        ("bigquery", "CREATE TABLE analytics.events (id INT64);", "analytics.events"),
        ("bigquery", "CREATE TABLE project.analytics.events (id INT64);", "project.analytics.events"),
        ("bigquery", "INSERT INTO `project:analytics.events` (id) VALUES (1);", "`project:analytics.events`"),
        ("mysql", "CREATE TABLE other_db.events (id BIGINT);", "other_db.events"),
        ("clickhouse", "CREATE TABLE other_db.events (id Int64) ENGINE = MergeTree ORDER BY tuple();", "other_db.events"),
    ],
)
def test_qualified_write_targets_reject_qualified_writes(
    engine: str,
    sql: str,
    expected_target: str,
) -> None:
    violations = SqlProgram(sql, engine=engine).section_write_violations("up")

    assert len(violations) == 1
    assert violations[0].target == expected_target
