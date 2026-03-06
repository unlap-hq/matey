from __future__ import annotations

import pytest

from matey.sql import (
    has_executable_sql,
    migration_sections,
    normalize_sql_for_compare,
    qualified_write_targets,
    split_sql_statements,
)


def test_split_sql_statements_respects_quoted_semicolons() -> None:
    sql = "CREATE TABLE t (v TEXT DEFAULT 'a;b');\nINSERT INTO t VALUES ('x;y');\n"
    statements = split_sql_statements(sql)
    assert statements == (
        "CREATE TABLE t (v TEXT DEFAULT 'a;b')",
        "INSERT INTO t VALUES ('x;y')",
    )


def test_normalize_sql_for_compare_postgres_strips_set_and_db_qualifier() -> None:
    dump = """
SET statement_timeout = 0;
SET transaction_timeout = 0;
CREATE TABLE app_db.widgets (id bigint);
"""
    expected = "CREATE TABLE widgets (id bigint);"
    normalized_dump = normalize_sql_for_compare(
        dump,
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    )
    normalized_expected = normalize_sql_for_compare(
        expected,
        context_url="postgresql://u:p@host:5432/app_db?sslmode=disable",
    )
    assert normalized_dump == normalized_expected


def test_normalize_sql_for_compare_mysql_strips_dump_noise() -> None:
    dump = """
/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
CREATE TABLE `db_name`.`items` (
  `id` bigint NOT NULL
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
LOCK TABLES `items` WRITE;
UNLOCK TABLES;
"""
    expected = "CREATE TABLE items (id bigint NOT NULL);"
    normalized_dump = normalize_sql_for_compare(
        dump,
        context_url="mysql://u:p@127.0.0.1:3306/db_name",
    )
    normalized_expected = normalize_sql_for_compare(
        expected,
        context_url="mysql://u:p@127.0.0.1:3306/db_name",
    )
    assert normalized_dump == normalized_expected


def test_normalize_sql_for_compare_clickhouse_strips_settings_and_qualifier() -> None:
    dump = """
SET allow_experimental_analyzer=1;
CREATE TABLE `analytics`.`events` (`id` Int64) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 8192;
"""
    expected = "CREATE TABLE events (id Int64) ENGINE = MergeTree ORDER BY tuple();"
    normalized_dump = normalize_sql_for_compare(
        dump,
        context_url="clickhouse://u:p@127.0.0.1:9000/analytics",
    )
    normalized_expected = normalize_sql_for_compare(
        expected,
        context_url="clickhouse://u:p@127.0.0.1:9000/analytics",
    )
    assert normalized_dump == normalized_expected


def test_normalize_sql_for_compare_bigquery_normalizes_only_current_target_dataset() -> None:
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
    normalized_dump = normalize_sql_for_compare(
        dump,
        context_url="bigquery://example-project/us/matey_ds",
    )
    normalized_expected = normalize_sql_for_compare(
        expected,
        context_url="bigquery://example-project/us/other_ds",
    )
    assert normalized_dump == normalized_expected


def test_normalize_sql_for_compare_bigquery_preserves_foreign_dataset_references() -> None:
    left = """
CREATE VIEW `example-project.matey_ds.events_view` AS
SELECT * FROM `example-project.analytics.users`;
"""
    right = """
CREATE VIEW `example-project.matey_ds.events_view` AS
SELECT * FROM `example-project.staging.users`;
"""

    normalized_left = normalize_sql_for_compare(
        left,
        context_url="bigquery://example-project/us/matey_ds",
    )
    normalized_right = normalize_sql_for_compare(
        right,
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

    up_sql, down_sql = migration_sections(migration_sql)

    assert "CREATE TABLE events" in up_sql
    assert "DROP TABLE events" in down_sql


def test_has_executable_sql_ignores_comments_only() -> None:
    assert has_executable_sql("\n-- comment\n/* block */\n;\n") is False
    assert has_executable_sql("\n-- comment\nDROP TABLE events;\n") is True


def test_qualified_write_targets_allow_unqualified_bigquery_write() -> None:
    violations = qualified_write_targets(
        "CREATE TABLE events (id INT64);",
        engine="bigquery",
    )

    assert violations == ()


def test_qualified_write_targets_allow_foreign_bigquery_read() -> None:
    violations = qualified_write_targets(
        "CREATE VIEW events AS SELECT * FROM other_ds.users;",
        engine="bigquery",
    )

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
    violations = qualified_write_targets(sql, engine=engine)

    assert len(violations) == 1
    assert violations[0].target == expected_target
