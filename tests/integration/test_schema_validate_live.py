from __future__ import annotations

from pathlib import Path

import pytest

from matey.domain import ResolvedPaths
from matey.workflows.schema import validate_schema_clean_target


def _write_migration(
    root: Path,
    *,
    filename: str,
    up_sql: str,
    down_sql: str,
) -> ResolvedPaths:
    db_root = root / "db"
    migrations_dir = db_root / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    migration_text = (
        "-- migrate:up\n"
        f"{up_sql}\n\n"
        "-- migrate:down\n"
        f"{down_sql}\n"
    )
    (migrations_dir / filename).write_text(migration_text, encoding="utf-8")
    schema_file = db_root / "schema.sql"
    schema_file.write_text("-- placeholder\n", encoding="utf-8")
    return ResolvedPaths(
        db_dir=db_root,
        migrations_dir=migrations_dir,
        schema_file=schema_file,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    ("engine", "real_url", "up_sql", "down_sql"),
    [
        (
            "sqlite",
            "sqlite3:/tmp/matey-base.sqlite3",
            "CREATE TABLE widgets (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL);",
            "DROP TABLE widgets;",
        ),
        (
            "postgres",
            "postgres://matey:matey@127.0.0.1:5432/postgres?sslmode=disable",
            "CREATE TABLE widgets (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL);",
            "DROP TABLE widgets;",
        ),
        (
            "mysql",
            "mysql://root:matey@127.0.0.1:3306/mysql",
            "CREATE TABLE widgets (id BIGINT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(255) NOT NULL);",
            "DROP TABLE widgets;",
        ),
        (
            "clickhouse",
            "clickhouse://default@127.0.0.1:9000/default",
            "CREATE TABLE widgets (id UInt64, name String) ENGINE = MergeTree ORDER BY id;",
            "DROP TABLE widgets;",
        ),
    ],
)
def test_schema_validate_clean_no_repo_check_live(
    tmp_path: Path,
    dbmate_binary: Path,
    docker_available: bool,
    engine_supported: callable,
    engine: str,
    real_url: str,
    up_sql: str,
    down_sql: str,
) -> None:
    if not engine_supported(engine):
        pytest.fail(f"dbmate binary does not support required integration engine: {engine}")

    if engine in {"postgres", "mysql", "clickhouse"} and not docker_available:
        pytest.fail("Docker is required for postgres/mysql/clickhouse integration tests.")

    paths = _write_migration(
        tmp_path,
        filename=f"202602240001_create_widgets_{engine}.sql",
        up_sql=up_sql,
        down_sql=down_sql,
    )

    result = validate_schema_clean_target(
        target_name=engine,
        dbmate_binary=dbmate_binary,
        paths=paths,
        real_url=real_url,
        test_url=None,
        keep_scratch=False,
        no_repo_check=True,
        schema_only=True,
    )
    assert result.success, result.error
