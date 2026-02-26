from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from matey.domain import ResolvedPaths
from matey.workflows.schema import validate_schema_clean_target


def _bigquery_test_url() -> str | None:
    return os.getenv("MATEY_BIGQUERY_TEST_URL") or os.getenv("MATEY_TEST_URL")


def _require_bigquery_test_url() -> str:
    test_url = _bigquery_test_url()
    if not test_url:
        pytest.skip(
            "Set MATEY_BIGQUERY_TEST_URL=bigquery://<project>/<location> "
            "to run BigQuery integration tests."
        )
    if not test_url.startswith("bigquery://"):
        pytest.skip(
            "BigQuery integration test requires a BigQuery scratch URL "
            "(bigquery://...) in MATEY_BIGQUERY_TEST_URL or MATEY_TEST_URL."
        )
    return test_url


def _run_git(repo_root: Path, args: list[str]) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (result.stderr or result.stdout or "").strip()
    return result


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_migration_file(path: Path, *, up_sql: str, down_sql: str) -> None:
    _write(
        path,
        f"-- migrate:up\n{up_sql}\n\n-- migrate:down\n{down_sql}\n",
    )


def _write_clean_check_migration(root: Path) -> ResolvedPaths:
    db_root = root / "db"
    migrations_dir = db_root / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    migration_text = (
        "-- migrate:up\n"
        "CREATE TABLE widgets (id INT64, name STRING);\n\n"
        "-- migrate:down\n"
        "DROP TABLE widgets;\n"
    )
    (migrations_dir / "202602260001_create_widgets_bigquery.sql").write_text(
        migration_text,
        encoding="utf-8",
    )
    schema_file = db_root / "schema.sql"
    schema_file.write_text("-- placeholder\n", encoding="utf-8")
    return ResolvedPaths(
        db_dir=db_root,
        migrations_dir=migrations_dir,
        schema_file=schema_file,
    )


def _init_feature_repo(tmp_path: Path) -> tuple[Path, ResolvedPaths]:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    _run_git(repo_root, ["init"])
    _run_git(repo_root, ["config", "user.name", "Matey Tests"])
    _run_git(repo_root, ["config", "user.email", "matey-tests@example.com"])

    db_dir = repo_root / "db"
    _write(db_dir / "schema.sql", "-- placeholder\n")
    _write_migration_file(
        db_dir / "migrations" / "202602260101_create_widgets.sql",
        up_sql="CREATE TABLE widgets (id INT64, payload STRING NOT NULL);",
        down_sql="DROP TABLE widgets;",
    )
    _run_git(repo_root, ["add", "."])
    _run_git(repo_root, ["commit", "-m", "base migration"])
    _run_git(repo_root, ["branch", "-M", "main"])
    _run_git(repo_root, ["checkout", "-b", "feature"])

    paths = ResolvedPaths(
        db_dir=db_dir,
        migrations_dir=db_dir / "migrations",
        schema_file=db_dir / "schema.sql",
    )
    return repo_root, paths


@pytest.mark.integration
def test_schema_validate_clean_no_repo_check_bigquery_live(
    tmp_path: Path,
    dbmate_binary: Path,
) -> None:
    test_url = _require_bigquery_test_url()

    paths = _write_clean_check_migration(tmp_path)
    result = validate_schema_clean_target(
        target_name="bigquery",
        dbmate_binary=dbmate_binary,
        paths=paths,
        real_url=None,
        test_url=test_url,
        keep_scratch=False,
        no_repo_check=True,
        schema_only=True,
    )
    assert result.success, result.error


@pytest.mark.integration
def test_schema_validate_upgrade_path_bigquery_happy_path(
    tmp_path: Path,
    dbmate_binary: Path,
) -> None:
    test_url = _require_bigquery_test_url()
    repo_root, paths = _init_feature_repo(tmp_path)
    _write_migration_file(
        paths.migrations_dir / "202602260102_add_source.sql",
        up_sql="ALTER TABLE widgets ADD COLUMN source STRING;",
        down_sql="ALTER TABLE widgets DROP COLUMN source;",
    )
    _run_git(repo_root, ["add", "."])
    _run_git(repo_root, ["commit", "-m", "feature migration"])

    result = validate_schema_clean_target(
        target_name="bigquery",
        dbmate_binary=dbmate_binary,
        paths=paths,
        real_url=None,
        test_url=test_url,
        keep_scratch=False,
        no_repo_check=True,
        schema_only=False,
        path_only=False,
        no_upgrade_diff=False,
        base_branch="main",
        cwd=repo_root,
    )
    assert result.success, result.error
    assert result.upgrade_diff_text is None


@pytest.mark.integration
def test_schema_validate_upgrade_path_bigquery_detects_rewritten_base_migration(
    tmp_path: Path,
    dbmate_binary: Path,
) -> None:
    test_url = _require_bigquery_test_url()
    repo_root, paths = _init_feature_repo(tmp_path)
    _write_migration_file(
        paths.migrations_dir / "202602260101_create_widgets.sql",
        up_sql="CREATE TABLE widgets (id INT64, payload STRING NOT NULL, source STRING);",
        down_sql="DROP TABLE widgets;",
    )
    _run_git(repo_root, ["add", "."])
    _run_git(repo_root, ["commit", "-m", "rewrite base migration on feature"])

    result = validate_schema_clean_target(
        target_name="bigquery",
        dbmate_binary=dbmate_binary,
        paths=paths,
        real_url=None,
        test_url=test_url,
        keep_scratch=False,
        no_repo_check=True,
        schema_only=False,
        path_only=False,
        no_upgrade_diff=False,
        base_branch="main",
        cwd=repo_root,
    )
    assert result.success is False
    assert result.upgrade_diff_text is not None
