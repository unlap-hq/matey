from __future__ import annotations

import os
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = PROJECT_ROOT / "src"


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


def _write_migration(path: Path, *, up_sql: str, down_sql: str) -> None:
    _write(
        path,
        f"-- migrate:up\n{up_sql}\n\n-- migrate:down\n{down_sql}\n",
    )


def _init_feature_repo(tmp_path: Path) -> Path:
    repo_root = tmp_path / "repo"
    repo_root.mkdir(parents=True, exist_ok=True)
    _run_git(repo_root, ["init"])
    _run_git(repo_root, ["config", "user.name", "Matey Tests"])
    _run_git(repo_root, ["config", "user.email", "matey-tests@example.com"])

    db_dir = repo_root / "db"
    _write(db_dir / "schema.sql", "-- placeholder\n")
    _write_migration(
        db_dir / "migrations" / "202602240001_create_widgets.sql",
        up_sql="CREATE TABLE widgets (id INTEGER PRIMARY KEY, payload TEXT NOT NULL);",
        down_sql="DROP TABLE widgets;",
    )
    _run_git(repo_root, ["add", "."])
    _run_git(repo_root, ["commit", "-m", "base migration"])
    _run_git(repo_root, ["branch", "-M", "main"])
    _run_git(repo_root, ["checkout", "-b", "feature"])
    return repo_root


def _run_matey_validate(
    repo_root: Path,
    dbmate_binary: Path,
    *,
    extra_args: list[str] | None = None,
) -> subprocess.CompletedProcess[str]:
    sqlite_url = f"sqlite3:{(repo_root / 'runtime.sqlite3').as_posix()}"
    env = os.environ.copy()
    env["MATEY_URL"] = sqlite_url
    env["MATEY_DBMATE_BIN"] = str(dbmate_binary)
    pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{SRC_ROOT}{os.pathsep}{pythonpath}" if pythonpath else str(SRC_ROOT)
    command = [
        sys.executable,
        "-m",
        "matey",
        "--base",
        "main",
        "schema",
        "validate",
        "--no-repo-check",
        *(extra_args or []),
    ]
    return subprocess.run(
        command,
        cwd=repo_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.integration
def test_schema_validate_upgrade_path_cli_happy_path(dbmate_binary: Path) -> None:
    repo_root = _init_feature_repo(Path(tempfile.mkdtemp(prefix="matey-upgrade-ok-")))
    _write_migration(
        repo_root / "db" / "migrations" / "202602240002_add_source.sql",
        up_sql="ALTER TABLE widgets ADD COLUMN source TEXT DEFAULT 'api';",
        down_sql="ALTER TABLE widgets DROP COLUMN source;",
    )
    _run_git(repo_root, ["add", "."])
    _run_git(repo_root, ["commit", "-m", "feature migration"])

    result = _run_matey_validate(repo_root, dbmate_binary)
    assert result.returncode == 0, (result.stderr or result.stdout or "").strip()
    assert "schema validation passed" in result.stdout


@pytest.mark.integration
def test_schema_validate_upgrade_path_cli_detects_rewritten_base_migration(dbmate_binary: Path) -> None:
    repo_root = _init_feature_repo(Path(tempfile.mkdtemp(prefix="matey-upgrade-diff-")))

    _write_migration(
        repo_root / "db" / "migrations" / "202602240001_create_widgets.sql",
        up_sql=(
            "CREATE TABLE widgets ("
            "id INTEGER PRIMARY KEY, payload TEXT NOT NULL, source TEXT DEFAULT 'api' NOT NULL);"
        ),
        down_sql="DROP TABLE widgets;",
    )
    _run_git(repo_root, ["add", "."])
    _run_git(repo_root, ["commit", "-m", "rewrite base migration on feature"])

    result = _run_matey_validate(repo_root, dbmate_binary)
    assert result.returncode == 1
    assert "clean vs upgrade (upgrade differs from fresh install)" in result.stdout
