from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pygit2
import pytest

cli = import_module("matey.cli.app")

pytestmark = pytest.mark.integration


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pygit2.init_repository(str(path), initial_head="main")


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_lint_reports_semantic_and_style_issues(tmp_path: Path, monkeypatch, capsys) -> None:
    _init_repo(tmp_path)
    _write(
        tmp_path / "matey.toml",
        'targets = ["db/core"]\n',
    )
    _write(
        tmp_path / "db" / "core" / "config.toml",
        'engine = "mysql"\nurl_env = "DATABASE_URL"\ntest_url_env = "TEST_DATABASE_URL"\n',
    )
    _write(
        tmp_path / "db" / "core" / "migrations" / "001_init.sql",
        "CREATE TABLE other_db.events (id BIGINT);\n",
    )
    monkeypatch.chdir(tmp_path)

    rc = cli.main(["lint"])
    out = capsys.readouterr().out

    assert rc == 1
    assert "L101" in out
    assert "L103" in out
    assert "L104" in out


def test_lint_reports_style_and_lock_issues(tmp_path: Path, monkeypatch, capsys) -> None:
    _init_repo(tmp_path)
    _write(
        tmp_path / "matey.toml",
        'targets = ["db/core"]\n',
    )
    _write(
        tmp_path / "db" / "core" / "config.toml",
        'engine = "postgres"\nurl_env = "DATABASE_URL"\ntest_url_env = "TEST_DATABASE_URL"\n',
    )
    _write(tmp_path / "db" / "core" / "schema.lock.toml", "not toml\n")
    _write(tmp_path / "db" / "core" / "migrations" / "001_init.sql", "select  1  from foo\n")
    monkeypatch.chdir(tmp_path)

    rc = cli.main(["lint"])
    out = capsys.readouterr().out

    assert rc == 1
    assert "L201" in out
    assert "SF.LT01" in out
