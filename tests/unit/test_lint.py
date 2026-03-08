from __future__ import annotations

from importlib import import_module
from pathlib import Path

import pygit2
import pytest

from matey.lint.semantic import lint_target
from matey.lint.sqlfluff import lint_paths
from matey.project import TargetConfig

cli = import_module("matey.cli.app")


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pygit2.init_repository(str(path), initial_head="main")


def _target(tmp_path: Path) -> TargetConfig:
    return TargetConfig(
        name="core",
        root=(tmp_path / "db" / "core").resolve(),
        engine="postgres",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
    )


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_semantic_lint_reports_duplicate_versions_and_missing_down(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "CREATE TABLE a(id INT);\n")
    _write(target.migrations / "001_other.sql", "CREATE TABLE b(id INT);\n")

    result = lint_target(target, engine="postgres")
    codes = {finding.code for finding in result.findings}

    assert "L002" in codes
    assert "L101" in codes
    assert "L103" in codes


def test_semantic_lint_reports_qualified_write(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        "CREATE TABLE other_db.events (id BIGINT);\n",
    )

    result = lint_target(target, engine="mysql")

    assert any(f.code == "L104" for f in result.findings)
    assert any(
        "cross-database writes are not allowed" in f.message
        for f in result.findings
        if f.code == "L104"
    )


def test_semantic_lint_reports_transaction_false_warning(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        "-- migrate:up transaction:false\nCREATE TABLE a(id INT);\n",
    )

    result = lint_target(target, engine="postgres")

    assert any(f.code == "L106" for f in result.findings)


def test_semantic_lint_reports_artifact_state_findings_without_lock(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(target.schema, "CREATE TABLE a(id INT);\n")
    _write(target.checkpoints / "001_init.sql", "CREATE TABLE a(id INT);\n")

    result = lint_target(target, engine=None)
    codes = {finding.code for finding in result.findings}

    assert "L205" in codes
    assert "L206" in codes


def test_semantic_lint_reports_lock_engine_mismatch(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.lockfile,
        """
lock_version = 0
hash_algorithm = "blake2b-256"
canonicalizer = "matey-sql-v0"
engine = "postgres"
target = "core"
schema_file = "schema.sql"
migrations_dir = "migrations"
checkpoints_dir = "checkpoints"
head_index = 0
head_chain_hash = "x"
head_schema_digest = "y"
steps = []
""".strip()
        + "\n",
    )

    result = lint_target(target, engine="mysql")

    assert any(f.code == "L209" for f in result.findings)


def test_semantic_lint_reports_bigquery_emulator_unsupported_features(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        "CREATE MATERIALIZED VIEW ds.v AS SELECT 1;\n"
        "CREATE TABLE ds.t (id INT64 DEFAULT (1), ts TIMESTAMP) PARTITION BY DATE(ts);\n",
    )

    result = lint_target(target, engine="bigquery-emulator")

    l301 = [f.message for f in result.findings if f.code == "L301"]
    assert any("materialized views" in msg for msg in l301)
    assert any("partitioning, clustering, or column defaults" in msg for msg in l301)


def test_semantic_lint_reports_lock_diagnostics(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(target.schema, "CREATE TABLE a(id INT);\n")
    _write(target.lockfile, "not toml\n")

    result = lint_target(target, engine="postgres")

    assert any(f.code == "L201" for f in result.findings)


def test_sqlfluff_lint_reports_style_violation(tmp_path: Path) -> None:
    target = _target(tmp_path)
    path = target.migrations / "001_init.sql"
    _write(path, "select  1  from foo\n")

    findings = lint_paths(
        target_name=target.name,
        paths=(path,),
        target_root=target.root,
        engine="postgres",
    )

    assert any(f.code.startswith("SF.") for f in findings)


def test_sqlfluff_lint_reports_parse_violation(tmp_path: Path) -> None:
    target = _target(tmp_path)
    path = target.migrations / "001_init.sql"
    _write(path, "SELECT FROM\n")

    findings = lint_paths(
        target_name=target.name,
        paths=(path,),
        target_root=target.root,
        engine="postgres",
    )

    assert any(f.code == "SF.PRS" for f in findings)


def test_cli_lint_json_and_exit_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    _init_repo(tmp_path)
    _write(
        tmp_path / "matey.toml",
        'targets = ["db/core"]\n',
    )
    _write(
        tmp_path / "db" / "core" / "config.toml",
        'engine = "postgres"\nurl_env = "DATABASE_URL"\ntest_url_env = "TEST_DATABASE_URL"\n',
    )
    _write(tmp_path / "db" / "core" / "migrations" / "001_init.sql", "select  1  from foo\n")
    monkeypatch.chdir(tmp_path)

    rc = cli.main(["lint", "--format", "json"])
    out = capsys.readouterr().out

    assert rc == 1
    assert '"code": "L101"' in out
    assert '"code": "SF.LT01"' in out
