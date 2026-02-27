from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from matey.cli import app
from matey.workflows.db_live import DbMutationResult, LiveDiffResult

runner = CliRunner()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_db_up_uses_zero_config_defaults(tmp_path: Path, monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))

    def _fake_guarded(**kwargs: Any) -> DbMutationResult:
        calls.append(kwargs)
        return DbMutationResult(target_name="default", success=True)

    monkeypatch.setattr("matey.cli.commands.db.guarded_mutate_live_db", _fake_guarded)

    result = runner.invoke(app, ["db", "up"], env={"MATEY_URL": "postgres://localhost/app"})

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["verb"] == "up"
    assert calls[0]["live_url"] == "postgres://localhost/app"
    assert calls[0]["paths"].migrations_dir == tmp_path / "db" / "migrations"
    assert calls[0]["paths"].schema_file == tmp_path / "db" / "schema.sql"


def test_db_diff_uses_live_diff_workflow(tmp_path: Path, monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))

    def _fake_diff(**kwargs: Any) -> LiveDiffResult:
        calls.append(kwargs)
        return LiveDiffResult(
            target_name="default",
            success=True,
            diff_text=None,
            expected_schema_sql="",
            live_schema_sql="",
            scratch_url="scratch://x",
        )

    monkeypatch.setattr("matey.cli.commands.db.run_live_db_diff", _fake_diff)

    result = runner.invoke(app, ["db", "diff"], env={"MATEY_URL": "postgres://localhost/app"})

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["live_url"] == "postgres://localhost/app"


def test_multi_target_requires_target_or_all(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(
        tmp_path / "matey.toml",
        """
[targets.core]
url_env = "CORE_URL"

[targets.analytics]
url_env = "ANALYTICS_URL"
""".strip(),
    )

    result = runner.invoke(app, ["db", "status"])

    assert result.exit_code == 2
    assert "Multiple targets configured" in result.output


def test_target_scoped_command_uses_target_paths(tmp_path: Path, monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr("matey.cli.common.run_dbmate", lambda **kwargs: calls.append(kwargs) or 0)
    _write(
        tmp_path / "matey.toml",
        """
[defaults]
dir = "dbroot"

[targets.core]
url_env = "CORE_URL"
""".strip(),
    )

    result = runner.invoke(app, ["--target", "core", "db", "status"], env={"CORE_URL": "postgres://core/db"})

    assert result.exit_code == 0
    assert len(calls) == 1
    assert calls[0]["verb"] == "status"
    assert calls[0]["migrations_dir"] == tmp_path / "dbroot" / "core" / "migrations"
    assert calls[0]["schema_file"] == tmp_path / "dbroot" / "core" / "schema.sql"


def test_url_override_requires_single_target_selection(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(
        tmp_path / "matey.toml",
        """
[targets.core]
url_env = "CORE_URL"

[targets.analytics]
url_env = "ANALYTICS_URL"
""".strip(),
    )

    result = runner.invoke(app, ["--all", "--url", "postgres://override/db", "db", "up"])
    assert result.exit_code == 2
    assert "--url is only allowed when a single target is selected." in result.output


def test_all_runs_all_targets_and_aggregates_failures(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(
        tmp_path / "matey.toml",
        """
[targets.core]
url_env = "CORE_URL"

[targets.analytics]
url_env = "ANALYTICS_URL"
""".strip(),
    )
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    calls: list[dict[str, Any]] = []

    def _fake_run_dbmate(**kwargs: Any) -> int:
        calls.append(kwargs)
        return 1 if kwargs["url"] == "postgres://analytics/db" else 0

    monkeypatch.setattr("matey.cli.common.run_dbmate", _fake_run_dbmate)

    result = runner.invoke(
        app,
        ["--all", "db", "wait"],
        env={
            "CORE_URL": "postgres://core/db",
            "ANALYTICS_URL": "postgres://analytics/db",
        },
    )
    assert result.exit_code == 1
    assert len(calls) == 2


def test_down_passes_step_count(tmp_path: Path, monkeypatch) -> None:
    calls: list[dict[str, Any]] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))

    def _fake_guarded(**kwargs: Any) -> DbMutationResult:
        calls.append(kwargs)
        return DbMutationResult(target_name="default", success=True)

    monkeypatch.setattr("matey.cli.commands.db.guarded_mutate_live_db", _fake_guarded)

    result = runner.invoke(app, ["db", "down", "3"], env={"MATEY_URL": "postgres://localhost/app"})
    assert result.exit_code == 0
    assert calls[0]["verb"] == "rollback"
    assert calls[0]["down_steps"] == 3


def test_db_rollback_alias_is_removed(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["db", "rollback", "1"], env={"MATEY_URL": "postgres://localhost/app"})
    assert result.exit_code != 0
    assert "No such command 'rollback'" in result.output
