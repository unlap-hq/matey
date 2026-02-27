from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from matey.cli import app
from matey.workflows.schema_lock import SchemaDownResult, SchemaReplayResult

runner = CliRunner()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_schema_validate_runs_and_reports_success(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=True,
            head_schema_sql="CREATE TABLE t (id INT);\n",
            expected_schema_sql="CREATE TABLE t (id INT);\n",
            diff_text=None,
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )

    result = runner.invoke(app, ["schema", "validate"], env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"})
    assert result.exit_code == 0
    assert "schema validation passed" in result.output


def test_schema_validate_fails_when_target_result_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=False,
            head_schema_sql="CREATE TABLE t (id INT);\n",
            expected_schema_sql="CREATE TABLE broken (id INT);\n",
            diff_text="--- expected\n+++ actual\n",
            scratch_url="postgres://u:p@localhost/scratch",
            error="boom",
        ),
    )

    result = runner.invoke(app, ["schema", "validate"], env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"})
    assert result.exit_code == 1
    assert "boom" in result.output


def test_schema_validate_down_forwards_and_passes(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=True,
            head_schema_sql="CREATE TABLE t (id INT);\n",
            expected_schema_sql="CREATE TABLE t (id INT);\n",
            diff_text=None,
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )
    seen: dict[str, object] = {}

    def _fake_down(**kwargs):
        seen.update(kwargs)
        return SchemaDownResult(
            target_name="default",
            success=True,
            scratch_url="postgres://u:p@localhost/scratch-down",
        )

    monkeypatch.setattr("matey.cli.commands.schema.validate_schema_lock_down_target", _fake_down)

    result = runner.invoke(
        app,
        ["schema", "validate", "--down"],
        env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"},
    )
    assert result.exit_code == 0
    assert seen["target_name"] == "default"


def test_schema_validate_down_failure_exits_nonzero(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=True,
            head_schema_sql="CREATE TABLE t (id INT);\n",
            expected_schema_sql="CREATE TABLE t (id INT);\n",
            diff_text=None,
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )
    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_lock_down_target",
        lambda **_: SchemaDownResult(
            target_name="default",
            success=False,
            scratch_url="postgres://u:p@localhost/scratch-down",
            error="down failed",
        ),
    )

    result = runner.invoke(
        app,
        ["schema", "validate", "--down"],
        env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"},
    )
    assert result.exit_code == 1
    assert "down failed" in result.output
