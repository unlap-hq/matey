from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from matey.cli import app
from matey.workflows.schema import SchemaValidateResult

runner = CliRunner()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_schema_validate_runs_and_reports_success(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_clean_target",
        lambda **_: SchemaValidateResult(
            target_name="default",
            success=True,
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )

    result = runner.invoke(app, ["schema", "validate"], env={"MATEY_URL": "postgres://u:p@localhost/app"})
    assert result.exit_code == 0
    assert "schema validation passed" in result.output


def test_schema_validate_fails_when_target_result_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_clean_target",
        lambda **_: SchemaValidateResult(
            target_name="default",
            success=False,
            scratch_url="postgres://u:p@localhost/scratch",
            error="boom",
        ),
    )

    result = runner.invoke(app, ["schema", "validate"], env={"MATEY_URL": "postgres://u:p@localhost/app"})
    assert result.exit_code == 1
    assert "boom" in result.output


def test_schema_validate_path_only_is_supported_and_forwarded(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    seen: dict[str, object] = {}

    def _fake_validate(**kwargs):
        seen.update(kwargs)
        return SchemaValidateResult(
            target_name="default",
            success=True,
            scratch_url="postgres://u:p@localhost/scratch",
        )

    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_clean_target",
        _fake_validate,
    )

    result = runner.invoke(
        app,
        ["schema", "validate", "--path-only"],
        env={"MATEY_URL": "postgres://u:p@localhost/app"},
    )
    assert result.exit_code == 0
    assert seen["path_only"] is True
