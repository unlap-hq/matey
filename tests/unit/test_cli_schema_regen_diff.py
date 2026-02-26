from __future__ import annotations

from pathlib import Path
from typing import Any

from typer.testing import CliRunner

from matey.cli import app
from matey.workflows.schema import SchemaValidateResult

runner = CliRunner()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_schema_regen_writes_clean_schema_even_when_repo_diff(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    schema_file = tmp_path / "db" / "schema.sql"
    _write(schema_file, "CREATE TABLE old_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_clean_target",
        lambda **_: SchemaValidateResult(
            target_name="default",
            success=False,
            scratch_url="postgres://u:p@localhost/scratch",
            diff_text="--- repo\n+++ clean\n",
            clean_schema_sql="CREATE TABLE new_table (id INT);\n",
            upgrade_schema_sql="CREATE TABLE new_table (id INT);\n",
        ),
    )

    result = runner.invoke(app, ["schema", "regen"], env={"MATEY_URL": "postgres://u:p@localhost/app"})

    assert result.exit_code == 0
    assert "repo vs clean (regen would change)" in result.output
    assert schema_file.read_text(encoding="utf-8") == "CREATE TABLE new_table (id INT);\n"


def test_schema_regen_refuses_write_when_upgrade_mismatch_without_force(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    schema_file = tmp_path / "db" / "schema.sql"
    _write(schema_file, "CREATE TABLE old_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_clean_target",
        lambda **_: SchemaValidateResult(
            target_name="default",
            success=False,
            scratch_url="postgres://u:p@localhost/scratch",
            upgrade_diff_text="--- clean\n+++ upgrade\n",
            clean_schema_sql="CREATE TABLE clean_table (id INT);\n",
            upgrade_schema_sql="CREATE TABLE upgrade_table (id INT);\n",
        ),
    )

    result = runner.invoke(app, ["schema", "regen"], env={"MATEY_URL": "postgres://u:p@localhost/app"})

    assert result.exit_code == 1
    assert "refusing to write schema.sql" in result.output
    assert schema_file.read_text(encoding="utf-8") == "CREATE TABLE old_table (id INT);\n"


def test_schema_regen_force_writes_when_upgrade_mismatch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    schema_file = tmp_path / "db" / "schema.sql"
    _write(schema_file, "CREATE TABLE old_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_clean_target",
        lambda **_: SchemaValidateResult(
            target_name="default",
            success=False,
            scratch_url="postgres://u:p@localhost/scratch",
            upgrade_diff_text="--- clean\n+++ upgrade\n",
            clean_schema_sql="CREATE TABLE clean_table (id INT);\n",
            upgrade_schema_sql="CREATE TABLE upgrade_table (id INT);\n",
        ),
    )

    result = runner.invoke(
        app,
        ["schema", "regen", "--force"],
        env={"MATEY_URL": "postgres://u:p@localhost/app"},
    )

    assert result.exit_code == 0
    assert schema_file.read_text(encoding="utf-8") == "CREATE TABLE clean_table (id INT);\n"


def test_schema_diff_scratch_exits_nonzero_when_diff_present(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.validate_schema_clean_target",
        lambda **_: SchemaValidateResult(
            target_name="default",
            success=False,
            scratch_url="postgres://u:p@localhost/scratch",
            diff_text="--- repo\n+++ clean\n",
        ),
    )

    result = runner.invoke(app, ["schema", "diff"], env={"MATEY_URL": "postgres://u:p@localhost/app"})

    assert result.exit_code == 1
    assert "repo vs clean (regen would change)" in result.output


def test_schema_diff_live_repo_expected_uses_live_dump(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE expected_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.dump_schema_for_url",
        lambda **_: "CREATE TABLE live_table (id INT);\n",
    )

    result = runner.invoke(
        app,
        ["--url", "postgres://u:p@localhost/live", "schema", "diff", "--live"],
    )

    assert result.exit_code == 1
    assert "expected vs live (--live)" in result.output


def test_schema_diff_live_clean_expected_runs_clean_scratch(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE expected_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))

    seen: dict[str, Any] = {}

    def _fake_validate(**kwargs: Any) -> SchemaValidateResult:
        seen.update(kwargs)
        return SchemaValidateResult(
            target_name="default",
            success=True,
            scratch_url="postgres://u:p@localhost/scratch",
            clean_schema_sql="CREATE TABLE clean_table (id INT);\n",
        )

    monkeypatch.setattr("matey.cli.commands.schema.validate_schema_clean_target", _fake_validate)
    monkeypatch.setattr(
        "matey.cli.commands.schema.dump_schema_for_url",
        lambda **_: "CREATE TABLE clean_table (id INT);\n",
    )

    result = runner.invoke(
        app,
        ["schema", "diff", "--live", "--expected", "clean"],
        env={"MATEY_URL": "postgres://u:p@localhost/app"},
    )

    assert result.exit_code == 0
    assert seen["schema_only"] is True
    assert seen["no_repo_check"] is True


def test_schema_diff_rejects_path_only_in_live_mode(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE expected_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))

    result = runner.invoke(
        app,
        ["schema", "diff", "--live", "--path-only"],
    )

    assert result.exit_code == 2
    assert "--path-only is incompatible with live mode" in result.output


def test_schema_diff_rejects_global_url_without_live(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE expected_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))

    result = runner.invoke(
        app,
        ["--url", "postgres://u:p@localhost/live", "schema", "diff"],
    )

    assert result.exit_code == 2
    assert "--url can only be used with schema diff --live." in result.output
