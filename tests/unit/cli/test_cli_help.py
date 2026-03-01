from typer.testing import CliRunner

from matey.cli import app


def test_root_help_lists_groups() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "db" in result.output
    assert "schema" in result.output
    assert "template" in result.output


def test_db_help_contains_plan_and_drift() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["db", "--help"])
    assert result.exit_code == 0
    assert "drift" in result.output
    assert "plan" in result.output
    assert "dbmate" in result.output
