from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

import matey.__main__ as main_module
import matey.cli as cli_module
from matey.cli import _root_help_text, app
from matey.errors import CliUsageError, MateyError


def test_root_help_text_includes_group_and_subgroup_lists() -> None:
    text = _root_help_text()
    assert "db: new, create, wait, up, migrate, status, drift, plan, load, dump, down, drop, dbmate" in text
    assert "db.plan: diff, sql" in text
    assert "schema.plan: diff, sql" in text


def test_group_help_is_non_empty_for_all_groups() -> None:
    runner = CliRunner()
    for command, expected in (
        ("db", "Live database commands."),
        ("schema", "Schema artifact workflows."),
        ("template", "Template helpers."),
    ):
        result = runner.invoke(app, [command, "--help"])
        assert result.exit_code == 0
        assert expected in result.output


def test_main_exit_mapping_for_typed_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_usage() -> None:
        raise CliUsageError("bad args")

    monkeypatch.setattr(main_module, "app", _raise_usage)
    assert main_module.main() == 2

    def _raise_domain() -> None:
        raise MateyError("failure")

    monkeypatch.setattr(main_module, "app", _raise_domain)
    assert main_module.main() == 1

    def _raise_unknown() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(main_module, "app", _raise_unknown)
    assert main_module.main() == 1


def test_template_commands_print_and_write(tmp_path: Path) -> None:
    runner = CliRunner()

    print_result = runner.invoke(app, ["template", "config"])
    assert print_result.exit_code == 0
    assert "url_env" in print_result.output

    config_path = tmp_path / "matey.toml"
    write_result = runner.invoke(app, ["template", "config", "--path", str(config_path)])
    assert write_result.exit_code == 0
    assert config_path.exists()
    assert "test_url_env" in config_path.read_text(encoding="utf-8")

    ci_result = runner.invoke(app, ["template", "ci"])
    assert ci_result.exit_code == 0
    assert "jobs:" in ci_result.output


def test_template_commands_do_not_build_runtime_context(monkeypatch: pytest.MonkeyPatch) -> None:
    def _boom(*, cwd: Path | None = None, dbmate_bin: Path | None = None):
        del cwd, dbmate_bin
        raise AssertionError("build_context should not be called for template commands")

    monkeypatch.setattr(cli_module, "build_context", _boom)
    runner = CliRunner()
    result = runner.invoke(app, ["template", "config"])
    assert result.exit_code == 0
