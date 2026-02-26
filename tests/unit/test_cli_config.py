from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from matey.cli import app

runner = CliRunner()


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_config_print_with_targets_outputs_skeleton(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["config", "print", "--targets", "core,analytics"])
    assert result.exit_code == 0
    assert "[defaults]" in result.output
    assert "[targets.core]" in result.output
    assert 'url_env = "CORE_DATABASE_URL"' in result.output
    assert "[targets.analytics]" in result.output
    assert 'url_env = "ANALYTICS_DATABASE_URL"' in result.output
    assert not (tmp_path / "matey.toml").exists()


def test_config_print_without_targets_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["config", "print"])
    assert result.exit_code == 2
    assert "Cannot render target skeleton config without targets." in result.output


def test_config_init_writes_config_skeleton(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["config", "init", "--targets", "core,analytics"])
    config_path = tmp_path / "matey.toml"
    assert result.exit_code == 0
    assert config_path.exists()
    config_text = _read(config_path)
    assert "[targets.core]" in config_text
    assert 'url_env = "CORE_DATABASE_URL"' in config_text
    assert "[targets.analytics]" in config_text
    assert 'url_env = "ANALYTICS_DATABASE_URL"' in config_text


def test_config_init_respects_global_config_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    custom_path = tmp_path / "configs" / "custom-matey.toml"
    result = runner.invoke(
        app,
        ["--config", str(custom_path), "config", "init", "--targets", "core"],
    )
    assert result.exit_code == 0
    assert custom_path.exists()
    assert "[targets.core]" in _read(custom_path)


def test_config_init_refuses_overwrite_without_force(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "matey.toml"
    config_path.write_text("existing\n", encoding="utf-8")

    result = runner.invoke(app, ["config", "init", "--targets", "core"])
    assert result.exit_code == 1
    assert "refusing to overwrite existing file" in result.output
    assert _read(config_path) == "existing\n"


def test_config_init_force_overwrites_existing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "matey.toml"
    config_path.write_text("existing\n", encoding="utf-8")

    result = runner.invoke(app, ["config", "init", "--targets", "core", "--force"])
    assert result.exit_code == 0
    assert _read(config_path) != "existing\n"
    assert "[targets.core]" in _read(config_path)
