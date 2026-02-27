from __future__ import annotations

from pathlib import Path

from typer.testing import CliRunner

from matey.cli import app

runner = CliRunner()


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_ci_print_github_default(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["ci", "print", "github"])
    assert result.exit_code == 0
    assert "matey schema validate" in result.output
    assert "pull_request" in result.output


def test_ci_print_github_with_targets_includes_matrix(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["ci", "print", "github", "--targets", "core,analytics"])
    assert result.exit_code == 0
    assert "matrix" in result.output
    assert '"core"' in result.output
    assert '"analytics"' in result.output


def test_ci_init_writes_workflow_file(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["ci", "init", "github"])
    workflow_path = tmp_path / ".github" / "workflows" / "matey-schema-validate.yml"
    assert result.exit_code == 0
    assert workflow_path.exists()
    assert "matey schema validate" in _read(workflow_path)


def test_ci_init_targets_only_writes_workflow(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["ci", "init", "github", "--targets", "core,analytics"])
    workflow_path = tmp_path / ".github" / "workflows" / "matey-schema-validate.yml"
    assert result.exit_code == 0
    assert workflow_path.exists()
    assert not (tmp_path / "matey.toml").exists()


def test_ci_init_rejects_print_flag(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["ci", "init", "github", "--targets", "core", "--print"])
    assert result.exit_code != 0
    assert "No such option: --print" in result.output
    assert not (tmp_path / ".github" / "workflows" / "matey-schema-validate.yml").exists()


def test_ci_init_refuses_overwrite_without_force(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    workflow_path = tmp_path / ".github" / "workflows" / "matey-schema-validate.yml"
    workflow_path.parent.mkdir(parents=True, exist_ok=True)
    workflow_path.write_text("existing\n", encoding="utf-8")

    result = runner.invoke(app, ["ci", "init", "github"])
    assert result.exit_code == 1
    assert "refusing to overwrite existing file" in result.output
    assert _read(workflow_path) == "existing\n"


def test_ci_init_force_overwrites(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    workflow_path = tmp_path / ".github" / "workflows" / "matey-schema-validate.yml"
    workflow_path.parent.mkdir(parents=True, exist_ok=True)
    workflow_path.write_text("existing\n", encoding="utf-8")

    result = runner.invoke(app, ["ci", "init", "github", "--force"])
    assert result.exit_code == 0
    assert "matey schema validate" in _read(workflow_path)
    assert _read(workflow_path) != "existing\n"


def test_ci_print_invalid_target_name_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["ci", "print", "github", "--targets", "core,bad target"])
    assert result.exit_code == 2
    assert "Invalid target name" in result.output
