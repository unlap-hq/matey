from __future__ import annotations

from pathlib import Path

from matey import __main__ as cli


def test_template_config_prints_to_stdout(capsys) -> None:
    rc = cli.main(["template", "config"])
    assert rc == 0
    captured = capsys.readouterr()
    assert 'dir = "db"' in captured.out
    assert 'url_env = "DATABASE_URL"' in captured.out


def test_template_config_writes_when_path_provided(tmp_path: Path) -> None:
    destination = tmp_path / "matey.toml"
    rc = cli.main(["template", "config", "core", "--path", str(destination)])
    assert rc == 0
    assert destination.exists()
    content = destination.read_text(encoding="utf-8")
    assert "[core]" in content


def test_template_config_refuses_overwrite_without_flag(tmp_path: Path) -> None:
    destination = tmp_path / "matey.toml"
    destination.write_text("existing\n", encoding="utf-8")
    rc = cli.main(["template", "config", "--path", str(destination)])
    assert rc == 2


def test_template_ci_prints_provider_template(capsys) -> None:
    rc = cli.main(["template", "ci", "--provider", "github"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "${{ github.base_ref }}" in captured.out


def test_template_ci_writes_when_path_provided(tmp_path: Path) -> None:
    destination = tmp_path / "ci.yml"
    rc = cli.main(
        ["template", "ci", "--provider", "gitlab", "--path", str(destination)]
    )
    assert rc == 0
    assert destination.exists()
    content = destination.read_text(encoding="utf-8")
    assert "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME" in content

