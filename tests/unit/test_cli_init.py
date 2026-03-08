from __future__ import annotations

from importlib import import_module
from pathlib import Path

cli = import_module("matey.cli.app")


def test_init_no_target_writes_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(["init", "--no-target"])

    assert rc == 0
    content = (tmp_path / "matey.toml").read_text(encoding="utf-8")
    assert 'dir = "db"' in content
    assert 'url_env = "DATABASE_URL"' in content


def test_init_with_ci_writes_default_ci_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(["init", "--no-target", "--ci", "github"])

    assert rc == 0
    ci_path = tmp_path / ".github" / "workflows" / "matey-schema.yml"
    assert ci_path.exists()
    assert "${{ github.base_ref }}" in ci_path.read_text(encoding="utf-8")


def test_init_target_creates_zero_state_target(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(["init", "--engine", "sqlite"])

    assert rc == 0
    assert (tmp_path / "matey.toml").exists()
    assert (tmp_path / "db" / "schema.sql").exists()
    assert (tmp_path / "db" / "schema.lock.toml").exists()
    assert (tmp_path / "db" / "migrations").is_dir()
    assert (tmp_path / "db" / "checkpoints").is_dir()


def test_init_preserves_existing_config_comments_with_tomlkit(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "matey.toml").write_text(
        '# keep me\n'
        'dir = "db"\n'
        'url_env = "DATABASE_URL"\n'
        'test_url_env = "TEST_DATABASE_URL"\n',
        encoding="utf-8",
    )

    rc = cli.main(["init", "--no-target", "--url-env", "NEW_DATABASE_URL"])

    assert rc == 0
    content = (tmp_path / "matey.toml").read_text(encoding="utf-8")
    assert "# keep me" in content
    assert 'url_env = "NEW_DATABASE_URL"' in content


def test_init_named_target_omits_implicit_dir(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(
        [
            "init",
            "--target",
            "core",
            "--engine",
            "sqlite",
            "--no-target",
        ]
    )

    assert rc == 0
    content = (tmp_path / "matey.toml").read_text(encoding="utf-8")
    assert "[core]" in content
    assert 'dir = "db/core"' not in content
