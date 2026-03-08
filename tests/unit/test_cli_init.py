from __future__ import annotations

from importlib import import_module
from pathlib import Path

cli = import_module("matey.cli.app")


def test_init_defaults_to_current_directory_target(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(
        [
            "init",
            "--engine",
            "sqlite",
            "--url-env",
            "DATABASE_URL",
            "--test-url-env",
            "TEST_DATABASE_URL",
        ]
    )

    assert rc == 0
    assert (tmp_path / "matey.toml").exists()
    assert (tmp_path / "config.toml").exists()
    assert (tmp_path / "schema.sql").exists()
    assert (tmp_path / "schema.lock.toml").exists()
    assert (tmp_path / "migrations").is_dir()
    assert (tmp_path / "checkpoints").is_dir()
    content = (tmp_path / "matey.toml").read_text(encoding="utf-8")
    assert 'targets = ["."]' in content


def test_init_with_ci_writes_default_ci_path(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(
        [
            "init",
            "--engine",
            "sqlite",
            "--url-env",
            "DATABASE_URL",
            "--test-url-env",
            "TEST_DATABASE_URL",
            "--ci",
            "github",
        ]
    )

    assert rc == 0
    ci_path = tmp_path / ".github" / "workflows" / "matey-schema.yml"
    assert ci_path.exists()
    assert "${{ github.base_ref }}" in ci_path.read_text(encoding="utf-8")


def test_init_target_creates_workspace_target_and_zero_state(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(
        [
            "init",
            "--path",
            "db/core",
            "--engine",
            "sqlite",
            "--url-env",
            "CORE_DATABASE_URL",
            "--test-url-env",
            "CORE_TEST_DATABASE_URL",
        ]
    )

    assert rc == 0
    assert (tmp_path / "matey.toml").exists()
    assert (tmp_path / "db" / "core" / "config.toml").exists()
    assert (tmp_path / "db" / "core" / "schema.sql").exists()
    assert (tmp_path / "db" / "core" / "schema.lock.toml").exists()
    assert (tmp_path / "db" / "core" / "migrations").is_dir()
    assert (tmp_path / "db" / "core" / "checkpoints").is_dir()


def test_init_preserves_existing_workspace_comments_with_tomlkit(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "matey.toml").write_text('# keep me\ntargets = ["db/core"]\n', encoding="utf-8")
    (tmp_path / "db" / "core").mkdir(parents=True, exist_ok=True)
    (tmp_path / "db" / "core" / "config.toml").write_text(
        'engine = "sqlite"\nurl_env = "CORE_DATABASE_URL"\ntest_url_env = "CORE_TEST_DATABASE_URL"\n',
        encoding="utf-8",
    )

    rc = cli.main(
        [
            "init",
            "--path",
            "db/analytics",
            "--engine",
            "sqlite",
            "--url-env",
            "ANALYTICS_DATABASE_URL",
            "--test-url-env",
            "ANALYTICS_TEST_DATABASE_URL",
        ]
    )

    assert rc == 0
    content = (tmp_path / "matey.toml").read_text(encoding="utf-8")
    assert "# keep me" in content
    assert '"db/core"' in content
    assert '"db/analytics"' in content


def test_init_updates_target_local_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    target_root = tmp_path / "db" / "core"
    target_root.mkdir(parents=True)
    (target_root / "config.toml").write_text(
        'engine = "sqlite"\nurl_env = "OLD_URL"\ntest_url_env = "OLD_TEST_URL"\n# keep me\n',
        encoding="utf-8",
    )

    rc = cli.main(
        [
            "init",
            "--path",
            "db/core",
            "--url-env",
            "CORE_DATABASE_URL",
            "--test-url-env",
            "CORE_TEST_DATABASE_URL",
        ]
    )

    assert rc == 0
    content = (target_root / "config.toml").read_text(encoding="utf-8")
    assert 'engine = "sqlite"' in content
    assert 'url_env = "CORE_DATABASE_URL"' in content
    assert "# keep me" in content
    assert "[codegen]" in content
    assert "enabled = true" in content
    assert 'generator = "tables"' in content
    assert '#  options = "..."' in content
