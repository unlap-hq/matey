from __future__ import annotations

from pathlib import Path

import pytest

from matey.config import (
    TARGET_CONFIG_FILE,
    WORKSPACE_CONFIG_FILE,
    CodegenConfig,
    Config,
    ConfigError,
    load_target,
    load_workspace,
    target_env_stem,
)


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_load_workspace_from_dedicated_file(tmp_path: Path) -> None:
    _write(tmp_path / WORKSPACE_CONFIG_FILE, 'targets = ["db/core"]\n')

    workspace = load_workspace(tmp_path)

    assert workspace.source_kind == "workspace"
    assert workspace.targets == ((tmp_path / "db" / "core").resolve(),)


def test_load_workspace_falls_back_to_pyproject(tmp_path: Path) -> None:
    _write(
        tmp_path / "pyproject.toml",
        '[tool.matey]\ntargets = ["db/core"]\n',
    )

    workspace = load_workspace(tmp_path)

    assert workspace.source_kind == "pyproject"
    assert workspace.targets == ((tmp_path / "db" / "core").resolve(),)


def test_workspace_file_wins_over_pyproject(tmp_path: Path) -> None:
    _write(tmp_path / WORKSPACE_CONFIG_FILE, 'targets = ["db/core"]\n')
    _write(
        tmp_path / "pyproject.toml",
        '[tool.matey]\ntargets = ["db/analytics"]\n',
    )

    workspace = load_workspace(tmp_path)

    assert workspace.targets == ((tmp_path / "db" / "core").resolve(),)


def test_load_target_local_config(tmp_path: Path) -> None:
    _write(
        tmp_path / "db" / "core" / TARGET_CONFIG_FILE,
        'engine = "postgres"\nurl_env = "CORE_DATABASE_URL"\ntest_url_env = "CORE_TEST_DATABASE_URL"\n\n[codegen]\nenabled = true\ngenerator = "tables"\n',
    )

    target = load_target(path="db/core", repo_root=tmp_path)

    assert target.name == "db/core"
    assert target.dir == (tmp_path / "db" / "core").resolve()
    assert target.engine == "postgres"
    assert target.url_env == "CORE_DATABASE_URL"
    assert target.test_url_env == "CORE_TEST_DATABASE_URL"
    assert target.codegen == CodegenConfig(enabled=True, generator="tables", options=None)


def test_load_target_rejects_invalid_env_name(tmp_path: Path) -> None:
    _write(
        tmp_path / "db" / "core" / TARGET_CONFIG_FILE,
        'engine = "postgres"\nurl_env = "bad-name"\ntest_url_env = "CORE_TEST_DATABASE_URL"\n',
    )

    with pytest.raises(ConfigError, match="invalid environment variable name"):
        load_target(path="db/core", repo_root=tmp_path)


def test_load_target_rejects_symlinked_target_root(tmp_path: Path) -> None:
    real = tmp_path / "realdb"
    real.mkdir()
    (tmp_path / "db-link").symlink_to(real, target_is_directory=True)
    _write(
        real / TARGET_CONFIG_FILE,
        'engine = "postgres"\nurl_env = "DATABASE_URL"\ntest_url_env = "TEST_DATABASE_URL"\n',
    )

    with pytest.raises(ConfigError, match="symlinked path segment"):
        load_target(path="db-link", repo_root=tmp_path)


def test_config_load_and_select_path_native(tmp_path: Path) -> None:
    _write(tmp_path / WORKSPACE_CONFIG_FILE, 'targets = ["db/core", "db/analytics"]\n')
    _write(
        tmp_path / "db" / "core" / TARGET_CONFIG_FILE,
        'engine = "postgres"\nurl_env = "CORE_DATABASE_URL"\ntest_url_env = "CORE_TEST_DATABASE_URL"\n',
    )
    _write(
        tmp_path / "db" / "analytics" / TARGET_CONFIG_FILE,
        'engine = "mysql"\nurl_env = "ANALYTICS_DATABASE_URL"\ntest_url_env = "ANALYTICS_TEST_DATABASE_URL"\n',
    )

    config = Config.load(tmp_path)

    assert tuple(config.targets.keys()) == ("db/analytics", "db/core")
    assert tuple(target.name for target in config.select(path="db/core")) == ("db/core",)
    assert tuple(target.name for target in config.select(all_targets=True)) == ("db/analytics", "db/core")
    with pytest.raises(ConfigError, match="Multiple targets configured"):
        config.select()


def test_config_select_direct_path_not_in_workspace(tmp_path: Path) -> None:
    _write(tmp_path / WORKSPACE_CONFIG_FILE, 'targets = ["db/core"]\n')
    _write(
        tmp_path / "db" / "core" / TARGET_CONFIG_FILE,
        'engine = "postgres"\nurl_env = "CORE_DATABASE_URL"\ntest_url_env = "CORE_TEST_DATABASE_URL"\n',
    )
    _write(
        tmp_path / "other" / TARGET_CONFIG_FILE,
        'engine = "sqlite"\nurl_env = "OTHER_DATABASE_URL"\ntest_url_env = "OTHER_TEST_DATABASE_URL"\n',
    )

    config = Config.load(tmp_path)

    with pytest.raises(ConfigError, match="is not configured in workspace"):
        config.select(path="other")


def test_config_select_all_requires_workspace_targets(tmp_path: Path) -> None:
    config = Config.load(tmp_path)

    with pytest.raises(ConfigError, match="No targets configured in workspace"):
        config.select(all_targets=True)


def test_explicit_missing_config_path_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        Config.load(tmp_path, config_path=Path("missing.toml"))


def test_config_read_io_error_is_wrapped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    workspace_path = tmp_path / WORKSPACE_CONFIG_FILE
    _write(workspace_path, 'targets = []\n')

    original = Path.read_text

    def _boom(self: Path, *args: object, **kwargs: object) -> str:
        if self == workspace_path:
            raise OSError("permission denied")
        return original(self, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", _boom)

    with pytest.raises(ConfigError, match="Unable to read"):
        load_workspace(tmp_path)


def test_config_invalid_utf8_is_wrapped(tmp_path: Path) -> None:
    workspace_path = tmp_path / WORKSPACE_CONFIG_FILE
    workspace_path.write_bytes(b"\xff\xfe\x00")

    with pytest.raises(ConfigError, match="Unable to decode"):
        load_workspace(tmp_path)


def test_target_env_stem_from_path() -> None:
    assert target_env_stem("db/core") == "DB_CORE"
    assert target_env_stem(".") == "DEFAULT"
