from __future__ import annotations

from pathlib import Path

import pytest

from matey.config import Config, ConfigError


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_load_implicit_default_target_from_top_level(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "MATEY_URL"
test_url_env = "MATEY_TEST_URL"
""".strip(),
    )

    config = Config.load(tmp_path)
    targets = config.targets
    assert tuple(targets.keys()) == ("default",)
    default = targets["default"]
    assert default.dir == (tmp_path / "db").resolve()
    assert default.url_env == "MATEY_URL"
    assert default.test_url_env == "MATEY_TEST_URL"
    assert default.schema == (tmp_path / "db" / "schema.sql").resolve()


def test_target_dir_defaults_to_defaults_dir_plus_target_name(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "MATEY_URL"
test_url_env = "MATEY_TEST_URL"

[core]
url_env = "CORE_URL"
test_url_env = "CORE_TEST_URL"
""".strip(),
    )

    config = Config.load(tmp_path)
    core = config.targets["core"]
    assert core.dir == (tmp_path / "db" / "core").resolve()


def test_matey_toml_overrides_pyproject_tool_matey(tmp_path: Path) -> None:
    _write(
        tmp_path / "pyproject.toml",
        """
[tool.matey]
dir = "db_py"
url_env = "PY_URL"
test_url_env = "PY_TEST_URL"
""".strip(),
    )
    _write(
        tmp_path / "matey.toml",
        """
dir = "db_file"
url_env = "FILE_URL"
test_url_env = "FILE_TEST_URL"
""".strip(),
    )

    config = Config.load(tmp_path)
    default = config.targets["default"]
    assert default.dir == (tmp_path / "db_file").resolve()
    assert default.url_env == "FILE_URL"
    assert default.test_url_env == "FILE_TEST_URL"


def test_legacy_defaults_shape_is_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[defaults]
dir = "db"
""".strip(),
    )

    with pytest.raises(ConfigError):
        Config.load(tmp_path)


def test_unknown_key_is_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "MATEY_URL"
test_url_env = "MATEY_TEST_URL"
surprise = "x"
""".strip(),
    )

    with pytest.raises(ConfigError):
        Config.load(tmp_path)


def test_invalid_target_name_is_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "MATEY_URL"
test_url_env = "MATEY_TEST_URL"

["bad.name"]
url_env = "X"
test_url_env = "Y"
""".strip(),
    )

    with pytest.raises(ConfigError):
        Config.load(tmp_path)


def test_absolute_dir_is_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "/abs/path"
url_env = "MATEY_URL"
test_url_env = "MATEY_TEST_URL"
""".strip(),
    )

    with pytest.raises(ConfigError):
        Config.load(tmp_path)


def test_duplicate_resolved_target_dirs_are_rejected(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "MATEY_URL"
test_url_env = "MATEY_TEST_URL"

[core]
dir = "db/shared"
url_env = "CORE_URL"
test_url_env = "CORE_TEST_URL"

[analytics]
dir = "db/shared"
url_env = "AN_URL"
test_url_env = "AN_TEST_URL"
""".strip(),
    )

    with pytest.raises(ConfigError):
        Config.load(tmp_path)


def test_select_rules(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "MATEY_URL"
test_url_env = "MATEY_TEST_URL"

[core]
url_env = "CORE_URL"
test_url_env = "CORE_TEST_URL"

[analytics]
url_env = "AN_URL"
test_url_env = "AN_TEST_URL"
""".strip(),
    )

    config = Config.load(tmp_path)
    assert tuple(target.name for target in config.select(target="core")) == ("core",)
    assert tuple(target.name for target in config.select(all_targets=True)) == ("analytics", "core")

    with pytest.raises(ConfigError):
        config.select()
    with pytest.raises(ConfigError):
        config.select(target="core", all_targets=True)


def test_explicit_missing_config_path_errors(tmp_path: Path) -> None:
    with pytest.raises(ConfigError):
        Config.load(tmp_path, config_path=Path("missing.toml"))
