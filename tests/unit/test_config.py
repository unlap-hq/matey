from __future__ import annotations

from pathlib import Path

import pytest

from matey.domain import ConfigError
from matey.settings.config import load_config


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def test_load_config_defaults_when_no_config_files(tmp_path: Path) -> None:
    config = load_config(cwd=tmp_path)

    assert config.source_path is None
    assert config.defaults.dir == "db"
    assert config.defaults.url_env == "MATEY_URL"
    assert config.defaults.test_url_env == "MATEY_TEST_URL"
    assert config.targets == {}


def test_load_config_prefers_matey_toml_over_pyproject(tmp_path: Path) -> None:
    _write(
        tmp_path / "pyproject.toml",
        """
[tool.matey.defaults]
dir = "from-pyproject"
""".strip(),
    )
    _write(
        tmp_path / "matey.toml",
        """
[defaults]
dir = "from-matey"
url_env = "MY_DB_URL"
""".strip(),
    )

    config = load_config(cwd=tmp_path)
    assert config.source_path == tmp_path / "matey.toml"
    assert config.defaults.dir == "from-matey"
    assert config.defaults.url_env == "MY_DB_URL"


def test_load_config_uses_pyproject_when_matey_toml_missing(tmp_path: Path) -> None:
    _write(
        tmp_path / "pyproject.toml",
        """
[tool.matey.defaults]
dir = "from-pyproject"
url_env = "PYPROJECT_URL"

[tool.matey.targets.core]
url_env = "CORE_URL"
""".strip(),
    )

    config = load_config(cwd=tmp_path)

    assert config.source_path == tmp_path / "pyproject.toml"
    assert config.defaults.dir == "from-pyproject"
    assert config.defaults.url_env == "PYPROJECT_URL"
    assert list(config.targets) == ["core"]


def test_load_config_raises_when_explicit_config_missing(tmp_path: Path) -> None:
    missing = tmp_path / "missing.toml"
    with pytest.raises(ConfigError, match="Config file not found"):
        load_config(missing, cwd=tmp_path)


def test_load_config_rejects_multi_target_without_url_env(tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[targets.core]
url_env = "CORE_URL"

[targets.analytics]
test_url_env = "ANALYTICS_TEST_URL"
""".strip(),
    )

    with pytest.raises(ConfigError, match="Multi-target configs require url_env"):
        load_config(cwd=tmp_path)
