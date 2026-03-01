from pathlib import Path

import pytest

from matey.config import load_effective_config, select_target_names
from matey.errors import ConfigError, TargetSelectionError


def test_load_effective_config_from_top_level_target_tables(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "matey.toml").write_text(
        """
dir = "db"
url_env = "DATABASE_URL"
test_url_env = "TEST_DATABASE_URL"

[core]
dir = "db/core"
url_env = "CORE_URL"
test_url_env = "CORE_TEST_URL"
""",
        encoding="utf-8",
    )

    config = load_effective_config(repo_root=repo, config_path=None)
    assert "core" in config.targets
    assert config.targets["core"].db_dir == (repo / "db/core").resolve()
    assert config.targets["core"].url_env == "CORE_URL"
    assert config.targets["core"].test_url_env == "CORE_TEST_URL"


def test_load_effective_config_uses_implicit_default_target_when_no_tables(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "matey.toml").write_text(
        """
dir = "db/main"
url_env = "DATABASE_URL"
test_url_env = "TEST_DATABASE_URL"
""",
        encoding="utf-8",
    )

    config = load_effective_config(repo_root=repo, config_path=None)
    assert tuple(config.targets) == ("default",)
    assert config.targets["default"].db_dir == (repo / "db/main").resolve()


def test_select_target_names_requires_explicit_selection_for_multi_target(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "matey.toml").write_text(
        """
[a]
dir = "db/a"
[b]
dir = "db/b"
""",
        encoding="utf-8",
    )
    config = load_effective_config(repo_root=repo, config_path=None)
    with pytest.raises(TargetSelectionError):
        select_target_names(config=config, target=None, select_all=False)


def test_load_effective_config_rejects_legacy_shape_as_invalid(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "matey.toml").write_text(
        """
[defaults]
dir = "db"
[targets.core]
dir = "db/core"
""",
        encoding="utf-8",
    )
    with pytest.raises(ConfigError, match="Unsupported keys under target \\[targets\\]"):
        load_effective_config(repo_root=repo, config_path=None)
