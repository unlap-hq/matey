from __future__ import annotations

import tomllib
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from matey.domain import ConfigError, DefaultsConfig, MateyConfig, TargetConfig


def _load_toml(path: Path) -> dict[str, Any]:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise ConfigError(f"Config file not found: {path}") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"Invalid TOML in {path}: {error}") from error


def _as_mapping(value: Any, *, name: str) -> Mapping[str, Any]:
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return value
    raise ConfigError(f"Expected [{name}] to be a table.")


def _as_str(value: Any, *, name: str, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ConfigError(f"Missing required key: {name}")
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Expected key '{name}' to be a string.")
    stripped = value.strip()
    if required and not stripped:
        raise ConfigError(f"Key '{name}' cannot be empty.")
    return stripped or None


def _parse_config(data: Mapping[str, Any], source_path: Path | None) -> MateyConfig:
    defaults_data = _as_mapping(data.get("defaults"), name="defaults")
    defaults = DefaultsConfig(
        dir=_as_str(defaults_data.get("dir"), name="defaults.dir") or "db",
        url_env=_as_str(defaults_data.get("url_env"), name="defaults.url_env") or "MATEY_URL",
        test_url_env=(
            _as_str(defaults_data.get("test_url_env"), name="defaults.test_url_env")
            or "MATEY_TEST_URL"
        ),
    )

    targets_data = _as_mapping(data.get("targets"), name="targets")
    targets: dict[str, TargetConfig] = {}
    for target_name, target_value in targets_data.items():
        if not isinstance(target_name, str) or not target_name.strip():
            raise ConfigError("Target names must be non-empty strings.")
        target_table = _as_mapping(target_value, name=f"targets.{target_name}")
        targets[target_name] = TargetConfig(
            name=target_name,
            url_env=_as_str(target_table.get("url_env"), name=f"targets.{target_name}.url_env"),
            dir=_as_str(target_table.get("dir"), name=f"targets.{target_name}.dir"),
            test_url_env=_as_str(
                target_table.get("test_url_env"),
                name=f"targets.{target_name}.test_url_env",
            ),
        )

    if len(targets) > 1:
        missing = [target.name for target in targets.values() if not target.url_env]
        if missing:
            missing_joined = ", ".join(missing)
            raise ConfigError(
                "Multi-target configs require url_env on each target. "
                f"Missing for: {missing_joined}"
            )

    return MateyConfig(defaults=defaults, targets=targets, source_path=source_path)


def load_config(config_path: Path | None = None, *, cwd: Path | None = None) -> MateyConfig:
    root = (cwd or Path.cwd()).resolve()

    if config_path is not None:
        explicit_path = config_path if config_path.is_absolute() else (root / config_path)
        explicit_data = _load_toml(explicit_path)
        return _parse_config(explicit_data, explicit_path)

    matey_path = root / "matey.toml"
    if matey_path.exists():
        return _parse_config(_load_toml(matey_path), matey_path)

    pyproject_path = root / "pyproject.toml"
    if pyproject_path.exists():
        pyproject_data = _load_toml(pyproject_path)
        tool_table = _as_mapping(pyproject_data.get("tool"), name="tool")
        matey_table = _as_mapping(tool_table.get("matey"), name="tool.matey")
        if matey_table:
            return _parse_config(matey_table, pyproject_path)

    return _parse_config({}, None)
