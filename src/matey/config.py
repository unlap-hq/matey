from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from types import MappingProxyType
from typing import Any

from matey.paths import (
    PathBoundaryError,
    RelativePathError,
    describe_path_boundary_error,
    normalize_relative_posix_path,
    safe_descendant,
)

_DEFAULTS = {
    "dir": "db",
    "url_env": "DATABASE_URL",
    "test_url_env": "TEST_DATABASE_URL",
}
DEFAULT_CONFIG_VALUES = MappingProxyType(dict(_DEFAULTS))
_SCALAR_KEYS = frozenset(_DEFAULTS.keys())
_TARGET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")
_ENV_NAME_PATTERN = re.compile(r"^[A-Z_][A-Z0-9_]*$")

DefaultsMap = dict[str, str]
TargetsMap = dict[str, dict[str, str]]


class ConfigError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class TargetConfig:
    name: str
    dir: Path
    url_env: str
    test_url_env: str

    @property
    def schema(self) -> Path:
        return self.dir / "schema.sql"

    @property
    def migrations(self) -> Path:
        return self.dir / "migrations"

    @property
    def checkpoints(self) -> Path:
        return self.dir / "checkpoints"

    @property
    def lockfile(self) -> Path:
        return self.dir / "schema.lock.toml"


class Config:
    def __init__(self, targets: dict[str, TargetConfig]) -> None:
        if not targets:
            raise ConfigError("Config must contain at least one target.")
        ordered = dict(sorted(targets.items(), key=lambda item: item[0]))
        self._targets = MappingProxyType(ordered)

    @property
    def targets(self) -> MappingProxyType[str, TargetConfig]:
        return self._targets

    @classmethod
    def load(
        cls,
        repo_root: Path,
        config_path: Path | None = None,
        config_root: Path | None = None,
    ) -> Config:
        py_defaults, py_targets = _load_pyproject_source(repo_root)
        file_defaults, file_targets = _load_matey_source(repo_root, config_path)
        defaults, targets = _merge_sources(
            defaults_a=py_defaults,
            targets_a=py_targets,
            defaults_b=file_defaults,
            targets_b=file_targets,
        )
        target_root = (config_root if config_root is not None else repo_root).resolve()
        return cls(_resolve_targets(repo_root=target_root, defaults=defaults, targets=targets))

    def select(
        self,
        *,
        target: str | None = None,
        all_targets: bool = False,
    ) -> tuple[TargetConfig, ...]:
        if target is not None and all_targets:
            raise ConfigError("Cannot combine --target with --all.")

        if target is not None:
            selected = self._targets.get(target)
            if selected is None:
                available = ", ".join(self._targets.keys())
                raise ConfigError(f"Unknown target {target!r}. Available targets: {available}")
            return (selected,)

        if all_targets:
            return tuple(self._targets.values())

        if len(self._targets) == 1:
            return tuple(self._targets.values())

        available = ", ".join(self._targets.keys())
        raise ConfigError(
            "Multiple targets configured; choose one with --target or use --all. "
            f"Available targets: {available}"
        )


def _load_toml(path: Path, *, label: str) -> dict[str, Any]:
    try:
        parsed = tomllib.loads(path.read_text(encoding="utf-8"))
    except OSError as error:
        raise ConfigError(f"Unable to read {label}: {error.strerror or error}") from error
    except UnicodeDecodeError as error:
        raise ConfigError(f"Unable to decode {label} as UTF-8.") from error
    except tomllib.TOMLDecodeError as error:
        raise ConfigError(f"Unable to parse {label}: {error}") from error
    if not isinstance(parsed, dict):
        raise ConfigError(f"Invalid TOML in {label}: expected a top-level table.")
    return parsed


def _load_pyproject_source(repo_root: Path) -> tuple[DefaultsMap, TargetsMap]:
    pyproject_path = repo_root / "pyproject.toml"
    if not pyproject_path.exists():
        return {}, {}

    parsed = _load_toml(pyproject_path, label="pyproject.toml")
    tool = parsed.get("tool")
    if tool is None:
        return {}, {}
    if not isinstance(tool, dict):
        raise ConfigError("Invalid pyproject.toml: [tool] must be a table.")

    section = tool.get("matey")
    if section is None:
        return {}, {}
    if not isinstance(section, dict):
        raise ConfigError("Invalid pyproject.toml: [tool.matey] must be a table.")

    return _extract_source(section, source="pyproject.toml [tool.matey]")


def _load_matey_source(
    repo_root: Path,
    config_path: Path | None,
) -> tuple[DefaultsMap, TargetsMap]:
    if config_path is None:
        path = repo_root / "matey.toml"
        if not path.exists():
            return {}, {}
    else:
        path = config_path if config_path.is_absolute() else (repo_root / config_path)
        if not path.exists():
            raise ConfigError(f"Config file not found: {path}")

    parsed = _load_toml(path, label=str(path))
    return _extract_source(parsed, source=str(path))


def _extract_source(doc: dict[str, Any], *, source: str) -> tuple[DefaultsMap, TargetsMap]:
    defaults: DefaultsMap = {}
    targets: TargetsMap = {}

    for key, value in doc.items():
        if key in _SCALAR_KEYS:
            if not isinstance(value, str):
                raise ConfigError(f"{source}: {key!r} must be a string.")
            defaults[key] = value
            continue

        if key in {"defaults", "targets", "base_ref"}:
            raise ConfigError(
                f"{source}: legacy key {key!r} is not supported. "
                "Use top-level defaults plus direct target tables ([core], [analytics], ...)."
            )

        _require_target_name(key, source=source)
        if not isinstance(value, dict):
            raise ConfigError(f"{source}: {key!r} must be a target table.")

        override: dict[str, str] = {}
        for override_key, override_value in value.items():
            if override_key not in _SCALAR_KEYS:
                raise ConfigError(f"{source}: target {key!r} has unsupported key {override_key!r}.")
            if not isinstance(override_value, str):
                raise ConfigError(
                    f"{source}: target {key!r} field {override_key!r} must be a string."
                )
            override[override_key] = override_value
        targets[key] = override

    return defaults, targets


def _merge_sources(
    *,
    defaults_a: DefaultsMap,
    targets_a: TargetsMap,
    defaults_b: DefaultsMap,
    targets_b: TargetsMap,
) -> tuple[DefaultsMap, TargetsMap]:
    defaults = dict(_DEFAULTS)
    defaults.update(defaults_a)
    defaults.update(defaults_b)

    targets: TargetsMap = {}
    for source_targets in (targets_a, targets_b):
        for name, override in source_targets.items():
            targets.setdefault(name, {})
            targets[name].update(override)
    return defaults, targets


def _resolve_targets(
    *,
    repo_root: Path,
    defaults: DefaultsMap,
    targets: TargetsMap,
) -> dict[str, TargetConfig]:
    _require_env_name(defaults["url_env"], source="defaults.url_env")
    _require_env_name(defaults["test_url_env"], source="defaults.test_url_env")

    if not targets:
        targets = {
            "default": {
                "dir": defaults["dir"],
                "url_env": defaults["url_env"],
                "test_url_env": defaults["test_url_env"],
            }
        }

    root = repo_root.resolve()
    resolved: dict[str, TargetConfig] = {}
    seen_dirs: dict[Path, str] = {}

    for name in sorted(targets.keys()):
        override = targets[name]
        dir_value = override.get("dir", _target_default_dir(defaults["dir"], name))
        url_env = override.get("url_env", defaults["url_env"])
        test_url_env = override.get("test_url_env", defaults["test_url_env"])

        _require_env_name(url_env, source=f"{name}.url_env")
        _require_env_name(test_url_env, source=f"{name}.test_url_env")
        dir_path = _normalize_rel_dir(root=root, raw=dir_value, source=f"{name}.dir")

        previous = seen_dirs.get(dir_path)
        if previous is not None:
            raise ConfigError(
                f"Targets {previous!r} and {name!r} resolve to the same directory: {dir_path}"
            )
        seen_dirs[dir_path] = name
        resolved[name] = TargetConfig(
            name=name,
            dir=dir_path,
            url_env=url_env,
            test_url_env=test_url_env,
        )

    return resolved


def normalize_target_names(targets: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in targets:
        value = raw.strip()
        if not value:
            raise ConfigError("Target names cannot be empty.")
        if not _TARGET_NAME_PATTERN.fullmatch(value):
            raise ConfigError(f"Invalid target name: {value!r}")
        if value in seen:
            raise ConfigError(f"Duplicate target name: {value!r}")
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def target_env_stem(target: str) -> str:
    _require_target_name(target, source="target")
    stem = target.replace("-", "_").upper()
    if stem and stem[0].isdigit():
        stem = f"_{stem}"
    return stem


def _target_default_dir(default_dir: str, target_name: str) -> str:
    return (PurePosixPath(default_dir) / target_name).as_posix()


def _normalize_rel_dir(*, root: Path, raw: str, source: str) -> Path:
    try:
        normalized = normalize_relative_posix_path(raw, label=f"{source}: dir")
    except RelativePathError as error:
        raise ConfigError(str(error)) from error

    try:
        return safe_descendant(
            root=root,
            candidate=root / Path(normalized),
            label=f"{source} dir",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
    except PathBoundaryError as error:
        raise ConfigError(
            describe_path_boundary_error(
                error,
                path=root / Path(normalized),
                symlink_message=f"{source}: dir uses symlinked path segment",
            )
        ) from error


def _require_target_name(name: str, *, source: str) -> None:
    if not _TARGET_NAME_PATTERN.fullmatch(name):
        raise ConfigError(f"{source}: invalid target name {name!r}.")


def _require_env_name(name: str, *, source: str) -> None:
    if not _ENV_NAME_PATTERN.fullmatch(name):
        raise ConfigError(
            f"{source}: invalid environment variable name {name!r}; expected [A-Z_][A-Z0-9_]*."
        )


__all__ = [
    "DEFAULT_CONFIG_VALUES",
    "Config",
    "ConfigError",
    "TargetConfig",
    "normalize_target_names",
    "target_env_stem",
]
