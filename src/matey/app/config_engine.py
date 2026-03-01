from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from matey.domain.errors import ConfigError, TargetSelectionError
from matey.domain.model import (
    CHECKPOINTS_DIRNAME,
    LOCK_FILENAME,
    MIGRATIONS_DIRNAME,
    SCHEMA_FILENAME,
    ConfigDefaults,
    ConfigTarget,
    MateyConfig,
    ResolvedTargetConfig,
    TargetId,
    TargetPaths,
)

_DEFAULT_CONFIG_TEMPLATE = """dir = "db"
url_env = "DATABASE_URL"
test_url_env = "TEST_DATABASE_URL"

[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
"""

_RESERVED_SCALAR_KEYS = {"dir", "url_env", "test_url_env"}
_TARGET_ALLOWED_KEYS = {"dir", "url_env", "test_url_env"}


@dataclass(frozen=True)
class ResolvedConfig:
    defaults: ConfigDefaults
    targets: dict[str, ResolvedTargetConfig]


@dataclass(frozen=True)
class TargetRuntime:
    target_id: TargetId
    paths: TargetPaths
    url_env: str
    test_url_env: str


class ConfigTemplateEngine:
    def render(self) -> str:
        return _DEFAULT_CONFIG_TEMPLATE

    def write(self, *, path: Path, overwrite: bool) -> None:
        if path.exists() and not overwrite:
            raise ConfigError(f"Config file already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.render(), encoding="utf-8")


def _load_toml_file(path: Path) -> dict:
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except Exception as error:
        raise ConfigError(f"Failed to parse config file {path}: {error}") from error


def _optional_string(*, payload: dict, key: str) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise ConfigError(f"Expected {key!r} to be a string.")
    stripped = value.strip()
    return stripped or None


def _parse_target_table(*, target_name: str, payload: dict) -> ConfigTarget:
    unknown_keys = sorted(set(payload.keys()) - _TARGET_ALLOWED_KEYS)
    if unknown_keys:
        raise ConfigError(
            f"Unsupported keys under target [{target_name}]: {', '.join(unknown_keys)}."
        )
    return ConfigTarget(
        dir=_optional_string(payload=payload, key="dir"),
        url_env=_optional_string(payload=payload, key="url_env"),
        test_url_env=_optional_string(payload=payload, key="test_url_env"),
    )


def _parse_matey_payload(payload: dict) -> MateyConfig:
    defaults = ConfigDefaults(
        dir=_optional_string(payload=payload, key="dir") or "db",
        url_env=_optional_string(payload=payload, key="url_env") or "MATEY_URL",
        test_url_env=_optional_string(payload=payload, key="test_url_env") or "MATEY_TEST_URL",
    )

    targets: dict[str, ConfigTarget] = {}
    for key, value in payload.items():
        if key in _RESERVED_SCALAR_KEYS:
            continue
        if not isinstance(value, dict):
            raise ConfigError(
                f"Unexpected top-level key {key!r}; expected a target table."
            )
        targets[key] = _parse_target_table(target_name=key, payload=value)

    return MateyConfig(defaults=defaults, targets=targets or None)


def _config_from_file(path: Path | None) -> MateyConfig:
    if path is None:
        return MateyConfig()
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    payload = _load_toml_file(path)
    try:
        return _parse_matey_payload(payload)
    except ConfigError:
        raise
    except Exception as error:
        raise ConfigError(f"Invalid matey config at {path}: {error}") from error


def _merge_target(base: ConfigTarget | None, override: ConfigTarget) -> ConfigTarget:
    if base is None:
        return override
    return ConfigTarget(
        dir=override.dir if override.dir is not None else base.dir,
        url_env=override.url_env if override.url_env is not None else base.url_env,
        test_url_env=override.test_url_env if override.test_url_env is not None else base.test_url_env,
    )


def _merge_config(base: MateyConfig, override: MateyConfig) -> MateyConfig:
    merged_defaults = ConfigDefaults(
        dir=override.defaults.dir or base.defaults.dir,
        url_env=override.defaults.url_env or base.defaults.url_env,
        test_url_env=override.defaults.test_url_env or base.defaults.test_url_env,
    )
    merged_targets: dict[str, ConfigTarget] = {}

    for source in (base.targets or {}, override.targets or {}):
        for name, target in source.items():
            existing = merged_targets.get(name)
            merged_targets[name] = _merge_target(existing, target)

    return MateyConfig(defaults=merged_defaults, targets=merged_targets or None)


def load_effective_config(*, repo_root: Path, config_path: Path | None) -> ResolvedConfig:
    if config_path is not None:
        cfg = _config_from_file(config_path)
    else:
        matey_toml = repo_root / "matey.toml"
        cfg = _config_from_file(matey_toml if matey_toml.exists() else None)

    merged = _merge_config(MateyConfig(), cfg)
    defaults = merged.defaults

    targets: dict[str, ResolvedTargetConfig] = {}
    raw_targets = merged.targets or {}
    if not raw_targets:
        db_dir = (repo_root / defaults.dir).resolve()
        targets["default"] = ResolvedTargetConfig(
            name="default",
            db_dir=db_dir,
            url_env=defaults.url_env,
            test_url_env=defaults.test_url_env,
        )
    else:
        for name, target_cfg in sorted(raw_targets.items()):
            dir_value = target_cfg.dir if target_cfg.dir is not None else defaults.dir
            url_env = target_cfg.url_env if target_cfg.url_env is not None else defaults.url_env
            test_url_env = (
                target_cfg.test_url_env
                if target_cfg.test_url_env is not None
                else defaults.test_url_env
            )
            targets[name] = ResolvedTargetConfig(
                name=name,
                db_dir=(repo_root / dir_value).resolve(),
                url_env=url_env,
                test_url_env=test_url_env,
            )

    return ResolvedConfig(defaults=defaults, targets=targets)


def select_target_names(*, config: ResolvedConfig, target: str | None, select_all: bool) -> tuple[str, ...]:
    if target and select_all:
        raise TargetSelectionError("Use either --target or --all, not both.")

    names = tuple(sorted(config.targets.keys()))
    if target is not None:
        if target not in config.targets:
            raise TargetSelectionError(f"Unknown target: {target}")
        return (target,)

    if select_all:
        return names

    if len(names) == 1:
        return names

    raise TargetSelectionError("Multiple targets configured. Use --target <name> or --all.")


def build_target_runtime(*, resolved: ResolvedTargetConfig) -> TargetRuntime:
    db_dir = resolved.db_dir
    return TargetRuntime(
        target_id=TargetId(resolved.name),
        paths=TargetPaths(
            db_dir=db_dir,
            migrations_dir=db_dir / MIGRATIONS_DIRNAME,
            checkpoints_dir=db_dir / CHECKPOINTS_DIRNAME,
            schema_file=db_dir / SCHEMA_FILENAME,
            lock_file=db_dir / LOCK_FILENAME,
        ),
        url_env=resolved.url_env,
        test_url_env=resolved.test_url_env,
    )
