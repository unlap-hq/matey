from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from matey.domain import (
    MateyConfig,
    PathResolutionError,
    ResolvedPaths,
    SelectedTarget,
    TargetSelectionError,
    URLResolutionError,
)
from matey.env import load_runtime_env


def select_targets(
    config: MateyConfig,
    *,
    target_name: str | None,
    all_targets: bool,
) -> list[SelectedTarget]:
    if all_targets and target_name:
        raise TargetSelectionError("Cannot use --target and --all together.")

    if not config.targets:
        if target_name:
            raise TargetSelectionError("No targets are configured; do not pass --target.")
        return [SelectedTarget(name="default", config=None, implicit=True)]

    if all_targets:
        return [
            SelectedTarget(name=name, config=target_config, implicit=False)
            for name, target_config in config.targets.items()
        ]

    if target_name:
        selected = config.targets.get(target_name)
        if selected is None:
            options = ", ".join(config.targets)
            raise TargetSelectionError(f"Unknown target '{target_name}'. Configured targets: {options}")
        return [SelectedTarget(name=target_name, config=selected, implicit=False)]

    if len(config.targets) == 1:
        only_name, only_target = next(iter(config.targets.items()))
        return [SelectedTarget(name=only_name, config=only_target, implicit=False)]

    raise TargetSelectionError("Multiple targets configured; pass --target NAME or --all.")


def derive_paths(
    config: MateyConfig,
    selected_target: SelectedTarget,
    *,
    dir_override: Path | None,
    cwd: Path | None = None,
) -> ResolvedPaths:
    root = (cwd or Path.cwd()).resolve()

    target_dir = selected_target.config.dir if selected_target.config else None
    raw_dir = str(dir_override) if dir_override is not None else target_dir or config.defaults.dir
    if not raw_dir:
        raise PathResolutionError("Could not resolve migration directory root.")

    db_root = Path(raw_dir)
    if not db_root.is_absolute():
        db_root = root / db_root

    db_dir = db_root / selected_target.name if config.targets else db_root

    return ResolvedPaths(
        db_dir=db_dir,
        migrations_dir=db_dir / "migrations",
        schema_file=db_dir / "schema.sql",
    )


def resolve_real_url(
    config: MateyConfig,
    target: SelectedTarget,
    *,
    cli_url: str | None,
    environ: Mapping[str, str] | None = None,
) -> str:
    if cli_url:
        return cli_url

    runtime_env = load_runtime_env(environ=environ)
    env_name = target.config.url_env if target.config and target.config.url_env else config.defaults.url_env
    value = runtime_env.get(env_name)
    if value:
        return value

    raise URLResolutionError(f"Missing database URL. Set {env_name} or pass --url.")


def resolve_test_url(
    config: MateyConfig,
    target: SelectedTarget,
    *,
    cli_test_url: str | None,
    environ: Mapping[str, str] | None = None,
) -> str | None:
    if cli_test_url:
        return cli_test_url

    runtime_env = load_runtime_env(environ=environ)
    env_names: list[str] = []
    if target.config and target.config.test_url_env:
        env_names.append(target.config.test_url_env)
    env_names.append(config.defaults.test_url_env)

    for env_name in env_names:
        value = runtime_env.get(env_name)
        if value:
            return value
    return None
