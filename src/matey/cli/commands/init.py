from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

import matey.schema as schema_api
from matey.cli.template import (
    TemplateProvider,
    default_ci_template_path,
    render_ci_template,
    render_updated_config,
    write_text_file,
)
from matey.config import Config, ConfigError

from ..render import Renderer
from .common import CliUsageError, ConfigOpt, EngineOpt, OverwriteOpt, find_repo_root_or_none

DirOpt = Annotated[str | None, Parameter(name="--dir", help="Target directory relative to the config root.")]
UrlEnvInitOpt = Annotated[str | None, Parameter(name="--url-env", help="Live database URL environment variable.")]
TestUrlEnvInitOpt = Annotated[
    str | None,
    Parameter(name="--test-url-env", help="Scratch database URL environment variable."),
]
CiOpt = Annotated[
    TemplateProvider | None,
    Parameter(name="--ci", help="Write a CI template for the selected provider."),
]
ConfigOnlyOpt = Annotated[
    bool,
    Parameter(
        name="--config-only",
        negative=(),
        help="Create/update config only; skip zero-state target initialization.",
    ),
]
TargetOpt = Annotated[str | None, Parameter(name="--target", help="Initialize or add a named target; omit for the root/default target.")]


def register_init_command(*, root_app: App, renderer: Renderer) -> None:
    @root_app.command(name="init", sort_key=30)
    def init_command(
        target: TargetOpt = None,
        config: ConfigOpt = None,
        engine: EngineOpt = None,
        dir: DirOpt = None,
        url_env: UrlEnvInitOpt = None,
        test_url_env: TestUrlEnvInitOpt = None,
        ci: CiOpt = None,
        config_only: ConfigOnlyOpt = False,
        overwrite: OverwriteOpt = False,
    ) -> None:
        """Initialize matey config, zero-state target artifacts, and optional CI."""
        repo_root, config_path = resolve_init_paths(config)
        current = load_existing_config(config_path=config_path, repo_root=repo_root)
        existing_text = config_path.read_text(encoding="utf-8") if config_path.exists() else None
        rendered, config_obj = render_updated_config(
            current=current,
            repo_root=repo_root,
            config_root=config_path.parent,
            existing_text=existing_text,
            target_name=target,
            dir_value=dir,
            url_env=url_env,
            test_url_env=test_url_env,
        )
        init_plan = None
        if not config_only:
            selected_target = (
                config_obj.default_target
                if target is None
                else config_obj.select(target=target, all_targets=False)[0]
            )
            init_plan = schema_api.prepare_init_target(
                selected_target,
                engine=engine,
                overwrite=overwrite,
            )

        if existing_text is None or existing_text != rendered:
            write_text_file(config_path, rendered, overwrite=True)
            renderer.template_written(str(config_path))

        if ci is not None:
            ci_path = repo_root / default_ci_template_path(ci)
            write_text_file(ci_path, render_ci_template(ci), overwrite=overwrite)
            renderer.template_written(str(ci_path))

        if init_plan is not None:
            renderer.init_target(schema_api.apply_init_target(init_plan))


def resolve_init_paths(config_path: Path | None) -> tuple[Path, Path]:
    if config_path is not None:
        resolved = config_path if config_path.is_absolute() else (Path.cwd() / config_path)
        resolved = resolved.resolve()
        repo_root = find_repo_root_or_none(resolved.parent) or resolved.parent
        return repo_root, resolved
    repo_root = find_repo_root_or_none(Path.cwd().resolve()) or Path.cwd().resolve()
    return repo_root, repo_root / "matey.toml"


def load_existing_config(*, config_path: Path, repo_root: Path) -> Config | None:
    if not config_path.exists():
        return None
    try:
        return Config.load(
            repo_root,
            config_path=config_path,
            config_root=config_path.parent,
        )
    except ConfigError as error:
        raise CliUsageError(str(error)) from error


__all__ = ["register_init_command", "schema_api"]
