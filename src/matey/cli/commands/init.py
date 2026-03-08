from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

import matey.schema as schema_api
from matey.cli.ci import (
    TemplateProvider,
    default_ci_template_path,
    render_ci_template,
    write_text_file,
)
from matey.paths import normalize_target_path_ref, safe_descendant
from matey.project import (
    DEFAULT_CODEGEN,
    ConfigEditor,
    ConfigError,
    TargetConfig,
    Workspace,
    default_target_config_values,
)

from ..render import Renderer
from .common import EngineOpt, ForceOpt, PathOpt
from .common import WorkspaceOpt as WorkspacePathOpt

UrlEnvInitOpt = Annotated[
    str | None, Parameter(name="--url-env", help="Live database URL environment variable.")
]
TestUrlEnvInitOpt = Annotated[
    str | None,
    Parameter(name="--test-url-env", help="Scratch database URL environment variable."),
]
CiOpt = Annotated[
    TemplateProvider | None,
    Parameter(name="--ci", help="Write a CI template for the selected provider."),
]


def register_init_command(*, root_app: App, renderer: Renderer) -> None:
    @root_app.command(name="init", sort_key=10)
    def init_command(
        workspace: WorkspacePathOpt = None,
        path: PathOpt = None,
        engine: EngineOpt = None,
        url_env: UrlEnvInitOpt = None,
        test_url_env: TestUrlEnvInitOpt = None,
        ci: CiOpt = None,
        force: ForceOpt = False,
    ) -> None:
        """Initialize workspace config, target config, zero-state artifacts, and optional CI."""
        workspace_obj = Workspace.discover(
            start=Path.cwd().resolve(),
            workspace=workspace,
            allow_create_fallback=True,
        )
        existing_workspace_text = (
            workspace_obj.config_path.read_text(encoding="utf-8")
            if workspace_obj.config_path.exists()
            else None
        )

        if path is not None:
            path_value = normalize_target_path_ref(path, label="target path")
        else:
            cwd = Path.cwd().resolve()
            if not cwd.is_relative_to(workspace_obj.root):
                raise ConfigError("Current directory must be inside the selected workspace root.")
            path_value = cwd.relative_to(workspace_obj.root).as_posix() or "."
        target_root = safe_descendant(
            root=workspace_obj.root,
            candidate=workspace_obj.root / Path(path_value),
            label="target path",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
        current_target = (
            TargetConfig.load(path=target_root, workspace_root=workspace_obj.root)
            if target_root.joinpath("config.toml").exists()
            else None
        )
        default_url_env, default_test_url_env = default_target_config_values(path_value)
        resolved_engine = engine or (current_target.engine if current_target is not None else None)
        resolved_url_env = url_env or (
            current_target.url_env if current_target is not None else default_url_env
        )
        resolved_test_url_env = test_url_env or (
            current_target.test_url_env if current_target is not None else default_test_url_env
        )
        if resolved_engine is None:
            raise ConfigError("--engine is required when creating a new target config.")
        target = TargetConfig(
            name=path_value,
            root=target_root,
            engine=resolved_engine,
            url_env=resolved_url_env,
            test_url_env=resolved_test_url_env,
            codegen=(
                current_target.codegen
                if current_target is not None and current_target.codegen is not None
                else DEFAULT_CODEGEN
            ),
        )

        existing_target_text = (
            target.config_path.read_text(encoding="utf-8") if target.config_path.exists() else None
        )
        editor = ConfigEditor("workspace")
        target_rendered = (
            editor.render_target(
                engine=resolved_engine,
                url_env=resolved_url_env,
                test_url_env=resolved_test_url_env,
                codegen=target.codegen,
            )
            if existing_target_text is None
            else editor.update_target(
                existing_text=existing_target_text,
                engine=resolved_engine,
                url_env=resolved_url_env,
                test_url_env=resolved_test_url_env,
                codegen=target.codegen,
            )
        )
        workspace_rendered = workspace_obj.render_updated(
            target_path=path_value,
            existing_text=existing_workspace_text,
        )
        init_plan = schema_api.prepare_init_target(target, engine=resolved_engine, force=force)

        if existing_workspace_text is None or existing_workspace_text != workspace_rendered:
            write_text_file(workspace_obj.config_path, workspace_rendered, overwrite=True)
            renderer.template_written(str(workspace_obj.config_path))

        if existing_target_text is None or existing_target_text != target_rendered:
            write_text_file(target.config_path, target_rendered, overwrite=True)
            renderer.template_written(str(target.config_path))

        if ci is not None:
            ci_path = workspace_obj.root / default_ci_template_path(ci)
            write_text_file(ci_path, render_ci_template(ci), overwrite=force)
            renderer.template_written(str(ci_path))

        renderer.init_target(schema_api.apply_init_target(init_plan))


__all__ = ["register_init_command", "schema_api"]
