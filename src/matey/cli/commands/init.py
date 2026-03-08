from __future__ import annotations

from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

import matey.schema as schema_api
from matey.cli.template import (
    TemplateProvider,
    default_ci_template_path,
    default_target_config_values,
    render_ci_template,
    render_target_config,
    render_workspace_config,
    target_config_path,
    update_target_config_text,
    update_workspace_text,
    workspace_default_path,
    write_text_file,
)
from matey.config import (
    ConfigError,
    TargetConfig,
    load_target,
    load_workspace,
    normalize_target_path_ref,
)
from matey.paths import (
    PathBoundaryError,
    RelativePathError,
    describe_path_boundary_error,
    safe_descendant,
)

from ..render import Renderer
from .common import (
    CliUsageError,
    EngineOpt,
    ForceOpt,
    PathOpt,
    resolve_workspace_root,
)
from .common import (
    WorkspaceOpt as WorkspacePathOpt,
)

UrlEnvInitOpt = Annotated[str | None, Parameter(name="--url-env", help="Live database URL environment variable.")]
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
        """Initialize matey workspace config, target config, zero-state artifacts, and optional CI."""
        repo_root, workspace_path = resolve_init_paths(workspace)
        workspace_config = load_workspace(repo_root, config_path=workspace_path if workspace_path.exists() else None)
        existing_workspace_text = workspace_out_text = workspace_path.read_text(encoding="utf-8") if workspace_path.exists() else None

        path_value = _normalized_path(path) if path is not None else _default_init_path(repo_root)

        workspace_rendered, workspace_out_path = _workspace_output(
            repo_root=repo_root,
            workspace_config=workspace_config,
            workspace_path=workspace_path,
            existing_text=workspace_out_text,
            path_value=path_value,
        )

        target_rendered = None
        target_config_path = None
        init_plan = None
        target_root = _target_root(repo_root=repo_root, path_value=path_value)
        current_target = _load_existing_target(target_root, repo_root=repo_root)
        resolved_engine, resolved_url_env, resolved_test_url_env = _target_values(
            path_value=path_value,
            current=current_target,
            engine=engine,
            url_env=url_env,
            test_url_env=test_url_env,
        )
        target_config_obj = TargetConfig(
            name=path_value,
            dir=target_root,
            engine=resolved_engine,
            url_env=resolved_url_env,
            test_url_env=resolved_test_url_env,
            codegen=current_target.codegen if current_target is not None else None,
        )
        target_config_path = target_config_path_for_root(target_root)
        existing_target_text = target_config_path.read_text(encoding="utf-8") if target_config_path.exists() else None
        target_rendered = (
            render_target_config(
                engine=resolved_engine,
                url_env=resolved_url_env,
                test_url_env=resolved_test_url_env,
                codegen=target_config_obj.codegen,
            )
            if existing_target_text is None
            else update_target_config_text(
                existing_text=existing_target_text,
                engine=resolved_engine,
                url_env=resolved_url_env,
                test_url_env=resolved_test_url_env,
                codegen=target_config_obj.codegen,
            )
        )
        init_plan = schema_api.prepare_init_target(
            target_config_obj,
            engine=resolved_engine,
            force=force,
        )

        if workspace_rendered is not None and (
            existing_workspace_text is None or existing_workspace_text != workspace_rendered
        ):
            write_text_file(workspace_out_path, workspace_rendered, overwrite=True)
            renderer.template_written(str(workspace_out_path))

        if target_rendered is not None and target_config_path is not None and (
            existing_target_text is None or existing_target_text != target_rendered
        ):
            write_text_file(target_config_path, target_rendered, overwrite=True)
            renderer.template_written(str(target_config_path))

        if ci is not None:
            ci_path = repo_root / default_ci_template_path(ci)
            write_text_file(ci_path, render_ci_template(ci), overwrite=force)
            renderer.template_written(str(ci_path))

        if init_plan is not None:
            renderer.init_target(schema_api.apply_init_target(init_plan))


def resolve_init_paths(workspace_path: Path | None) -> tuple[Path, Path]:
    repo_root = resolve_workspace_root(workspace_path, allow_create_fallback=True)
    for candidate in (repo_root / "matey.toml", repo_root / "pyproject.toml"):
        if candidate.exists():
            return repo_root, candidate
    return repo_root, workspace_default_path(repo_root)


def _workspace_output(
    *,
    repo_root: Path,
    workspace_config,
    workspace_path: Path,
    existing_text: str | None,
    path_value: str | None,
) -> tuple[str | None, Path]:
    if workspace_config.source_kind == "workspace":
        out_path = workspace_config.source_path or workspace_default_path(repo_root)
    elif workspace_config.source_kind == "pyproject":
        out_path = workspace_config.source_path or (repo_root / "pyproject.toml")
    else:
        out_path = workspace_path

    if out_path != workspace_path:
        existing_text = out_path.read_text(encoding="utf-8") if out_path.exists() else None
    use_pyproject = workspace_config.source_kind == "pyproject" or (
        workspace_config.source_kind == "none" and out_path.name == "pyproject.toml"
    )
    if existing_text is None:
        rendered = render_workspace_config(
            (path_value,) if path_value is not None else (),
            pyproject=use_pyproject,
        )
    else:
        rendered = update_workspace_text(
            existing_text=existing_text,
            target_path=path_value,
            pyproject=use_pyproject,
        )
    return rendered, out_path


def _normalized_path(path: str) -> str:
    try:
        return normalize_target_path_ref(path, label="target path")
    except RelativePathError as error:
        raise CliUsageError(str(error)) from error


def _default_init_path(repo_root: Path) -> str:
    cwd = Path.cwd().resolve()
    try:
        relative = cwd.relative_to(repo_root)
    except ValueError as error:
        raise CliUsageError("Current directory must be inside the selected workspace root.") from error
    return relative.as_posix() or "."


def _target_root(*, repo_root: Path, path_value: str) -> Path:
    candidate = repo_root / Path(path_value)
    try:
        return safe_descendant(
            root=repo_root,
            candidate=candidate,
            label="target path",
            allow_missing_leaf=True,
            expected_kind="dir",
        )
    except PathBoundaryError as error:
        raise CliUsageError(
            describe_path_boundary_error(
                error,
                path=candidate,
                symlink_message="target path uses symlinked path segment",
            )
        ) from error


def _load_existing_target(target_root: Path, *, repo_root: Path) -> TargetConfig | None:
    try:
        return load_target(path=target_root, repo_root=repo_root)
    except ConfigError as error:
        if target_config_path_for_root(target_root).exists():
            raise CliUsageError(str(error)) from error
        return None


def _target_values(
    *,
    path_value: str,
    current: TargetConfig | None,
    engine: str | None,
    url_env: str | None,
    test_url_env: str | None,
) -> tuple[str, str, str]:
    default_url_env, default_test_url_env = default_target_config_values(path_value)
    resolved_engine = engine or (current.engine if current is not None else None)
    resolved_url_env = url_env or (current.url_env if current is not None else default_url_env)
    resolved_test_url_env = test_url_env or (current.test_url_env if current is not None else default_test_url_env)
    if resolved_engine is None:
        raise CliUsageError("--engine is required when creating a new target config.")
    return resolved_engine, resolved_url_env, resolved_test_url_env


def target_config_path_for_root(target_root: Path) -> Path:
    return target_config_path(target_root)


__all__ = ["register_init_command", "schema_api"]
