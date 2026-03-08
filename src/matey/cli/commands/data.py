from __future__ import annotations

from typing import Annotated

from cyclopts import App, Parameter

import matey.data as data_api
from matey.project import TargetConfig

from ..render import Renderer
from .common import PathOpt, UrlOpt, WorkspaceOpt, run_targets

SetOpt = Annotated[str | None, Parameter(name="--set", help="Data set name.")]


def register_data_commands(*, data_app: App, renderer: Renderer) -> None:
    @data_app.command(name="apply", sort_key=10)
    def data_apply_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        url: UrlOpt = None,
        set: SetOpt = None,
    ) -> None:
        """Apply a configured data set to a live database that already matches worktree head schema."""

        def render_target(item: TargetConfig) -> None:
            renderer.data_apply(data_api.apply(target=item, url=url, set_name=set))

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @data_app.command(name="export", sort_key=20)
    def data_export_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        url: UrlOpt = None,
        set: SetOpt = None,
    ) -> None:
        """Export a configured data set from a live database that matches worktree head schema."""

        def render_target(item: TargetConfig) -> None:
            renderer.data_export(data_api.export(target=item, url=url, set_name=set))

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )


__all__ = ["data_api", "register_data_commands"]
