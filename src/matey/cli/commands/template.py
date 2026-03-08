from __future__ import annotations

from cyclopts import App

from matey.cli.template import TemplateProvider, render_ci_template, render_config_template

from ..render import Renderer
from .common import CliUsageError, OverwriteOpt, PathOpt, emit_template


def register_template_commands(*, template_app: App, renderer: Renderer) -> None:
    @template_app.command(name="config", sort_key=10)
    def template_config_command(*targets: str, path: PathOpt = None, overwrite: OverwriteOpt = False) -> None:
        """Render matey config template."""
        try:
            content = render_config_template(tuple(targets))
        except ValueError as error:
            raise CliUsageError(str(error)) from error
        emit_template(content=content, path=path, overwrite=overwrite, renderer=renderer)

    @template_app.command(name="ci", sort_key=20)
    def template_ci_command(
        provider: TemplateProvider,
        path: PathOpt = None,
        overwrite: OverwriteOpt = False,
    ) -> None:
        """Render CI template."""
        content = render_ci_template(provider)
        emit_template(content=content, path=path, overwrite=overwrite, renderer=renderer)


__all__ = ["register_template_commands"]
