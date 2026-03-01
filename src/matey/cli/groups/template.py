from __future__ import annotations

from pathlib import Path

import typer

from matey.app.ci_engine import CiTemplateEngine
from matey.app.config_engine import ConfigTemplateEngine
from matey.cli.help import GroupMeta, command_help

PATH_OPT = typer.Option(
    None,
    "--path",
    resolve_path=True,
    help="Write template to this path; omit to print to stdout.",
)
OVERWRITE_OPT = typer.Option(False, "--overwrite", help="Allow overwriting existing file when --path is set.")


def register_template_group(
    *,
    parent: typer.Typer,
    config_engine: ConfigTemplateEngine,
    ci_engine: CiTemplateEngine,
    group_meta: GroupMeta,
) -> None:
    template_app = typer.Typer(help=group_meta.help)

    @template_app.command("config", help=command_help(group_name="template", command_name="config"))
    def template_config(
        path: Path | None = PATH_OPT,
        overwrite: bool = OVERWRITE_OPT,
    ) -> None:
        if path is None:
            typer.echo(config_engine.render())
            return
        config_engine.write(path=path, overwrite=overwrite)

    @template_app.command("ci", help=command_help(group_name="template", command_name="ci"))
    def template_ci(
        path: Path | None = PATH_OPT,
        overwrite: bool = OVERWRITE_OPT,
    ) -> None:
        if path is None:
            typer.echo(ci_engine.render())
            return
        ci_engine.write(path=path, overwrite=overwrite)

    parent.add_typer(template_app, name="template")
