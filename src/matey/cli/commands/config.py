from __future__ import annotations

from typing import Annotated

import typer

from matey.cli.common import config_output_path, get_options
from matey.templates import parse_target_list, render_config_template


def register(config_app: typer.Typer) -> None:
    @config_app.command("print", help="Print matey.toml skeleton to stdout.")
    def config_print(
        targets: Annotated[
            str | None,
            typer.Option("--targets", help="Comma-separated target names (for example: core,analytics)."),
        ] = None,
    ) -> None:
        try:
            parsed_targets = parse_target_list(targets)
            config_text = render_config_template(targets=parsed_targets)
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error

        typer.echo(config_text.rstrip())

    @config_app.command("init", help="Write matey.toml skeleton.")
    def config_init(
        ctx: typer.Context,
        targets: Annotated[
            str | None,
            typer.Option("--targets", help="Comma-separated target names (for example: core,analytics)."),
        ] = None,
        force: Annotated[
            bool,
            typer.Option("--force", help="Overwrite existing matey.toml."),
        ] = False,
    ) -> None:
        options = get_options(ctx)
        try:
            parsed_targets = parse_target_list(targets)
            config_text = render_config_template(targets=parsed_targets)
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error

        config_path = config_output_path(options)
        if config_path.exists() and not force:
            typer.secho(
                f"[matey] refusing to overwrite existing file: {config_path} (use --force)",
                fg="red",
            )
            raise typer.Exit(1)

        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_text(config_text, encoding="utf-8")
        if not options.quiet:
            typer.secho(f"[matey] wrote {config_path}", fg="green")
