from __future__ import annotations

from typing import Annotated

import typer

from matey.cli.common import get_options
from matey.templates import CIProvider, parse_target_list, render_cd_template
from matey.templates.types import TemplateFile


def register(cd_app: typer.Typer) -> None:
    @cd_app.command("print", help="Print provider CD template to stdout.")
    def cd_print(
        provider: Annotated[CIProvider, typer.Argument(help="CD provider: github, gitlab, buildkite.")],
        targets: Annotated[
            str | None,
            typer.Option("--targets", help="Comma-separated target names (for example: core,analytics)."),
        ] = None,
    ) -> None:
        try:
            parsed_targets = parse_target_list(targets)
            rendered = render_cd_template(provider, targets=parsed_targets)
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error

        typer.echo(rendered.content.rstrip())

    @cd_app.command("init", help="Write provider CD template.")
    def cd_init(
        ctx: typer.Context,
        provider: Annotated[CIProvider, typer.Argument(help="CD provider: github, gitlab, buildkite.")],
        force: Annotated[
            bool,
            typer.Option("--force", help="Overwrite existing files."),
        ] = False,
        targets: Annotated[
            str | None,
            typer.Option("--targets", help="Comma-separated target names (for example: core,analytics)."),
        ] = None,
    ) -> None:
        options = get_options(ctx)
        try:
            parsed_targets = parse_target_list(targets)
            rendered_files: list[TemplateFile] = [render_cd_template(provider, targets=parsed_targets)]
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error

        for rendered_file in rendered_files:
            if rendered_file.path.exists() and not force:
                typer.secho(
                    f"[matey] refusing to overwrite existing file: {rendered_file.path} (use --force)",
                    fg="red",
                )
                raise typer.Exit(1)
            rendered_file.path.parent.mkdir(parents=True, exist_ok=True)
            rendered_file.path.write_text(rendered_file.content, encoding="utf-8")
            if not options.quiet:
                typer.secho(f"[matey] wrote {rendered_file.path}", fg="green")
