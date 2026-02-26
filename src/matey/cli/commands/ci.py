from __future__ import annotations

from typing import Annotated

import typer

from matey.cli.common import get_options, print_rendered_files
from matey.templates import CIProvider, parse_target_list, render_ci_template
from matey.templates.types import TemplateFile


def register(ci_app: typer.Typer) -> None:
    @ci_app.command("print", help="Print provider CI template to stdout.")
    def ci_print(
        provider: Annotated[CIProvider, typer.Argument(help="CI provider: github, gitlab, buildkite.")],
        targets: Annotated[
            str | None,
            typer.Option("--targets", help="Comma-separated target names (for example: core,analytics)."),
        ] = None,
    ) -> None:
        try:
            parsed_targets = parse_target_list(targets)
            rendered = render_ci_template(provider, targets=parsed_targets)
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error

        typer.echo(rendered.content.rstrip())

    @ci_app.command("init", help="Write provider CI template.")
    def ci_init(
        ctx: typer.Context,
        provider: Annotated[CIProvider, typer.Argument(help="CI provider: github, gitlab, buildkite.")],
        print_mode: Annotated[
            bool,
            typer.Option("--print", help="Print files to stdout instead of writing."),
        ] = False,
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
            rendered_files: list[TemplateFile] = [render_ci_template(provider, targets=parsed_targets)]
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error

        if print_mode:
            print_rendered_files(rendered_files)
            return

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
