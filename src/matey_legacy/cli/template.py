from __future__ import annotations

from typing import Annotated

import typer

from matey.cli.common import config_output_path
from matey.cli.help import command_help
from matey.cli.options import get_options
from matey.services.template import render_ci, render_config, write_template_file
from matey.templates import CIProvider
from matey.templates.types import TemplateFile


def _write_or_fail(
    *,
    rendered: TemplateFile,
    force: bool,
    quiet: bool,
) -> None:
    try:
        path = write_template_file(rendered=rendered, force=force)
    except FileExistsError:
        typer.secho(
            f"[matey] refusing to overwrite existing file: {rendered.path} (use --force)",
            fg="red",
        )
        raise typer.Exit(1) from None
    if not quiet:
        typer.secho(f"[matey] wrote {path}", fg="green")


def register_ci(ci_app: typer.Typer) -> None:
    @ci_app.command("print", help=command_help("ci", "print"))
    def ci_print(
        provider: Annotated[CIProvider, typer.Argument(help="CI provider: github, gitlab, buildkite.")],
        targets: Annotated[
            str | None,
            typer.Option("--targets", help="Comma-separated target names (for example: core,analytics)."),
        ] = None,
    ) -> None:
        try:
            rendered = render_ci(provider=provider, targets=targets)
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error
        typer.echo(rendered.content.rstrip())

    @ci_app.command("init", help=command_help("ci", "init"))
    def ci_init(
        ctx: typer.Context,
        provider: Annotated[CIProvider, typer.Argument(help="CI provider: github, gitlab, buildkite.")],
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
            rendered = render_ci(provider=provider, targets=targets)
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error
        _write_or_fail(rendered=rendered, force=force, quiet=options.quiet)


def register_config(config_app: typer.Typer) -> None:
    @config_app.command("print", help=command_help("config", "print"))
    def config_print(
        targets: Annotated[
            str | None,
            typer.Option("--targets", help="Comma-separated target names (for example: core,analytics)."),
        ] = None,
    ) -> None:
        try:
            config_text = render_config(targets=targets)
        except ValueError as error:
            raise typer.BadParameter(str(error)) from error
        typer.echo(config_text.rstrip())

    @config_app.command("init", help=command_help("config", "init"))
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
            config_text = render_config(targets=targets)
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
