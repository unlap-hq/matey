from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from matey import __version__
from matey.core import MateyConfig, SelectedTarget


@dataclass(frozen=True)
class GlobalOptions:
    target_name: str | None
    all_targets: bool
    config_path: Path | None
    dir_override: Path | None
    base_branch: str | None
    url_override: str | None
    test_url_override: str | None
    keep_scratch: bool
    verbose: bool
    quiet: bool
    dbmate_bin: str | None


@dataclass(frozen=True)
class ExecutionContext:
    config: MateyConfig
    selected_targets: list[SelectedTarget]
    dbmate_binary: Path


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


def main_callback(
    ctx: typer.Context,
    target_name: Annotated[
        str | None,
        typer.Option("--target", help="Select target name.", rich_help_panel="Targeting"),
    ] = None,
    all_targets: Annotated[
        bool,
        typer.Option(
            "--all", help="Run command for all configured targets.", rich_help_panel="Targeting"
        ),
    ] = False,
    config_path: Annotated[
        Path | None,
        typer.Option("--config", help="Path to matey.toml config.", rich_help_panel="Config"),
    ] = None,
    dir_override: Annotated[
        Path | None,
        typer.Option("--dir", help="Database directory root override.", rich_help_panel="Config"),
    ] = None,
    base_branch: Annotated[
        str | None,
        typer.Option(
            "--base",
            help="Base branch for lockfile divergence checks.",
            rich_help_panel="Config",
        ),
    ] = None,
    url_override: Annotated[
        str | None,
        typer.Option(
            "--url",
            help="Override selected target database URL for this run.",
            rich_help_panel="URL Overrides",
        ),
    ] = None,
    test_url_override: Annotated[
        str | None,
        typer.Option(
            "--test-url",
            help="Override selected target scratch URL for this run.",
            rich_help_panel="URL Overrides",
        ),
    ] = None,
    keep_scratch: Annotated[
        bool,
        typer.Option(
            "--keep-scratch",
            help="Keep scratch DB/dataset after command.",
            rich_help_panel="Scratch",
        ),
    ] = False,
    dbmate_bin: Annotated[
        str | None,
        typer.Option(
            "--dbmate-bin",
            help="Path to dbmate binary. Overrides MATEY_DBMATE_BIN and bundled binary.",
            rich_help_panel="Execution",
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", help="Enable verbose output.", rich_help_panel="Output"),
    ] = False,
    quiet: Annotated[
        bool,
        typer.Option("--quiet", help="Reduce output.", rich_help_panel="Output"),
    ] = False,
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            callback=_version_callback,
            is_eager=True,
            help="Show version and exit.",
            rich_help_panel="Output",
        ),
    ] = False,
) -> None:
    del version
    if verbose and quiet:
        raise typer.BadParameter("--verbose and --quiet cannot be used together.")

    ctx.obj = GlobalOptions(
        target_name=target_name,
        all_targets=all_targets,
        config_path=config_path,
        dir_override=dir_override,
        base_branch=base_branch,
        url_override=url_override,
        test_url_override=test_url_override,
        keep_scratch=keep_scratch,
        verbose=verbose,
        quiet=quiet,
        dbmate_bin=dbmate_bin,
    )


def get_options(ctx: typer.Context) -> GlobalOptions:
    options = ctx.obj
    if not isinstance(options, GlobalOptions):
        raise typer.BadParameter("Internal error: CLI context is not initialized.")
    return options
