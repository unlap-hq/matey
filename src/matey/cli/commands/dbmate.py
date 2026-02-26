from __future__ import annotations

import subprocess

import typer

from matey.cli.common import get_options
from matey.drivers.dbmate import resolve_dbmate_binary


def register(app: typer.Typer) -> None:
    @app.command(
        "dbmate",
        context_settings={
            "allow_extra_args": True,
            "ignore_unknown_options": True,
            "help_option_names": [],
        },
        help="Run bundled dbmate directly.",
        rich_help_panel="Database Commands",
    )
    def dbmate_passthrough(ctx: typer.Context) -> None:
        options = get_options(ctx)
        binary = resolve_dbmate_binary(options.dbmate_bin)
        command = [str(binary), *ctx.args]
        result = subprocess.run(command, check=False)
        raise typer.Exit(result.returncode)
