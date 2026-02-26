from __future__ import annotations

from typing import Annotated

import typer

from matey.cli.common import run_db_verb


def register(app: typer.Typer) -> None:
    @app.command("new", help="Create a new migration file.", rich_help_panel="Database Commands")
    def new_migration(ctx: typer.Context, name: Annotated[str, typer.Argument(...)]) -> None:
        run_db_verb(ctx, verb="new", extra_args=[name])

    @app.command("up", help="Apply pending migrations.", rich_help_panel="Database Commands")
    def up(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="up")

    @app.command("migrate", help="Apply all migrations.", rich_help_panel="Database Commands")
    def migrate(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="migrate")

    @app.command("rollback", help="Roll back last migration (or N).", rich_help_panel="Database Commands")
    def rollback(ctx: typer.Context, steps: Annotated[int | None, typer.Argument()] = None) -> None:
        args = [str(steps)] if steps is not None else []
        run_db_verb(ctx, verb="rollback", extra_args=args)

    @app.command("down", help="Alias for rollback.", rich_help_panel="Database Commands")
    def down(ctx: typer.Context, steps: Annotated[int | None, typer.Argument()] = None) -> None:
        args = [str(steps)] if steps is not None else []
        run_db_verb(ctx, verb="rollback", extra_args=args)

    @app.command("status", help="Show migration status.", rich_help_panel="Database Commands")
    def status(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="status")

    @app.command("dump", help="Dump schema to stdout.", rich_help_panel="Database Commands")
    def dump(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="dump")

    @app.command("load", help="Load schema from schema file.", rich_help_panel="Database Commands")
    def load(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="load")

    @app.command("create", help="Create database/dataset where supported.", rich_help_panel="Database Commands")
    def create(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="create")

    @app.command("drop", help="Drop database/dataset where supported.", rich_help_panel="Database Commands")
    def drop(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="drop")

    @app.command("wait", help="Wait for database to become available.", rich_help_panel="Database Commands")
    def wait(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="wait")
