from __future__ import annotations

import subprocess
from typing import Annotated

import typer

from matey.adapters.dbmate import resolve_dbmate_binary
from matey.cli.common import (
    build_execution_context,
    require_real_url,
    resolve_target_execution,
    run_db_verb,
)
from matey.cli.help import command_help
from matey.cli.options import get_options
from matey.cli.output import OutputOptions, RichDbmateRenderer
from matey.core import (
    ConfigError,
    LockfileError,
    PathResolutionError,
    SchemaValidationError,
    TargetSelectionError,
    URLResolutionError,
)
from matey.services.db import guarded_mutate_live_db, run_live_db_diff


def _run_guarded_mutation(
    *,
    ctx: typer.Context,
    verb: str,
    down_steps: int | None = None,
) -> None:
    options = get_options(ctx)
    renderer = RichDbmateRenderer(
        options=OutputOptions(verbose=options.verbose, quiet=options.quiet),
    )
    try:
        context = build_execution_context(options)
    except (ConfigError, TargetSelectionError) as error:
        raise typer.BadParameter(str(error)) from error

    failures = 0
    for selected_target in context.selected_targets:
        try:
            paths, real_url, _test_url = resolve_target_execution(
                context=context,
                options=options,
                target=selected_target,
                require_real_url=True,
            )
            result = guarded_mutate_live_db(
                target_name=selected_target.name,
                dbmate_binary=context.dbmate_binary,
                paths=paths,
                live_url=require_real_url(real_url),
                verb=verb,
                down_steps=down_steps,
                on_dbmate_result=renderer.handle,
            )
        except (
            PathResolutionError,
            URLResolutionError,
            ConfigError,
            SchemaValidationError,
            LockfileError,
        ) as error:
            typer.secho(f"[matey] target={selected_target.name} db {verb} error: {error}", fg="red")
            failures += 1
            continue
        except Exception as error:
            typer.secho(
                f"[matey] target={selected_target.name} unexpected db {verb} error: {error}",
                fg="red",
            )
            failures += 1
            continue

        if result.precheck_diff_text:
            typer.echo(f"=== live precheck drift [target={selected_target.name}] ===")
            typer.echo(result.precheck_diff_text.rstrip())
        if result.postcheck_diff_text:
            typer.echo(f"=== live postcheck drift [target={selected_target.name}] ===")
            typer.echo(result.postcheck_diff_text.rstrip())
        if result.error:
            typer.secho(f"[matey] target={selected_target.name}: {result.error}", fg="red")
            failures += 1
            continue
        if not options.quiet:
            typer.secho(f"[matey] target={selected_target.name}: db {verb} succeeded.", fg="green")

    if failures:
        raise typer.Exit(1)


def register(db_app: typer.Typer) -> None:
    @db_app.command("new", help=command_help("db", "new"))
    def new_migration(ctx: typer.Context, name: Annotated[str, typer.Argument(...)]) -> None:
        run_db_verb(ctx, verb="new", extra_args=[name])

    @db_app.command("create", help=command_help("db", "create"))
    def create(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="create")

    @db_app.command("wait", help=command_help("db", "wait"))
    def wait(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="wait")

    @db_app.command("up", help=command_help("db", "up"))
    def up(ctx: typer.Context) -> None:
        _run_guarded_mutation(ctx=ctx, verb="up")

    @db_app.command("migrate", help=command_help("db", "migrate"))
    def migrate(ctx: typer.Context) -> None:
        _run_guarded_mutation(ctx=ctx, verb="migrate")

    @db_app.command("status", help=command_help("db", "status"))
    def status(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="status")

    @db_app.command("diff", help=command_help("db", "diff"))
    def db_diff(ctx: typer.Context) -> None:
        options = get_options(ctx)
        renderer = RichDbmateRenderer(
            options=OutputOptions(verbose=options.verbose, quiet=options.quiet),
        )
        try:
            context = build_execution_context(options)
        except (ConfigError, TargetSelectionError) as error:
            raise typer.BadParameter(str(error)) from error

        failures = 0
        for selected_target in context.selected_targets:
            try:
                paths, real_url, _test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=selected_target,
                    require_real_url=True,
                )
                result = run_live_db_diff(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    live_url=require_real_url(real_url),
                    on_dbmate_result=renderer.handle,
                )
            except (
                PathResolutionError,
                URLResolutionError,
                ConfigError,
                SchemaValidationError,
                LockfileError,
            ) as error:
                typer.secho(f"[matey] target={selected_target.name} db diff error: {error}", fg="red")
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name} unexpected db diff error: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if result.diff_text:
                typer.echo(f"=== expected(lock) vs live [target={selected_target.name}] ===")
                typer.echo(result.diff_text.rstrip())
            if result.error:
                typer.secho(f"[matey] target={selected_target.name}: {result.error}", fg="red")
                failures += 1
                continue
            if not result.success:
                failures += 1
            elif not options.quiet:
                typer.secho(f"[matey] target={selected_target.name}: no schema differences found.", fg="green")

        if failures:
            raise typer.Exit(1)

    @db_app.command("load", help=command_help("db", "load"))
    def load(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="load")

    @db_app.command("dump", help=command_help("db", "dump"))
    def dump(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="dump")

    @db_app.command("down", help=command_help("db", "down"))
    def down(ctx: typer.Context, steps: Annotated[int | None, typer.Argument()] = None) -> None:
        if steps is not None and steps <= 0:
            raise typer.BadParameter("Down step count must be a positive integer.")
        _run_guarded_mutation(ctx=ctx, verb="rollback", down_steps=steps)

    @db_app.command("drop", help=command_help("db", "drop"))
    def drop(ctx: typer.Context) -> None:
        run_db_verb(ctx, verb="drop")

    @db_app.command(
        "dbmate",
        context_settings={
            "allow_extra_args": True,
            "ignore_unknown_options": True,
            "help_option_names": [],
        },
        help=command_help("db", "dbmate"),
    )
    def dbmate_passthrough(ctx: typer.Context) -> None:
        options = get_options(ctx)
        binary = resolve_dbmate_binary(options.dbmate_bin)
        command = [str(binary), *ctx.args]
        result = subprocess.run(command, check=False)
        raise typer.Exit(result.returncode)
