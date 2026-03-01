from __future__ import annotations

import typer

from matey.cli.common import (
    build_execution_context,
    resolve_lock_engine_for_sync,
    resolve_target_execution,
)
from matey.cli.help import command_help
from matey.cli.options import get_options
from matey.core import (
    ConfigError,
    LockfileError,
    PathResolutionError,
    TargetSelectionError,
    URLResolutionError,
)
from matey.core.lock import lockfile_path
from matey.services.lock import doctor, sync


def register(lock_app: typer.Typer) -> None:
    @lock_app.command("doctor", help=command_help("lock", "doctor"))
    def lock_doctor(ctx: typer.Context) -> None:
        options = get_options(ctx)
        try:
            context = build_execution_context(options)
        except (ConfigError, TargetSelectionError) as error:
            raise typer.BadParameter(str(error)) from error

        failures = 0
        for target in context.selected_targets:
            try:
                paths, _real_url, _test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=target,
                    require_real_url=False,
                )
                lock = doctor(paths=paths)
            except (PathResolutionError, URLResolutionError, LockfileError) as error:
                typer.secho(f"[matey] target={target.name}: lock doctor failed: {error}", fg="red")
                failures += 1
                continue
            if not options.quiet:
                typer.secho(
                    f"[matey] target={target.name}: lock healthy "
                    f"(engine={lock.engine}, steps={lock.head_index}).",
                    fg="green",
                )
        if failures:
            raise typer.Exit(1)

    @lock_app.command("sync", help=command_help("lock", "sync"))
    def lock_sync(ctx: typer.Context) -> None:
        options = get_options(ctx)
        try:
            context = build_execution_context(options)
        except (ConfigError, TargetSelectionError) as error:
            raise typer.BadParameter(str(error)) from error

        failures = 0
        for target in context.selected_targets:
            try:
                paths, real_url, test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=target,
                    require_real_url=False,
                )
                engine = resolve_lock_engine_for_sync(
                    paths=paths,
                    real_url=real_url,
                    test_url=test_url,
                )
                lock = sync(
                    paths=paths,
                    engine=engine,
                    target=target.name,
                )
            except (PathResolutionError, URLResolutionError, LockfileError) as error:
                typer.secho(f"[matey] target={target.name}: lock sync failed: {error}", fg="red")
                failures += 1
                continue
            if not options.quiet:
                typer.secho(
                    f"[matey] target={target.name}: wrote {lockfile_path(paths)} "
                    f"(engine={lock.engine}, steps={lock.head_index}).",
                    fg="green",
                )
        if failures:
            raise typer.Exit(1)
