from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Annotated

import typer

from matey import __version__
from matey.cli.output import OutputOptions, RichDbmateRenderer
from matey.domain import (
    ConfigError,
    LockfileError,
    MateyConfig,
    PathResolutionError,
    ResolvedPaths,
    SelectedTarget,
    TargetSelectionError,
    URLResolutionError,
)
from matey.drivers.dbmate import DbmateLogContext, resolve_dbmate_binary, run_dbmate
from matey.drivers.scratch import detect_engine
from matey.settings.config import load_config
from matey.settings.resolve import derive_paths, resolve_real_url, resolve_test_url, select_targets
from matey.workflows.lockfile import load_schema_lock, lockfile_path
from matey.workflows.schema import read_schema_sql


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
        typer.Option("--all", help="Run command for all configured targets.", rich_help_panel="Targeting"),
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
        typer.Option("--keep-scratch", help="Keep scratch DB/dataset after command.", rich_help_panel="Scratch"),
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


def _validate_url_override(options: GlobalOptions, *, target_count: int) -> None:
    if options.url_override and target_count != 1:
        raise TargetSelectionError("--url is only allowed when a single target is selected.")


def build_execution_context(options: GlobalOptions) -> ExecutionContext:
    config = load_config(options.config_path)
    selected_targets = select_targets(
        config,
        target_name=options.target_name,
        all_targets=options.all_targets,
    )
    _validate_url_override(options, target_count=len(selected_targets))
    dbmate_binary = resolve_dbmate_binary(options.dbmate_bin)
    return ExecutionContext(
        config=config,
        selected_targets=selected_targets,
        dbmate_binary=dbmate_binary,
    )


def resolve_target_execution(
    *,
    context: ExecutionContext,
    options: GlobalOptions,
    target: SelectedTarget,
    require_real_url: bool,
) -> tuple[ResolvedPaths, str | None, str | None]:
    paths = derive_paths(
        context.config,
        target,
        dir_override=options.dir_override,
    )

    real_url: str | None
    if require_real_url:
        real_url = resolve_real_url(
            context.config,
            target,
            cli_url=options.url_override,
        )
    else:
        try:
            real_url = resolve_real_url(
                context.config,
                target,
                cli_url=options.url_override,
            )
        except URLResolutionError:
            real_url = None

    test_url = resolve_test_url(
        context.config,
        target,
        cli_test_url=options.test_url_override,
    )
    return paths, real_url, test_url


def require_real_url(real_url: str | None) -> str:
    if real_url is None:
        raise URLResolutionError("Missing database URL after target resolution.")
    return real_url


def write_schema_file(schema_file: Path, schema_sql: str) -> bool:
    normalized_schema = schema_sql
    previous = read_schema_sql(schema_file) if schema_file.exists() else ""
    if previous == normalized_schema:
        return False
    schema_file.parent.mkdir(parents=True, exist_ok=True)
    schema_file.write_text(normalized_schema, encoding="utf-8")
    return True


def config_output_path(options: GlobalOptions) -> Path:
    if options.config_path is not None:
        return options.config_path
    return Path("matey.toml")


def resolve_lock_engine_for_sync(
    *,
    paths: ResolvedPaths,
    real_url: str | None,
    test_url: str | None,
) -> str:
    lock_path = lockfile_path(paths)
    if lock_path.exists():
        lock_engine = load_schema_lock(lock_path).engine
        url = test_url or real_url
        if url is not None:
            detected = detect_engine(url)
            if detected != lock_engine:
                raise LockfileError(
                    "Engine mismatch between existing lockfile and provided URL "
                    f"({lock_engine} != {detected})."
                )
        return lock_engine

    url = test_url or real_url
    if url:
        return detect_engine(url)
    raise LockfileError(
        "Unable to infer engine for lock sync. Provide --test-url/--url or create a lockfile first."
    )


def run_db_verb(ctx: typer.Context, *, verb: str, extra_args: list[str] | None = None) -> None:
    options = get_options(ctx)
    args = extra_args or []
    renderer = RichDbmateRenderer(
        options=OutputOptions(verbose=options.verbose, quiet=options.quiet),
    )

    try:
        context = build_execution_context(options)
        exit_codes: list[int] = []
        for target in context.selected_targets:
            paths, real_url, _ = resolve_target_execution(
                context=context,
                options=options,
                target=target,
                require_real_url=True,
            )
            exit_code = run_dbmate(
                dbmate_binary=context.dbmate_binary,
                url=require_real_url(real_url),
                migrations_dir=paths.migrations_dir,
                schema_file=paths.schema_file,
                verb=verb,
                extra_args=args,
                log_context=DbmateLogContext(
                    target=target.name,
                    phase="direct",
                    step=verb,
                ),
                on_result=renderer.handle,
            )
            exit_codes.append(exit_code)
    except (ConfigError, TargetSelectionError, PathResolutionError, URLResolutionError) as error:
        raise typer.BadParameter(str(error)) from error

    if any(code != 0 for code in exit_codes):
        raise typer.Exit(1)
