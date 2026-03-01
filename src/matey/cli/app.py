from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import typer

from matey.app.ci_engine import CiTemplateEngine
from matey.app.config_engine import (
    ConfigTemplateEngine,
    ResolvedConfig,
    TargetRuntime,
    build_target_runtime,
    load_effective_config,
    select_target_names,
)
from matey.app.db_engine import DbEngine
from matey.app.kernel import build_context
from matey.app.schema_engine import SchemaEngine
from matey.cli.groups.db import register_db_group
from matey.cli.groups.schema import register_schema_group
from matey.cli.groups.template import register_template_group
from matey.cli.help import group_meta, root_help_text
from matey.cli.options import RootOptions
from matey.cli.presenter import CliPresenter
from matey.domain.config import ConfigDefaults, ResolvedTargetConfig
from matey.domain.errors import CliUsageError


@dataclass
class CliState:
    options: RootOptions
    schema_engine: SchemaEngine
    db_engine: DbEngine
    presenter: CliPresenter
    repo_root: Path
    _resolved_config: ResolvedConfig | None = None


app = typer.Typer(
    help=root_help_text(),
    pretty_exceptions_enable=False,
    pretty_exceptions_show_locals=False,
)

TARGET_OPT = typer.Option(None, "--target", help="Select target name.")
ALL_OPT = typer.Option(False, "--all", help="Run command for all configured targets.")
CONFIG_OPT = typer.Option(None, "--config", help="Path to matey.toml config.")
DIR_OPT = typer.Option(None, "--dir", help="Database directory root override.")
BASE_OPT = typer.Option(None, "--base", help="Base branch for replay checks.")
URL_OPT = typer.Option(None, "--url", help="Live database URL override.")
TEST_URL_OPT = typer.Option(None, "--test-url", help="Scratch database URL override.")
KEEP_SCRATCH_OPT = typer.Option(False, "--keep-scratch", help="Keep scratch targets after command.")
DBMATE_BIN_OPT = typer.Option(None, "--dbmate-bin", help="Path to dbmate binary.")
VERBOSE_OPT = typer.Option(False, "--verbose", help="Enable verbose output.")
QUIET_OPT = typer.Option(False, "--quiet", help="Reduce output.")


@app.callback()
def main(
    ctx: typer.Context,
    target: str | None = TARGET_OPT,
    all_targets: bool = ALL_OPT,
    config: Path | None = CONFIG_OPT,
    directory: Path | None = DIR_OPT,
    base: str | None = BASE_OPT,
    url: str | None = URL_OPT,
    test_url: str | None = TEST_URL_OPT,
    keep_scratch: bool = KEEP_SCRATCH_OPT,
    dbmate_bin: Path | None = DBMATE_BIN_OPT,
    verbose: bool = VERBOSE_OPT,
    quiet: bool = QUIET_OPT,
) -> None:
    context = build_context(cwd=Path.cwd(), dbmate_bin=dbmate_bin)
    schema_engine = SchemaEngine(context=context)
    db_engine = DbEngine(context=context, schema_engine=schema_engine)
    state = CliState(
        options=RootOptions(
            target=target,
            all_targets=all_targets,
            config_path=config,
            dir_override=directory,
            base_ref=base,
            url=url,
            test_url=test_url,
            keep_scratch=keep_scratch,
            verbose=verbose,
            quiet=quiet,
        ),
        schema_engine=schema_engine,
        db_engine=db_engine,
        presenter=CliPresenter(),
        repo_root=context.git.repo_root(),
    )
    ctx.obj = state


def _state(ctx: typer.Context) -> CliState:
    root_ctx = ctx.find_root()
    state = root_ctx.obj
    if not isinstance(state, CliState):
        raise CliUsageError("CLI state is not initialized.")
    return state


def _options(ctx: typer.Context) -> RootOptions:
    return _state(ctx).options


def _schema_engine(ctx: typer.Context) -> SchemaEngine:
    return _state(ctx).schema_engine


def _db_engine(ctx: typer.Context) -> DbEngine:
    return _state(ctx).db_engine


def _presenter(ctx: typer.Context) -> CliPresenter:
    return _state(ctx).presenter


def _resolved_config(state: CliState) -> ResolvedConfig:
    if state._resolved_config is None:
        state._resolved_config = load_effective_config(
            repo_root=state.repo_root,
            config_path=state.options.config_path,
        )
    return state._resolved_config


def _resolve_targets(ctx: typer.Context) -> tuple[tuple[str, TargetRuntime, ConfigDefaults], ...]:
    state = _state(ctx)
    opts = state.options
    resolved = _resolved_config(state)
    names = select_target_names(config=resolved, target=opts.target, select_all=opts.all_targets)

    if opts.dir_override is not None:
        if len(names) != 1:
            raise CliUsageError("--dir requires selecting exactly one target.")
        source = resolved.targets[names[0]]
        override = ResolvedTargetConfig(
            name=source.name,
            db_dir=opts.dir_override.resolve(),
            url_env=source.url_env,
            test_url_env=source.test_url_env,
        )
        runtime = build_target_runtime(resolved=override)
        return ((names[0], runtime, resolved.defaults),)

    rows: list[tuple[str, TargetRuntime, ConfigDefaults]] = []
    for name in names:
        runtime = build_target_runtime(resolved=resolved.targets[name])
        rows.append((name, runtime, resolved.defaults))
    return tuple(rows)


register_db_group(
    parent=app,
    db_engine_for_ctx=_db_engine,
    resolve_targets=_resolve_targets,
    options_for_ctx=_options,
    group_meta=group_meta("db"),
    presenter_for_ctx=_presenter,
)
register_schema_group(
    parent=app,
    schema_engine_for_ctx=_schema_engine,
    resolve_targets=_resolve_targets,
    options_for_ctx=_options,
    group_meta=group_meta("schema"),
    presenter_for_ctx=_presenter,
)
register_template_group(
    parent=app,
    config_engine=ConfigTemplateEngine(),
    ci_engine=CiTemplateEngine(),
    group_meta=group_meta("template"),
)
