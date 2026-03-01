from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import typer

from matey.config import (
    CiTemplateEngine,
    ConfigTemplateEngine,
    build_target_runtime,
    load_effective_config,
    select_target_names,
)
from matey.db import DbEngine
from matey.errors import CliUsageError
from matey.models import ConfigDefaults, ResolvedConfig, ResolvedTargetConfig, TargetRuntime
from matey.presenter import (
    CliPresenter,
    DbPlanRecord,
    OperationRecord,
    OutputFormat,
    SchemaPlanRecord,
    SchemaStatusRecord,
    TargetText,
)
from matey.runtime import build_context
from matey.schema import SchemaEngine


@dataclass(frozen=True)
class CommandMeta:
    name: str
    help: str


@dataclass(frozen=True)
class GroupMeta:
    name: str
    help: str
    commands: tuple[CommandMeta, ...]
    subgroups: tuple[GroupMeta, ...] = ()


DB_PLAN = GroupMeta(
    name="plan",
    help="Compare live database against expected schema.",
    commands=(
        CommandMeta(name="__summary__", help="Show plan summary."),
        CommandMeta(name="diff", help="Print live-vs-head schema diff."),
        CommandMeta(name="sql", help="Print expected head schema SQL from artifacts (offline)."),
    ),
)
SCHEMA_PLAN = GroupMeta(
    name="plan",
    help="Read-only replay plan and outputs.",
    commands=(
        CommandMeta(name="__summary__", help="Show plan summary."),
        CommandMeta(name="diff", help="Print A vs B replay diff."),
        CommandMeta(name="sql", help="Print planned normalized schema SQL (B)."),
    ),
)
DB_GROUP = GroupMeta(
    name="db",
    help="Live database commands.",
    commands=(
        CommandMeta(name="new", help="Create a new migration file in the target migrations directory."),
        CommandMeta(name="create", help="Create the target database/dataset if missing."),
        CommandMeta(name="wait", help="Wait until the target database is reachable."),
        CommandMeta(name="up", help="Run guarded dbmate up with pre/post schema checks."),
        CommandMeta(name="migrate", help="Run guarded dbmate migrate with pre/post schema checks."),
        CommandMeta(name="status", help="Show live migration status from dbmate."),
        CommandMeta(name="drift", help="Fail if live schema differs from expected schema at current index."),
        CommandMeta(name="plan", help=DB_PLAN.help),
        CommandMeta(name="load", help="Load schema SQL into live database via dbmate."),
        CommandMeta(name="dump", help="Dump live schema SQL via dbmate."),
        CommandMeta(name="down", help="Run guarded rollback of last migration(s) with pre/post checks."),
        CommandMeta(name="drop", help="Drop the target database/dataset."),
        CommandMeta(name="dbmate", help="Pass through raw arguments to dbmate under command scope."),
    ),
    subgroups=(DB_PLAN,),
)
SCHEMA_GROUP = GroupMeta(
    name="schema",
    help="Schema artifact workflows.",
    commands=(
        CommandMeta(name="status", help="Report schema artifact health and staleness."),
        CommandMeta(name="plan", help=SCHEMA_PLAN.help),
        CommandMeta(name="apply", help="Apply schema plan and rewrite artifacts atomically."),
    ),
    subgroups=(SCHEMA_PLAN,),
)
TEMPLATE_GROUP = GroupMeta(
    name="template",
    help="Template helpers.",
    commands=(
        CommandMeta(name="config", help="Print or write starter matey.toml template."),
        CommandMeta(name="ci", help="Print or write starter CI workflow template."),
    ),
)
ROOT_GROUPS: tuple[GroupMeta, ...] = (DB_GROUP, SCHEMA_GROUP, TEMPLATE_GROUP)


def _group_meta(group_name: str) -> GroupMeta:
    for group in ROOT_GROUPS:
        if group.name == group_name:
            return group
    raise KeyError(f"Unknown help group: {group_name}")


def _subgroup_meta(group_name: str, subgroup_name: str) -> GroupMeta:
    group = _group_meta(group_name)
    for subgroup in group.subgroups:
        if subgroup.name == subgroup_name:
            return subgroup
    raise KeyError(f"Unknown subgroup: {group_name}.{subgroup_name}")


def _command_help(*, group_name: str, command_name: str, subgroup_name: str | None = None) -> str:
    group = _subgroup_meta(group_name, subgroup_name) if subgroup_name else _group_meta(group_name)
    for command in group.commands:
        if command.name == command_name:
            return command.help
    raise KeyError(f"Unknown command in registry: {group_name}.{command_name}")


def _root_help_text() -> str:
    lines = ["matey: opinionated dbmate wrapper for repeatable migrations + schema safety.", ""]
    lines.append("Command Groups:")
    for group in ROOT_GROUPS:
        command_names = ", ".join(command.name for command in group.commands)
        lines.append(f"- {group.name}: {command_names}")
        for subgroup in group.subgroups:
            subgroup_commands = ", ".join(
                command.name for command in subgroup.commands if command.name != "__summary__"
            )
            lines.append(f"  - {group.name}.{subgroup.name}: {subgroup_commands}")
    return "\n".join(lines)


@dataclass(frozen=True)
class RootOptions:
    target: str | None
    all_targets: bool
    config_path: Path | None
    dir_override: Path | None
    base_ref: str | None
    url: str | None
    test_url: str | None
    keep_scratch: bool
    verbose: bool
    quiet: bool


@dataclass
class CliState:
    options: RootOptions
    schema_engine: SchemaEngine
    db_engine: DbEngine
    presenter: CliPresenter
    repo_root: Path
    _resolved_config: ResolvedConfig | None = None


app = typer.Typer(
    help=_root_help_text(),
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
OUTPUT_OPT = typer.Option("human", "--output", help="Output format: human or json.")
PATH_OPT = typer.Option(
    None,
    "--path",
    resolve_path=True,
    help="Write template to this path; omit to print to stdout.",
)
OVERWRITE_OPT = typer.Option(False, "--overwrite", help="Allow overwriting existing file when --path is set.")
SCHEMA_FILE_ARG = typer.Argument(..., exists=True, dir_okay=False, resolve_path=True)


def _parse_output(*, presenter: CliPresenter, value: str) -> OutputFormat:
    try:
        return presenter.parse_output_format(value)
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error


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
    if ctx.resilient_parsing or ctx.invoked_subcommand == "template":
        return

    context = build_context(cwd=Path.cwd(), dbmate_bin=dbmate_bin)
    schema_engine = SchemaEngine(context=context)
    db_engine = DbEngine(context=context, schema_engine=schema_engine)
    ctx.obj = CliState(
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


def _state(ctx: typer.Context) -> CliState:
    root_ctx = ctx.find_root()
    state = root_ctx.obj
    if not isinstance(state, CliState):
        raise CliUsageError("CLI state is not initialized.")
    return state


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

    return tuple((name, build_target_runtime(resolved=resolved.targets[name]), resolved.defaults) for name in names)


def _target_text_blocks(
    ctx: typer.Context,
    *,
    render_text,
) -> tuple[TargetText, ...]:
    return tuple(
        TargetText(target=name, text=render_text(name, runtime, defaults))
        for name, runtime, defaults in _resolve_targets(ctx)
    )


def _emit_target_text(
    ctx: typer.Context,
    *,
    command_id: str,
    render_text,
) -> None:
    state = _state(ctx)
    state.presenter.emit_target_text(
        command_id=command_id,
        blocks=_target_text_blocks(ctx, render_text=render_text),
    )


def _db_plan_records(ctx: typer.Context, *, mode: str) -> tuple[DbPlanRecord, ...]:
    state = _state(ctx)
    records: list[DbPlanRecord] = []
    for name, runtime, defaults in _resolve_targets(ctx):
        if mode == "drift":
            result = state.db_engine.db_drift(
                runtime=runtime,
                defaults=defaults,
                url_override=state.options.url,
                test_url_override=state.options.test_url,
                keep_scratch=state.options.keep_scratch,
            )
        elif mode == "plan":
            result = state.db_engine.db_plan(
                runtime=runtime,
                defaults=defaults,
                url_override=state.options.url,
                test_url_override=state.options.test_url,
                keep_scratch=state.options.keep_scratch,
            )
        else:
            raise CliUsageError(f"Unsupported db plan mode: {mode}")
        records.append(DbPlanRecord(target=name, result=result.result))
    return tuple(records)


def _schema_plan_records(ctx: typer.Context, *, clean: bool) -> tuple[SchemaPlanRecord, ...]:
    state = _state(ctx)
    return tuple(
        SchemaPlanRecord(
            target=name,
            result=state.schema_engine.schema_plan(
                runtime=runtime,
                defaults=defaults,
                base_ref=state.options.base_ref,
                clean=clean,
                keep_scratch=state.options.keep_scratch,
                url_override=state.options.url,
                test_url_override=state.options.test_url,
            ),
        )
        for name, runtime, defaults in _resolve_targets(ctx)
    )


db_app = typer.Typer(help=DB_GROUP.help)
db_plan_app = typer.Typer(help=DB_PLAN.help, invoke_without_command=True)
schema_app = typer.Typer(help=SCHEMA_GROUP.help)
schema_plan_app = typer.Typer(help=SCHEMA_PLAN.help, invoke_without_command=True)
template_app = typer.Typer(help=TEMPLATE_GROUP.help)
app.add_typer(db_app, name="db")
db_app.add_typer(db_plan_app, name="plan")
app.add_typer(schema_app, name="schema")
schema_app.add_typer(schema_plan_app, name="plan")
app.add_typer(template_app, name="template")


@db_app.command("new", help=_command_help(group_name="db", command_name="new"))
def db_new(ctx: typer.Context, name: str = typer.Argument(..., help="Migration name suffix.")) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.new",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_new(runtime=runtime, name=name),
    )


@db_app.command("create", help=_command_help(group_name="db", command_name="create"))
def db_create(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.create",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_create(
            runtime=runtime,
            url_override=state.options.url,
        ),
    )


@db_app.command("wait", help=_command_help(group_name="db", command_name="wait"))
def db_wait(ctx: typer.Context, timeout: int = typer.Option(60, "--timeout", min=1)) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.wait",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_wait(
            runtime=runtime,
            url_override=state.options.url,
            timeout_seconds=timeout,
        ),
    )


@db_app.command("up", help=_command_help(group_name="db", command_name="up"))
def db_up(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.up",
        render_text=lambda _name, runtime, defaults: state.db_engine.db_up(
            runtime=runtime,
            defaults=defaults,
            url_override=state.options.url,
            test_url_override=state.options.test_url,
            keep_scratch=state.options.keep_scratch,
        ),
    )


@db_app.command("migrate", help=_command_help(group_name="db", command_name="migrate"))
def db_migrate(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.migrate",
        render_text=lambda _name, runtime, defaults: state.db_engine.db_migrate(
            runtime=runtime,
            defaults=defaults,
            url_override=state.options.url,
            test_url_override=state.options.test_url,
            keep_scratch=state.options.keep_scratch,
        ),
    )


@db_app.command("status", help=_command_help(group_name="db", command_name="status"))
def db_status(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.status",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_status(
            runtime=runtime,
            url_override=state.options.url,
        ),
    )


@db_app.command("drift", help=_command_help(group_name="db", command_name="drift"))
def db_drift(ctx: typer.Context, output: str = OUTPUT_OPT) -> None:
    state = _state(ctx)
    records = _db_plan_records(ctx, mode="drift")
    exit_code = state.presenter.emit_db_plan(
        command_id="db.drift",
        mode="drift",
        records=records,
        output=_parse_output(presenter=state.presenter, value=output),
    )
    raise typer.Exit(exit_code)


def _db_plan_summary(ctx: typer.Context, *, output: str) -> None:
    if ctx.invoked_subcommand is not None:
        return
    state = _state(ctx)
    records = _db_plan_records(ctx, mode="plan")
    exit_code = state.presenter.emit_db_plan(
        command_id="db.plan",
        mode="plan",
        records=records,
        output=_parse_output(presenter=state.presenter, value=output),
    )
    raise typer.Exit(exit_code)


@db_plan_app.callback()
def db_plan_callback(ctx: typer.Context, output: str = OUTPUT_OPT) -> None:
    _db_plan_summary(ctx, output=output)


@db_plan_app.command("diff", help=_command_help(group_name="db", subgroup_name="plan", command_name="diff"))
def db_plan_diff(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.plan.diff",
        render_text=lambda _name, runtime, defaults: (
            state.db_engine.db_plan(
                runtime=runtime,
                defaults=defaults,
                url_override=state.options.url,
                test_url_override=state.options.test_url,
                keep_scratch=state.options.keep_scratch,
            ).result.comparison.diff
            or ""
        ),
    )


@db_plan_app.command("sql", help=_command_help(group_name="db", subgroup_name="plan", command_name="sql"))
def db_plan_sql(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.plan.sql",
        render_text=lambda _name, runtime, defaults: state.db_engine.db_plan_sql(
            runtime=runtime,
            defaults=defaults,
        ),
    )


@db_app.command("load", help=_command_help(group_name="db", command_name="load"))
def db_load(
    ctx: typer.Context,
    schema_file: Path = SCHEMA_FILE_ARG,
) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.load",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_load(
            runtime=runtime,
            url_override=state.options.url,
            schema_path=schema_file,
        ),
    )


@db_app.command("dump", help=_command_help(group_name="db", command_name="dump"))
def db_dump(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.dump",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_dump(
            runtime=runtime,
            url_override=state.options.url,
        ),
    )


@db_app.command("down", help=_command_help(group_name="db", command_name="down"))
def db_down(ctx: typer.Context, steps: int = typer.Argument(1, min=1)) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.down",
        render_text=lambda _name, runtime, defaults: state.db_engine.db_down(
            runtime=runtime,
            defaults=defaults,
            steps=steps,
            url_override=state.options.url,
            test_url_override=state.options.test_url,
            keep_scratch=state.options.keep_scratch,
        ),
    )


@db_app.command("drop", help=_command_help(group_name="db", command_name="drop"))
def db_drop(ctx: typer.Context) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="db.drop",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_drop(
            runtime=runtime,
            url_override=state.options.url,
        ),
    )


@db_app.command(
    "dbmate",
    help=_command_help(group_name="db", command_name="dbmate"),
    context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
)
def dbmate_passthrough(ctx: typer.Context) -> None:
    state = _state(ctx)
    suffix = tuple(ctx.args)
    _emit_target_text(
        ctx,
        command_id="db.dbmate",
        render_text=lambda _name, runtime, _defaults: state.db_engine.db_raw(
            runtime=runtime,
            url_override=state.options.url,
            argv_suffix=suffix,
        ),
    )


@schema_app.command("status", help=_command_help(group_name="schema", command_name="status"))
def schema_status(ctx: typer.Context, output: str = OUTPUT_OPT) -> None:
    state = _state(ctx)
    records = tuple(
        SchemaStatusRecord(
            target=name,
            result=state.schema_engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None),
        )
        for name, runtime, defaults in _resolve_targets(ctx)
    )
    exit_code = state.presenter.emit_schema_status(
        command_id="schema.status",
        records=records,
        output=_parse_output(presenter=state.presenter, value=output),
    )
    raise typer.Exit(exit_code)


def _schema_plan_summary(ctx: typer.Context, *, clean: bool, output: str) -> None:
    if ctx.invoked_subcommand is not None:
        return
    state = _state(ctx)
    records = _schema_plan_records(ctx, clean=clean)
    exit_code = state.presenter.emit_schema_plan(
        command_id="schema.plan",
        records=records,
        output=_parse_output(presenter=state.presenter, value=output),
    )
    raise typer.Exit(exit_code)


@schema_plan_app.callback()
def schema_plan_callback(
    ctx: typer.Context,
    clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
    output: str = OUTPUT_OPT,
) -> None:
    _schema_plan_summary(ctx, clean=clean, output=output)


@schema_plan_app.command("diff", help=_command_help(group_name="schema", subgroup_name="plan", command_name="diff"))
def schema_plan_diff(
    ctx: typer.Context,
    clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
) -> None:
    state = _state(ctx)
    exit_code = 0
    chunks: list[TargetText] = []
    for record in _schema_plan_records(ctx, clean=clean):
        result = record.result
        chunks.append(TargetText(target=record.target, text=result.comparison.diff or ""))
        if not result.comparison.equal:
            exit_code = 1
    state.presenter.emit_target_text(command_id="schema.plan.diff", blocks=tuple(chunks))
    raise typer.Exit(exit_code)


@schema_plan_app.command("sql", help=_command_help(group_name="schema", subgroup_name="plan", command_name="sql"))
def schema_plan_sql(
    ctx: typer.Context,
    clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
) -> None:
    state = _state(ctx)
    _emit_target_text(
        ctx,
        command_id="schema.plan.sql",
        render_text=lambda _name, runtime, defaults: state.schema_engine.schema_plan_sql(
            runtime=runtime,
            defaults=defaults,
            base_ref=state.options.base_ref,
            clean=clean,
            keep_scratch=state.options.keep_scratch,
            url_override=state.options.url,
            test_url_override=state.options.test_url,
        ),
    )


@schema_app.command("apply", help=_command_help(group_name="schema", command_name="apply"))
def schema_apply(
    ctx: typer.Context,
    clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
) -> None:
    state = _state(ctx)
    records: list[OperationRecord] = []
    for name, runtime, defaults in _resolve_targets(ctx):
        state.schema_engine.schema_apply(
            runtime=runtime,
            defaults=defaults,
            base_ref=state.options.base_ref,
            clean=clean,
            keep_scratch=state.options.keep_scratch,
            url_override=state.options.url,
            test_url_override=state.options.test_url,
        )
        records.append(
            OperationRecord(
                target=name,
                status="applied",
                detail="schema, checkpoints, and lockfile updated",
            )
        )
    state.presenter.emit_operations(command_id="schema.apply", records=tuple(records))


@template_app.command("config", help=_command_help(group_name="template", command_name="config"))
def template_config(path: Path | None = PATH_OPT, overwrite: bool = OVERWRITE_OPT) -> None:
    engine = ConfigTemplateEngine()
    if path is None:
        typer.echo(engine.render())
        return
    engine.write(path=path, overwrite=overwrite)


@template_app.command("ci", help=_command_help(group_name="template", command_name="ci"))
def template_ci(path: Path | None = PATH_OPT, overwrite: bool = OVERWRITE_OPT) -> None:
    engine = CiTemplateEngine()
    if path is None:
        typer.echo(engine.render())
        return
    engine.write(path=path, overwrite=overwrite)
