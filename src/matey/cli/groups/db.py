from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

import typer

from matey.app.db_engine import DbEngine
from matey.cli.help import GroupMeta, command_help
from matey.cli.presenter import (
    CliPresenter,
    DbPlanRecord,
    OutputFormat,
    TargetText,
)

ResolveTargetsFn = Callable[[typer.Context], tuple[tuple[str, object, object], ...]]
SCHEMA_FILE_ARG = typer.Argument(..., exists=True, dir_okay=False, resolve_path=True)
OUTPUT_OPT = typer.Option("human", "--output", help="Output format: human or json.")


def _parse_output(*, presenter: CliPresenter, value: str) -> OutputFormat:
    try:
        return presenter.parse_output_format(value)
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error


def register_db_group(
    *,
    parent: typer.Typer,
    db_engine_for_ctx: Callable[[typer.Context], DbEngine],
    resolve_targets: ResolveTargetsFn,
    options_for_ctx: Callable[[typer.Context], object],
    group_meta: GroupMeta,
    presenter_for_ctx: Callable[[typer.Context], CliPresenter] | None = None,
) -> None:
    if presenter_for_ctx is None:
        def presenter_for_ctx(_ctx: typer.Context) -> CliPresenter:
            return CliPresenter()

    db_app = typer.Typer(help=group_meta.help)
    plan_app = typer.Typer(
        help=command_help(group_name="db", command_name="plan"),
        invoke_without_command=True,
    )

    @db_app.command("new", help=command_help(group_name="db", command_name="new"))
    def db_new(
        ctx: typer.Context,
        name: str = typer.Argument(..., help="Migration name suffix."),
    ) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            output = db_engine.db_new(runtime=runtime, name=name)
            outputs.append(TargetText(target=target_name, text=output))
        presenter.emit_target_text(command_id="db.new", blocks=tuple(outputs))

    @db_app.command("create", help=command_help(group_name="db", command_name="create"))
    def db_create(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            output = db_engine.db_create(runtime=runtime, url_override=opts.url)
            outputs.append(TargetText(target=target_name, text=output))
        presenter.emit_target_text(command_id="db.create", blocks=tuple(outputs))

    @db_app.command("wait", help=command_help(group_name="db", command_name="wait"))
    def db_wait(
        ctx: typer.Context,
        timeout: int = typer.Option(60, "--timeout", min=1),
    ) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            output = db_engine.db_wait(
                runtime=runtime,
                url_override=opts.url,
                timeout_seconds=timeout,
            )
            outputs.append(TargetText(target=target_name, text=output))
        presenter.emit_target_text(command_id="db.wait", blocks=tuple(outputs))

    @db_app.command("up", help=command_help(group_name="db", command_name="up"))
    def db_up(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for name, runtime, defaults in selections:
            output = db_engine.db_up(
                runtime=runtime,
                defaults=defaults,
                url_override=opts.url,
                test_url_override=opts.test_url,
                keep_scratch=opts.keep_scratch,
            )
            outputs.append(TargetText(target=name, text=output))
        presenter.emit_target_text(command_id="db.up", blocks=tuple(outputs))

    @db_app.command("migrate", help=command_help(group_name="db", command_name="migrate"))
    def db_migrate(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for name, runtime, defaults in selections:
            output = db_engine.db_migrate(
                runtime=runtime,
                defaults=defaults,
                url_override=opts.url,
                test_url_override=opts.test_url,
                keep_scratch=opts.keep_scratch,
            )
            outputs.append(TargetText(target=name, text=output))
        presenter.emit_target_text(command_id="db.migrate", blocks=tuple(outputs))

    @db_app.command("status", help=command_help(group_name="db", command_name="status"))
    def db_status(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            status_text = db_engine.db_status(runtime=runtime, url_override=opts.url)
            outputs.append(TargetText(target=target_name, text=status_text))
        presenter.emit_target_text(command_id="db.status", blocks=tuple(outputs))

    @db_app.command("drift", help=command_help(group_name="db", command_name="drift"))
    def db_drift(
        ctx: typer.Context,
        output: str = OUTPUT_OPT,
    ) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        chosen_output = _parse_output(presenter=presenter, value=output)
        records: list[DbPlanRecord] = []
        for target_name, runtime, defaults in selections:
            plan = db_engine.db_drift(
                runtime=runtime,
                defaults=defaults,
                url_override=opts.url,
                test_url_override=opts.test_url,
                keep_scratch=opts.keep_scratch,
            )
            records.append(DbPlanRecord(target=target_name, result=plan.result))
        exit_code = presenter.emit_db_plan(
            command_id="db.drift",
            mode="drift",
            records=tuple(records),
            output=chosen_output,
        )
        raise typer.Exit(exit_code)

    db_app.add_typer(plan_app, name="plan")

    def db_plan_summary(ctx: typer.Context, *, output: str) -> None:
        if ctx.invoked_subcommand is not None:
            return
        presenter = presenter_for_ctx(ctx)
        chosen_output = _parse_output(presenter=presenter, value=output)
        db_engine = db_engine_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        records: list[DbPlanRecord] = []
        for target_name, runtime, defaults in selections:
            plan = db_engine.db_plan(
                runtime=runtime,
                defaults=defaults,
                url_override=opts.url,
                test_url_override=opts.test_url,
                keep_scratch=opts.keep_scratch,
            )
            records.append(DbPlanRecord(target=target_name, result=plan.result))
        exit_code = presenter.emit_db_plan(
            command_id="db.plan",
            mode="plan",
            records=tuple(records),
            output=chosen_output,
        )
        raise typer.Exit(exit_code)

    @plan_app.command("diff", help=command_help(group_name="db", subgroup_name="plan", command_name="diff"))
    def db_plan_diff(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, defaults in selections:
            plan = db_engine.db_plan(
                runtime=runtime,
                defaults=defaults,
                url_override=opts.url,
                test_url_override=opts.test_url,
                keep_scratch=opts.keep_scratch,
            )
            outputs.append(TargetText(target=target_name, text=plan.result.comparison.diff or ""))
        presenter.emit_target_text(command_id="db.plan.diff", blocks=tuple(outputs))

    @plan_app.command("sql", help=command_help(group_name="db", subgroup_name="plan", command_name="sql"))
    def db_plan_sql(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, defaults in selections:
            sql_text = db_engine.db_plan_sql(runtime=runtime, defaults=defaults)
            outputs.append(TargetText(target=target_name, text=sql_text))
        presenter.emit_target_text(command_id="db.plan.sql", blocks=tuple(outputs))

    @plan_app.callback()
    def db_plan_callback(
        ctx: typer.Context,
        output: str = OUTPUT_OPT,
    ) -> None:
        db_plan_summary(ctx, output=output)

    @db_app.command("load", help=command_help(group_name="db", command_name="load"))
    def db_load(
        ctx: typer.Context,
        schema_file: Path = SCHEMA_FILE_ARG,
    ) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            output = db_engine.db_load(
                runtime=runtime,
                url_override=opts.url,
                schema_path=schema_file,
            )
            outputs.append(TargetText(target=target_name, text=output))
        presenter.emit_target_text(command_id="db.load", blocks=tuple(outputs))

    @db_app.command("dump", help=command_help(group_name="db", command_name="dump"))
    def db_dump(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            output = db_engine.db_dump(runtime=runtime, url_override=opts.url)
            outputs.append(TargetText(target=target_name, text=output))
        presenter.emit_target_text(command_id="db.dump", blocks=tuple(outputs))

    @db_app.command("down", help=command_help(group_name="db", command_name="down"))
    def db_down(
        ctx: typer.Context,
        steps: int = typer.Argument(1, min=1),
    ) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for name, runtime, defaults in selections:
            output = db_engine.db_down(
                runtime=runtime,
                defaults=defaults,
                steps=steps,
                url_override=opts.url,
                test_url_override=opts.test_url,
                keep_scratch=opts.keep_scratch,
            )
            outputs.append(TargetText(target=name, text=output))
        presenter.emit_target_text(command_id="db.down", blocks=tuple(outputs))

    @db_app.command("drop", help=command_help(group_name="db", command_name="drop"))
    def db_drop(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            output = db_engine.db_drop(runtime=runtime, url_override=opts.url)
            outputs.append(TargetText(target=target_name, text=output))
        presenter.emit_target_text(command_id="db.drop", blocks=tuple(outputs))

    @db_app.command(
        "dbmate",
        help=command_help(group_name="db", command_name="dbmate"),
        context_settings={"allow_extra_args": True, "ignore_unknown_options": True},
    )
    def dbmate_passthrough(ctx: typer.Context) -> None:
        db_engine = db_engine_for_ctx(ctx)
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        suffix = tuple(ctx.args)
        outputs: list[TargetText] = []
        for target_name, runtime, _defaults in selections:
            output = db_engine.db_raw(runtime=runtime, url_override=opts.url, argv_suffix=suffix)
            outputs.append(TargetText(target=target_name, text=output))
        presenter.emit_target_text(command_id="db.dbmate", blocks=tuple(outputs))

    parent.add_typer(db_app, name="db")
