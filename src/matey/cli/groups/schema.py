from __future__ import annotations

from collections.abc import Callable

import typer

from matey.app.schema_engine import SchemaEngine
from matey.cli.help import GroupMeta, command_help
from matey.cli.presenter import (
    CliPresenter,
    OperationRecord,
    OutputFormat,
    SchemaPlanRecord,
    SchemaStatusRecord,
    TargetText,
)

ResolveTargetsFn = Callable[[typer.Context], tuple[tuple[str, object, object], ...]]
OUTPUT_OPT = typer.Option("human", "--output", help="Output format: human or json.")


def _parse_output(*, presenter: CliPresenter, value: str) -> OutputFormat:
    try:
        return presenter.parse_output_format(value)
    except ValueError as error:
        raise typer.BadParameter(str(error)) from error


def register_schema_group(
    *,
    parent: typer.Typer,
    schema_engine_for_ctx: Callable[[typer.Context], SchemaEngine],
    resolve_targets: ResolveTargetsFn,
    options_for_ctx: Callable[[typer.Context], object],
    group_meta: GroupMeta,
    presenter_for_ctx: Callable[[typer.Context], CliPresenter] | None = None,
) -> None:
    if presenter_for_ctx is None:
        def presenter_for_ctx(_ctx: typer.Context) -> CliPresenter:
            return CliPresenter()

    schema_app = typer.Typer(help=group_meta.help)
    plan_app = typer.Typer(
        help=command_help(group_name="schema", command_name="plan"),
        invoke_without_command=True,
    )

    @schema_app.command("status", help=command_help(group_name="schema", command_name="status"))
    def schema_status(
        ctx: typer.Context,
        output: str = OUTPUT_OPT,
    ) -> None:
        presenter = presenter_for_ctx(ctx)
        selections = resolve_targets(ctx)
        chosen_output = _parse_output(presenter=presenter, value=output)
        records: list[SchemaStatusRecord] = []
        schema_engine = schema_engine_for_ctx(ctx)
        for name, runtime, defaults in selections:
            result = schema_engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None)
            records.append(SchemaStatusRecord(target=name, result=result))
        exit_code = presenter.emit_schema_status(
            command_id="schema.status",
            records=tuple(records),
            output=chosen_output,
        )
        raise typer.Exit(exit_code)

    schema_app.add_typer(plan_app, name="plan")

    def schema_plan_summary(
        ctx: typer.Context,
        *,
        clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
        output: str = OUTPUT_OPT,
    ) -> None:
        if ctx.invoked_subcommand is not None:
            return
        presenter = presenter_for_ctx(ctx)
        chosen_output = _parse_output(presenter=presenter, value=output)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        records: list[SchemaPlanRecord] = []
        schema_engine = schema_engine_for_ctx(ctx)
        for name, runtime, defaults in selections:
            result = schema_engine.schema_plan(
                runtime=runtime,
                defaults=defaults,
                base_ref=opts.base_ref,
                clean=clean,
                keep_scratch=opts.keep_scratch,
                url_override=opts.url,
                test_url_override=opts.test_url,
            )
            records.append(SchemaPlanRecord(target=name, result=result))
        exit_code = presenter.emit_schema_plan(
            command_id="schema.plan",
            records=tuple(records),
            output=chosen_output,
        )
        raise typer.Exit(exit_code)

    @plan_app.callback()
    def schema_plan_callback(
        ctx: typer.Context,
        clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
        output: str = OUTPUT_OPT,
    ) -> None:
        schema_plan_summary(ctx, clean=clean, output=output)

    @plan_app.command(
        "diff",
        help=command_help(group_name="schema", subgroup_name="plan", command_name="diff"),
    )
    def schema_plan_diff(
        ctx: typer.Context,
        clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
    ) -> None:
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        exit_code = 0
        chunks: list[TargetText] = []
        schema_engine = schema_engine_for_ctx(ctx)
        for name, runtime, defaults in selections:
            result = schema_engine.schema_plan(
                runtime=runtime,
                defaults=defaults,
                base_ref=opts.base_ref,
                clean=clean,
                keep_scratch=opts.keep_scratch,
                url_override=opts.url,
                test_url_override=opts.test_url,
            )
            diff = result.comparison.diff or ""
            chunks.append(TargetText(target=name, text=diff))
            if not result.comparison.equal:
                exit_code = 1
        presenter.emit_target_text(command_id="schema.plan.diff", blocks=tuple(chunks))
        raise typer.Exit(exit_code)

    @plan_app.command(
        "sql",
        help=command_help(group_name="schema", subgroup_name="plan", command_name="sql"),
    )
    def schema_plan_sql(
        ctx: typer.Context,
        clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
    ) -> None:
        presenter = presenter_for_ctx(ctx)
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        outputs: list[TargetText] = []
        schema_engine = schema_engine_for_ctx(ctx)
        for name, runtime, defaults in selections:
            sql_text = schema_engine.schema_plan_sql(
                runtime=runtime,
                defaults=defaults,
                base_ref=opts.base_ref,
                clean=clean,
                keep_scratch=opts.keep_scratch,
                url_override=opts.url,
                test_url_override=opts.test_url,
            )
            outputs.append(TargetText(target=name, text=sql_text))
        presenter.emit_target_text(command_id="schema.plan.sql", blocks=tuple(outputs))

    @schema_app.command("apply", help=command_help(group_name="schema", command_name="apply"))
    def schema_apply(
        ctx: typer.Context,
        clean: bool = typer.Option(False, "--clean", help="Replay full chain from empty scratch."),
    ) -> None:
        opts = options_for_ctx(ctx)
        selections = resolve_targets(ctx)
        presenter = presenter_for_ctx(ctx)
        schema_engine = schema_engine_for_ctx(ctx)
        records: list[OperationRecord] = []
        for name, runtime, defaults in selections:
            schema_engine.schema_apply(
                runtime=runtime,
                defaults=defaults,
                base_ref=opts.base_ref,
                clean=clean,
                keep_scratch=opts.keep_scratch,
                url_override=opts.url,
                test_url_override=opts.test_url,
            )
            records.append(
                OperationRecord(
                    target=name,
                    status="applied",
                    detail="schema, checkpoints, and lockfile updated",
                )
            )
        presenter.emit_operations(command_id="schema.apply", records=tuple(records))

    parent.add_typer(schema_app, name="schema")
