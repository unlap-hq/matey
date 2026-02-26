from __future__ import annotations

from typing import Annotated, Literal

import typer

from matey.cli.common import (
    build_execution_context,
    get_options,
    resolve_target_execution,
    run_clean_upgrade_modes,
    write_schema_file,
)
from matey.cli.output import OutputOptions, RichDbmateRenderer
from matey.domain import (
    ConfigError,
    PathResolutionError,
    SchemaValidationError,
    TargetSelectionError,
    URLResolutionError,
)
from matey.workflows.schema import (
    dump_schema_for_url,
    read_schema_sql,
    schema_diff_text,
    validate_schema_clean_target,
)


def register(schema_app: typer.Typer) -> None:
    @schema_app.command("validate", help="Validate canonical schema against a clean scratch install.")
    def schema_validate(
        ctx: typer.Context,
        schema_only: Annotated[
            bool, typer.Option("--schema-only", help="Run schema consistency checks only.")
        ] = False,
        path_only: Annotated[
            bool, typer.Option("--path-only", help="Run upgrade-path checks only.")
        ] = False,
        no_upgrade_diff: Annotated[
            bool, typer.Option("--no-upgrade-diff", help="Disable clean vs upgrade diff checks.")
        ] = False,
        no_repo_check: Annotated[
            bool, typer.Option("--no-repo-check", help="Skip repo schema.sql comparison.")
        ] = False,
        keep_scratch: Annotated[
            bool, typer.Option("--keep-scratch", help="Keep scratch DB/dataset after validation.")
        ] = False,
    ) -> None:
        options = get_options(ctx)
        if schema_only and path_only:
            raise typer.BadParameter("--schema-only and --path-only cannot be used together.")

        keep_resources = keep_scratch or options.keep_scratch
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
                paths, real_url, test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=selected_target,
                    require_real_url=False,
                )
                result = validate_schema_clean_target(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    real_url=real_url,
                    test_url=test_url,
                    keep_scratch=keep_resources,
                    no_repo_check=no_repo_check,
                    schema_only=schema_only,
                    path_only=path_only,
                    no_upgrade_diff=no_upgrade_diff,
                    base_branch=options.base_branch,
                    on_dbmate_result=renderer.handle,
                )
            except (PathResolutionError, URLResolutionError, ConfigError, SchemaValidationError) as error:
                typer.secho(f"[matey] target={selected_target.name} validation error: {error}", fg="red")
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name} unexpected validation error: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if result.diff_text:
                typer.echo(f"=== repo vs clean (regen would change) [target={selected_target.name}] ===")
                typer.echo(result.diff_text.rstrip())
            if result.upgrade_diff_text:
                typer.echo(
                    "=== clean vs upgrade (upgrade differs from fresh install) "
                    f"[target={selected_target.name}] ==="
                )
                typer.echo(result.upgrade_diff_text.rstrip())
            if result.error:
                typer.secho(f"[matey] target={selected_target.name}: {result.error}", fg="red")
            elif not options.quiet:
                typer.secho(f"[matey] target={selected_target.name}: schema validation passed.", fg="green")

            if keep_resources:
                urls = result.scratch_urls or (result.scratch_url,)
                for url in urls:
                    typer.echo(f"[matey] target={selected_target.name}: keeping scratch at {url}")

            if not result.success:
                failures += 1

        if failures:
            raise typer.Exit(1)

    @schema_app.command("regen", help="Regenerate canonical schema.sql from a clean scratch install.")
    def schema_regen(
        ctx: typer.Context,
        schema_only: Annotated[
            bool, typer.Option("--schema-only", help="Run schema consistency checks only.")
        ] = False,
        path_only: Annotated[
            bool, typer.Option("--path-only", help="Run upgrade-path checks only.")
        ] = False,
        no_upgrade_diff: Annotated[
            bool, typer.Option("--no-upgrade-diff", help="Disable clean vs upgrade diff checks.")
        ] = False,
        no_repo_check: Annotated[
            bool, typer.Option("--no-repo-check", help="Skip repo schema.sql comparison.")
        ] = False,
        force: Annotated[
            bool,
            typer.Option("--force", help="Write schema.sql even when clean and upgrade schema differ."),
        ] = False,
        keep_scratch: Annotated[
            bool, typer.Option("--keep-scratch", help="Keep scratch DB/dataset after regen.")
        ] = False,
    ) -> None:
        options = get_options(ctx)
        if schema_only and path_only:
            raise typer.BadParameter("--schema-only and --path-only cannot be used together.")

        keep_resources = keep_scratch or options.keep_scratch
        renderer = RichDbmateRenderer(
            options=OutputOptions(verbose=options.verbose, quiet=options.quiet),
        )
        run_clean, run_upgrade = run_clean_upgrade_modes(schema_only=schema_only, path_only=path_only)

        try:
            context = build_execution_context(options)
        except (ConfigError, TargetSelectionError) as error:
            raise typer.BadParameter(str(error)) from error

        failures = 0
        for selected_target in context.selected_targets:
            try:
                paths, real_url, test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=selected_target,
                    require_real_url=False,
                )
                result = validate_schema_clean_target(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    real_url=real_url,
                    test_url=test_url,
                    keep_scratch=keep_resources,
                    no_repo_check=no_repo_check,
                    schema_only=schema_only,
                    path_only=path_only,
                    no_upgrade_diff=no_upgrade_diff,
                    base_branch=options.base_branch,
                    on_dbmate_result=renderer.handle,
                )
            except (PathResolutionError, URLResolutionError, ConfigError, SchemaValidationError) as error:
                typer.secho(f"[matey] target={selected_target.name} regen error: {error}", fg="red")
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name} unexpected regen error: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if result.diff_text:
                typer.echo(f"=== repo vs clean (regen would change) [target={selected_target.name}] ===")
                typer.echo(result.diff_text.rstrip())
            if result.upgrade_diff_text:
                typer.echo(
                    "=== clean vs upgrade (upgrade differs from fresh install) "
                    f"[target={selected_target.name}] ==="
                )
                typer.echo(result.upgrade_diff_text.rstrip())
            if result.error:
                typer.secho(f"[matey] target={selected_target.name}: {result.error}", fg="red")

            if keep_resources:
                urls = result.scratch_urls or (result.scratch_url,)
                for url in urls:
                    typer.echo(f"[matey] target={selected_target.name}: keeping scratch at {url}")

            if run_upgrade:
                upgrade_schema = result.upgrade_schema_sql
                clean_schema = result.clean_schema_sql
                schemas_match = upgrade_schema is None or clean_schema == upgrade_schema
                if not schemas_match and not force:
                    typer.secho(
                        f"[matey] target={selected_target.name}: refusing to write schema.sql "
                        "because clean and upgrade schema differ (use --force to override).",
                        fg="red",
                    )
                    failures += 1
                    continue

            if not run_clean:
                if result.error:
                    failures += 1
                continue

            clean_schema = result.clean_schema_sql
            if clean_schema is None:
                typer.secho(
                    f"[matey] target={selected_target.name}: missing clean schema output.",
                    fg="red",
                )
                failures += 1
                continue

            changed = write_schema_file(paths.schema_file, clean_schema)
            if not options.quiet:
                if changed:
                    typer.secho(
                        f"[matey] target={selected_target.name}: wrote {paths.schema_file}",
                        fg="green",
                    )
                else:
                    typer.secho(
                        f"[matey] target={selected_target.name}: schema already up to date.",
                        fg="green",
                    )

            if result.error and not changed:
                failures += 1

        if failures:
            raise typer.Exit(1)

    @schema_app.command("diff", help="Show schema differences in scratch or live mode.")
    def schema_diff(
        ctx: typer.Context,
        live: Annotated[
            bool,
            typer.Option("--live", help="Compare live DB schema against expected schema."),
        ] = False,
        expected: Annotated[
            Literal["repo", "clean"],
            typer.Option("--expected", help="Live mode expected schema source."),
        ] = "repo",
        schema_only: Annotated[
            bool, typer.Option("--schema-only", help="Run schema consistency checks only.")
        ] = False,
        path_only: Annotated[
            bool, typer.Option("--path-only", help="Run upgrade-path checks only.")
        ] = False,
        no_upgrade_diff: Annotated[
            bool, typer.Option("--no-upgrade-diff", help="Disable clean vs upgrade diff checks.")
        ] = False,
        keep_scratch: Annotated[
            bool, typer.Option("--keep-scratch", help="Keep scratch DB/dataset after diff.")
        ] = False,
    ) -> None:
        options = get_options(ctx)
        if schema_only and path_only:
            raise typer.BadParameter("--schema-only and --path-only cannot be used together.")
        if live and path_only:
            raise typer.BadParameter("--path-only is incompatible with live mode.")
        if options.url_override and not live:
            raise typer.BadParameter("--url can only be used with schema diff --live.")

        keep_resources = keep_scratch or options.keep_scratch
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
                paths, real_url, test_url = resolve_target_execution(
                    context=context,
                    options=options,
                    target=selected_target,
                    require_real_url=live,
                )
            except (PathResolutionError, URLResolutionError, ConfigError) as error:
                typer.secho(f"[matey] target={selected_target.name} diff error: {error}", fg="red")
                failures += 1
                continue

            if live:
                try:
                    if expected == "clean":
                        clean_result = validate_schema_clean_target(
                            target_name=selected_target.name,
                            dbmate_binary=context.dbmate_binary,
                            paths=paths,
                            real_url=real_url,
                            test_url=test_url,
                            keep_scratch=keep_resources,
                            no_repo_check=True,
                            schema_only=True,
                            path_only=False,
                            no_upgrade_diff=True,
                            base_branch=options.base_branch,
                            on_dbmate_result=renderer.handle,
                        )
                        if clean_result.error:
                            typer.secho(
                                f"[matey] target={selected_target.name}: {clean_result.error}",
                                fg="red",
                            )
                            failures += 1
                            continue
                        expected_sql = clean_result.clean_schema_sql or ""
                        expected_label = "clean scratch schema"
                        if keep_resources:
                            urls = clean_result.scratch_urls or (clean_result.scratch_url,)
                            for url in urls:
                                typer.echo(f"[matey] target={selected_target.name}: keeping scratch at {url}")
                    else:
                        expected_sql = read_schema_sql(paths.schema_file)
                        expected_label = str(paths.schema_file)

                    live_schema = dump_schema_for_url(
                        dbmate_binary=context.dbmate_binary,
                        paths=paths,
                        url=real_url,
                        target_name=selected_target.name,
                        on_dbmate_result=renderer.handle,
                    )
                except (URLResolutionError, ConfigError, SchemaValidationError) as error:
                    typer.secho(f"[matey] target={selected_target.name} live diff error: {error}", fg="red")
                    failures += 1
                    continue
                except Exception as error:
                    typer.secho(
                        f"[matey] target={selected_target.name} unexpected live diff error: {error}",
                        fg="red",
                    )
                    failures += 1
                    continue

                live_diff_text = schema_diff_text(
                    expected_sql,
                    live_schema,
                    expected_name=f"expected ({expected_label})",
                    actual_name="live",
                )
                if live_diff_text:
                    typer.echo(f"=== expected vs live (--live) [target={selected_target.name}] ===")
                    typer.echo(live_diff_text.rstrip())
                    failures += 1
                elif not options.quiet:
                    typer.secho(
                        f"[matey] target={selected_target.name}: no schema differences found.",
                        fg="green",
                    )
                continue

            try:
                result = validate_schema_clean_target(
                    target_name=selected_target.name,
                    dbmate_binary=context.dbmate_binary,
                    paths=paths,
                    real_url=real_url,
                    test_url=test_url,
                    keep_scratch=keep_resources,
                    no_repo_check=False,
                    schema_only=schema_only,
                    path_only=path_only,
                    no_upgrade_diff=no_upgrade_diff,
                    base_branch=options.base_branch,
                    on_dbmate_result=renderer.handle,
                )
            except (URLResolutionError, ConfigError, SchemaValidationError) as error:
                typer.secho(f"[matey] target={selected_target.name} diff error: {error}", fg="red")
                failures += 1
                continue
            except Exception as error:
                typer.secho(
                    f"[matey] target={selected_target.name} unexpected diff error: {error}",
                    fg="red",
                )
                failures += 1
                continue

            if result.diff_text:
                typer.echo(f"=== repo vs clean (regen would change) [target={selected_target.name}] ===")
                typer.echo(result.diff_text.rstrip())
            if result.upgrade_diff_text:
                typer.echo(
                    "=== clean vs upgrade (upgrade differs from fresh install) "
                    f"[target={selected_target.name}] ==="
                )
                typer.echo(result.upgrade_diff_text.rstrip())
            if result.error:
                typer.secho(f"[matey] target={selected_target.name}: {result.error}", fg="red")
                failures += 1
                continue
            if keep_resources:
                urls = result.scratch_urls or (result.scratch_url,)
                for url in urls:
                    typer.echo(f"[matey] target={selected_target.name}: keeping scratch at {url}")
            if not result.success:
                failures += 1
            elif not options.quiet:
                typer.secho(
                    f"[matey] target={selected_target.name}: no schema differences found.",
                    fg="green",
                )

        if failures:
            raise typer.Exit(1)
