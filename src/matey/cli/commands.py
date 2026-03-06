from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter

import matey.db as db_api
import matey.dbmate as dbmate_api
import matey.schema as schema_api
from matey.cli.template import (
    TemplateProvider,
    render_ci_template,
    render_config_template,
    write_text_file,
)
from matey.config import Config, ConfigError, TargetConfig
from matey.dbmate import CmdResult

from .render import Renderer


class CliUsageError(RuntimeError):
    pass


TargetOpt = Annotated[str | None, Parameter(name="--target", help="Select target name.")]
AllOpt = Annotated[bool, Parameter(name="--all", help="Run command for all configured targets.")]
ConfigOpt = Annotated[Path | None, Parameter(name="--config", help="Path to matey.toml config.")]
DbmateBinOpt = Annotated[Path | None, Parameter(name="--dbmate-bin", help="Path to dbmate binary.")]
UrlOpt = Annotated[str | None, Parameter(name="--url", help="Override selected target live database URL.")]
StepsOpt = Annotated[int, Parameter(name="--steps", help="Number of migrations to rollback.")]
BaseOpt = Annotated[str | None, Parameter(name="--base", help="Base ref for base-aware planning.")]
TestUrlOpt = Annotated[str | None, Parameter(name="--test-url", help="Scratch test base URL override.")]
CleanOpt = Annotated[bool, Parameter(name="--clean", help="Replay full migration chain from empty.")]
KeepScratchOpt = Annotated[bool, Parameter(name="--keep-scratch", help="Keep scratch database after command.")]
PathOpt = Annotated[Path | None, Parameter(name="--path", help="Write output to this path instead of stdout.")]
OverwriteOpt = Annotated[bool, Parameter(name="--overwrite", help="Allow overwriting existing file when writing.")]
SqlOpt = Annotated[bool, Parameter(name="--sql", help="Print expected SQL output.")]
DiffOpt = Annotated[bool, Parameter(name="--diff", help="Print unified diff output.")]
ProviderOpt = Annotated[TemplateProvider, Parameter(name="--provider", help="CI provider.")]


def register_commands(
    *,
    db_app: App,
    schema_app: App,
    template_app: App,
    root_app: App,
    renderer: Renderer,
) -> None:
    @db_app.command(name="up", sort_key=20)
    def up_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Create DB if missing, then apply pending migrations."""
        item = single_target(config_path=config, target=target, all_targets=all_targets)
        renderer.db_mutation("up", db_api.up(item, url=url, dbmate_bin=dbmate_bin))

    @db_app.command(name="migrate", sort_key=30)
    def migrate_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Apply pending migrations (no create-if-needed)."""
        item = single_target(config_path=config, target=target, all_targets=all_targets)
        renderer.db_mutation("migrate", db_api.migrate(item, url=url, dbmate_bin=dbmate_bin))

    @db_app.command(name="status", sort_key=10)
    def status_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Show live migration status."""
        def _run(item: TargetConfig) -> None:
            result = db_api.status_raw(item, url=url, dbmate_bin=dbmate_bin)
            require_cmd_success(result, context=f"db status ({item.name})")
            renderer.stdout_blob(result.stdout)

        for_targets(config_path=config, target=target, all_targets=all_targets, require_single=False, handler=_run, renderer=renderer)

    @db_app.command(name="new", sort_key=70)
    def new_command(
        name: str,
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
    ) -> None:
        """Create a new migration file."""
        item = single_target(config_path=config, target=target, all_targets=all_targets)
        result = db_api.new(item, name=name, dbmate_bin=dbmate_bin)
        require_cmd_success(result, context=f"db new ({item.name})")
        renderer.stdout_blob(result.stdout)
        renderer.stderr_blob(result.stderr)

    @db_app.command(name="drift", sort_key=50)
    def drift_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Check live schema drift."""
        def _run(item: TargetConfig) -> None:
            renderer.db_drift(db_api.drift(item, url=url, dbmate_bin=dbmate_bin))

        for_targets(config_path=config, target=target, all_targets=all_targets, require_single=False, handler=_run, renderer=renderer)

    @db_app.command(name="down", sort_key=40)
    def down_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
        steps: StepsOpt = 1,
    ) -> None:
        """Rollback migration(s)."""
        item = single_target(config_path=config, target=target, all_targets=all_targets)
        renderer.db_mutation("down", db_api.down(item, steps=steps, url=url, dbmate_bin=dbmate_bin))

    @db_app.command(name="plan", sort_key=60)
    def db_plan_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
        sql: SqlOpt = False,
        diff: DiffOpt = False,
    ) -> None:
        """Compare live schema to expected worktree target schema."""
        mode = plan_mode(sql=sql, diff=diff)

        def _run(item: TargetConfig) -> None:
            dispatch_plan_mode(
                mode=mode,
                summary=lambda: renderer.db_plan(db_api.plan(item, url=url, dbmate_bin=dbmate_bin)),
                sql=lambda: renderer.sql_blob(db_api.plan_sql(item, url=url, dbmate_bin=dbmate_bin)),
                diff=lambda: renderer.diff_blob(
                    db_api.plan_diff(item, url=url, dbmate_bin=dbmate_bin)
                ),
            )

        for_targets(config_path=config, target=target, all_targets=all_targets, require_single=False, handler=_run, renderer=renderer)

    @schema_app.command(name="status", sort_key=10)
    def schema_status_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
    ) -> None:
        """Show schema artifact health."""
        def _run(item: TargetConfig) -> None:
            renderer.schema_status(schema_api.status(item))

        for_targets(config_path=config, target=target, all_targets=all_targets, require_single=False, handler=_run, renderer=renderer)

    @schema_app.command(name="plan", sort_key=20)
    def schema_plan_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        base: BaseOpt = None,
        clean: CleanOpt = False,
        test_url: TestUrlOpt = None,
        keep_scratch: KeepScratchOpt = False,
        sql: SqlOpt = False,
        diff: DiffOpt = False,
    ) -> None:
        """Compute schema replay plan."""
        mode = plan_mode(sql=sql, diff=diff)

        def _run(item: TargetConfig) -> None:
            kwargs = {
                "base_ref": base,
                "clean": clean,
                "test_base_url": test_url,
                "keep_scratch": keep_scratch,
                "dbmate_bin": dbmate_bin,
            }
            dispatch_plan_mode(
                mode=mode,
                summary=lambda: renderer.schema_plan(schema_api.plan(item, **kwargs)),
                sql=lambda: renderer.sql_blob(schema_api.plan_sql(item, **kwargs)),
                diff=lambda: renderer.diff_blob(schema_api.plan_diff(item, **kwargs)),
            )

        for_targets(config_path=config, target=target, all_targets=all_targets, require_single=False, handler=_run, renderer=renderer)

    @schema_app.command(name="apply", sort_key=30)
    def schema_apply_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        base: BaseOpt = None,
        clean: CleanOpt = False,
        test_url: TestUrlOpt = None,
        keep_scratch: KeepScratchOpt = False,
    ) -> None:
        """Apply schema replay outputs."""
        item = single_target(config_path=config, target=target, all_targets=all_targets)
        renderer.schema_apply(
            schema_api.apply(
                item,
                base_ref=base,
                clean=clean,
                test_base_url=test_url,
                keep_scratch=keep_scratch,
                dbmate_bin=dbmate_bin,
            )
        )

    @template_app.command(name="config", sort_key=10)
    def template_config_command(*targets: str, path: PathOpt = None, overwrite: OverwriteOpt = False) -> None:
        """Render matey config template."""
        try:
            content = render_config_template(tuple(targets))
        except ValueError as error:
            raise CliUsageError(str(error)) from error
        emit_template(content=content, path=path, overwrite=overwrite, renderer=renderer)

    @template_app.command(name="ci", sort_key=20)
    def template_ci_command(provider: ProviderOpt, path: PathOpt = None, overwrite: OverwriteOpt = False) -> None:
        """Render CI template."""
        content = render_ci_template(provider)
        emit_template(content=content, path=path, overwrite=overwrite, renderer=renderer)

    @root_app.command(name="dbmate", sort_key=90, help_flags=[])
    def dbmate_passthrough_command(*args: str, dbmate_bin: DbmateBinOpt = None) -> None:
        """Run dbmate directly with verbatim arguments."""
        passthrough_args = args or ("--help",)
        result = dbmate_api.passthrough(*passthrough_args, dbmate_bin=dbmate_bin)
        renderer.stdout_blob(result.stdout)
        renderer.stderr_blob(result.stderr)
        if result.exit_code != 0:
            raise SystemExit(result.exit_code)


def for_targets(
    *,
    config_path: Path | None,
    target: str | None,
    all_targets: bool,
    require_single: bool,
    handler: Callable[[TargetConfig], None],
    renderer: Renderer,
) -> None:
    selected = select_targets(
        config_path=config_path,
        target=target,
        all_targets=all_targets,
        require_single=require_single,
    )
    show_headers = len(selected) > 1
    for item in selected:
        if show_headers:
            renderer.target_header(item.name)
        handler(item)


def select_targets(
    *,
    config_path: Path | None,
    target: str | None,
    all_targets: bool,
    require_single: bool,
) -> tuple[TargetConfig, ...]:
    if require_single and all_targets:
        raise CliUsageError("This command requires exactly one target; do not pass --all.")
    config = load_config(config_path)
    selected = config.select(target=target, all_targets=all_targets)
    if require_single and len(selected) != 1:
        raise CliUsageError("This command requires exactly one resolved target.")
    return selected


def single_target(
    *,
    config_path: Path | None,
    target: str | None,
    all_targets: bool,
) -> TargetConfig:
    return select_targets(
        config_path=config_path,
        target=target,
        all_targets=all_targets,
        require_single=True,
    )[0]


def load_config(config_path: Path | None) -> Config:
    repo_root = find_repo_root(Path.cwd().resolve())
    return Config.load(repo_root, config_path=config_path)


def find_repo_root(start: Path) -> Path:
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def require_cmd_success(result: CmdResult, *, context: str) -> None:
    if result.exit_code == 0:
        return
    raise CliUsageError(
        f"{context} failed (exit_code={result.exit_code}): "
        f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
    )


def emit_template(*, content: str, path: Path | None, overwrite: bool, renderer: Renderer) -> None:
    if path is None:
        renderer.template_content(content)
        return
    try:
        write_text_file(path, content, overwrite=overwrite)
    except FileExistsError as error:
        raise CliUsageError(str(error)) from error
    renderer.template_written(str(path))


def plan_mode(*, sql: bool, diff: bool) -> str:
    if sql and diff:
        raise CliUsageError("Cannot combine --sql and --diff.")
    if sql:
        return "sql"
    if diff:
        return "diff"
    return "summary"


def dispatch_plan_mode(
    *,
    mode: str,
    summary: Callable[[], None],
    sql: Callable[[], None],
    diff: Callable[[], None],
) -> None:
    match mode:
        case "summary":
            summary()
        case "sql":
            sql()
        case "diff":
            diff()
        case _:
            raise AssertionError("invalid plan mode")


__all__ = [
    "CliUsageError",
    "ConfigError",
    "db_api",
    "dbmate_api",
    "dispatch_plan_mode",
    "emit_template",
    "find_repo_root",
    "for_targets",
    "load_config",
    "plan_mode",
    "register_commands",
    "render_ci_template",
    "render_config_template",
    "require_cmd_success",
    "schema_api",
    "select_targets",
    "single_target",
    "write_text_file",
]
