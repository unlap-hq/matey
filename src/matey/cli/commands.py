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
from matey.repo import GitRepo, NotGitRepositoryError

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
    @db_app.command(name="status", sort_key=10)
    def status_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Show live migration status."""
        def render_target(item: TargetConfig) -> None:
            render_cmd_blob(
                renderer=renderer,
                result=db_api.status_raw(item, url=url, dbmate_bin=dbmate_bin),
                context="db status",
            )

        _run_targets(
            config_path=config,
            target=target,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

    @db_app.command(name="bootstrap", sort_key=15)
    def bootstrap_command(
        target: TargetOpt = None,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Load schema.sql into an empty DB and verify dbmate head state."""
        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation(
                "bootstrap",
                db_api.bootstrap(item, url=url, dbmate_bin=dbmate_bin),
            )

        _run_targets(
            config_path=config,
            target=target,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="up", sort_key=20)
    def up_command(
        target: TargetOpt = None,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Create DB if missing, then apply pending migrations."""
        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation(
                "up",
                db_api.up(item, url=url, dbmate_bin=dbmate_bin),
            )

        _run_targets(
            config_path=config,
            target=target,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="migrate", sort_key=30)
    def migrate_command(
        target: TargetOpt = None,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Apply pending migrations (no create-if-needed)."""
        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation(
                "migrate",
                db_api.migrate(item, url=url, dbmate_bin=dbmate_bin),
            )

        _run_targets(
            config_path=config,
            target=target,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="down", sort_key=40)
    def down_command(
        target: TargetOpt = None,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
        steps: StepsOpt = 1,
    ) -> None:
        """Rollback migration(s)."""
        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation(
                "down",
                db_api.down(item, steps=steps, url=url, dbmate_bin=dbmate_bin),
            )

        _run_targets(
            config_path=config,
            target=target,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="drift", sort_key=50)
    def drift_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Check live schema drift."""
        def render_target(item: TargetConfig) -> None:
            renderer.db_drift(
                db_api.drift(item, url=url, dbmate_bin=dbmate_bin)
            )

        _run_targets(
            config_path=config,
            target=target,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

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

        def render_target(item: TargetConfig) -> None:
            match mode:
                case "summary":
                    renderer.db_plan(db_api.plan(item, url=url, dbmate_bin=dbmate_bin))
                case "sql":
                    renderer.sql_blob(db_api.plan_sql(item, url=url, dbmate_bin=dbmate_bin))
                case "diff":
                    renderer.diff_blob(db_api.plan_diff(item, url=url, dbmate_bin=dbmate_bin))
                case _:
                    raise AssertionError("invalid plan mode")

        _run_targets(
            config_path=config,
            target=target,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

    @db_app.command(name="new", sort_key=70)
    def new_command(
        name: str,
        target: TargetOpt = None,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
    ) -> None:
        """Create a new migration file."""
        def render_target(item: TargetConfig) -> None:
            render_cmd_blob(
                renderer=renderer,
                result=db_api.new(item, name=name, dbmate_bin=dbmate_bin),
                context="db new",
            )

        _run_targets(
            config_path=config,
            target=target,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @schema_app.command(name="status", sort_key=10)
    def schema_status_command(
        target: TargetOpt = None,
        all_targets: AllOpt = False,
        config: ConfigOpt = None,
    ) -> None:
        """Show schema artifact health."""
        def render_target(item: TargetConfig) -> None:
            renderer.schema_status(schema_api.status(item))

        _run_targets(
            config_path=config,
            target=target,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

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
        """Run validated schema replay in scratch and inspect the resulting schema."""
        mode = plan_mode(sql=sql, diff=diff)

        def render_target(item: TargetConfig) -> None:
            kwargs = {
                "base_ref": base,
                "clean": clean,
                "test_base_url": test_url,
                "keep_scratch": keep_scratch,
                "dbmate_bin": dbmate_bin,
            }
            match mode:
                case "summary":
                    renderer.schema_plan(schema_api.plan(item, **kwargs))
                case "sql":
                    renderer.sql_blob(schema_api.plan_sql(item, **kwargs))
                case "diff":
                    renderer.diff_blob(schema_api.plan_diff(item, **kwargs))
                case _:
                    raise AssertionError("invalid plan mode")

        _run_targets(
            config_path=config,
            target=target,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

    @schema_app.command(name="apply", sort_key=30)
    def schema_apply_command(
        target: TargetOpt = None,
        config: ConfigOpt = None,
        dbmate_bin: DbmateBinOpt = None,
        base: BaseOpt = None,
        clean: CleanOpt = False,
        test_url: TestUrlOpt = None,
        keep_scratch: KeepScratchOpt = False,
    ) -> None:
        """Run validated schema replay in scratch, then write schema artifacts."""
        def render_target(item: TargetConfig) -> None:
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

        _run_targets(
            config_path=config,
            target=target,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
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
        # Keep a registered command so the root help surface advertises dbmate,
        # while the actual implementation stays shared with the top-level argv intercept.
        raise SystemExit(
            handle_dbmate_passthrough(
                argv=args,
                renderer=renderer,
                dbmate_bin=dbmate_bin,
            )
        )


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


def load_config(config_path: Path | None) -> Config:
    if config_path is not None:
        resolved_config = (
            config_path.resolve()
            if config_path.is_absolute()
            else (Path.cwd() / config_path).resolve()
        )
        repo_root = find_repo_root_or_none(resolved_config.parent) or resolved_config.parent
        return Config.load(
            repo_root,
            config_path=resolved_config,
            config_root=resolved_config.parent,
        )
    repo_root = find_repo_root(Path.cwd().resolve())
    return Config.load(repo_root, config_path=None)


def find_repo_root(start: Path) -> Path:
    repo_root = find_repo_root_or_none(start)
    if repo_root is not None:
        return repo_root
    raise CliUsageError(
        "Path is not inside a git repository. Run from a repo root/subdirectory or pass --config."
    )


def find_repo_root_or_none(start: Path) -> Path | None:
    try:
        return GitRepo.open(start).repo_root
    except NotGitRepositoryError:
        return None


def require_cmd_success(result: CmdResult, *, context: str) -> None:
    if result.exit_code == 0:
        return
    raise CliUsageError(
        f"{context} failed (exit_code={result.exit_code}): "
        f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
    )


def _parse_dbmate_passthrough_args(args: tuple[str, ...]) -> tuple[Path | None, tuple[str, ...]] | None:
    if not args or args[0] != "dbmate":
        return None

    dbmate_bin: Path | None = None
    passthrough: list[str] = []

    def _dbmate_bin_path(raw: str) -> Path:
        if raw == "":
            raise CliUsageError("--dbmate-bin requires a non-empty path value.")
        return Path(raw)

    index = 1
    while index < len(args):
        token = args[index]
        if token == "--":
            passthrough.extend(args[index + 1 :])
            break
        if token == "--dbmate-bin":
            if index + 1 >= len(args):
                raise CliUsageError("--dbmate-bin requires a path value.")
            if dbmate_bin is not None:
                raise CliUsageError("dbmate passthrough received duplicate --dbmate-bin values.")
            dbmate_bin = _dbmate_bin_path(args[index + 1])
            index += 2
            continue
        if token.startswith("--dbmate-bin="):
            if dbmate_bin is not None:
                raise CliUsageError("dbmate passthrough received duplicate --dbmate-bin values.")
            dbmate_bin = _dbmate_bin_path(token.split("=", 1)[1])
            index += 1
            continue
        passthrough.append(token)
        index += 1

    return dbmate_bin, tuple(passthrough) or ("--help",)


def handle_dbmate_passthrough(
    *,
    argv: tuple[str, ...],
    renderer: Renderer,
    dbmate_bin: Path | None = None,
) -> int:
    parsed = _parse_dbmate_passthrough_args(argv)
    if parsed is not None:
        parsed_dbmate_bin, passthrough_args = parsed
        if dbmate_bin is not None and parsed_dbmate_bin is not None:
            raise CliUsageError("dbmate passthrough received duplicate --dbmate-bin values.")
        dbmate_bin = parsed_dbmate_bin if parsed_dbmate_bin is not None else dbmate_bin
    else:
        passthrough_args = argv or ("--help",)

    result = dbmate_api.passthrough(*passthrough_args, dbmate_bin=dbmate_bin)
    renderer.stdout_blob(result.stdout)
    renderer.stderr_blob(result.stderr)
    return result.exit_code


def _run_targets(
    *,
    config_path: Path | None,
    target: str | None,
    all_targets: bool,
    renderer: Renderer,
    require_single: bool,
    body: Callable[[TargetConfig], None],
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
        body(item)


def render_cmd_blob(
    *,
    renderer: Renderer,
    result: CmdResult,
    context: str,
) -> None:
    require_cmd_success(result, context=context)
    renderer.stdout_blob(result.stdout)
    renderer.stderr_blob(result.stderr)


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


__all__ = [
    "CliUsageError",
    "ConfigError",
    "db_api",
    "dbmate_api",
    "emit_template",
    "find_repo_root",
    "handle_dbmate_passthrough",
    "load_config",
    "plan_mode",
    "register_commands",
    "render_ci_template",
    "render_config_template",
    "require_cmd_success",
    "schema_api",
    "select_targets",
    "write_text_file",
]
