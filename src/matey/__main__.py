from __future__ import annotations

import sys
import traceback
from collections.abc import Callable, Sequence
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Annotated

from cyclopts import App, Parameter
from cyclopts.exceptions import CycloptsError

import matey.db as db_api
import matey.dbmate as dbmate_api
import matey.schema as schema_api
from matey.cli_render import CliRenderer
from matey.config import Config, ConfigError, TargetConfig
from matey.dbmate import CmdResult, DbmateError
from matey.git_repo import GitRepoError
from matey.scratch import ScratchError
from matey.template import (
    TemplateProvider,
    render_ci_template,
    render_config_template,
    write_text_file,
)
from matey.tx import TxError


class CliUsageError(RuntimeError):
    pass


def _app_version() -> str:
    try:
        return version("matey")
    except PackageNotFoundError:
        return "0.0.0"


app = App(
    name="matey",
    help="matey: opinionated dbmate wrapper for repeatable migrations + schema safety.",
    version=_app_version,
    help_flags=["--help"],
    print_error=False,
    exit_on_error=False,
)
db_app = App(
    name="db",
    help="Live database commands.",
    help_flags=["--help"],
    print_error=False,
    exit_on_error=False,
    sort_key=10,
)
schema_app = App(
    name="schema",
    help="Schema artifact workflows.",
    help_flags=["--help"],
    print_error=False,
    exit_on_error=False,
    sort_key=20,
)
template_app = App(
    name="template",
    help="Project template helpers.",
    help_flags=["--help"],
    print_error=False,
    exit_on_error=False,
    sort_key=30,
)

_renderer = CliRenderer.create()

TargetOpt = Annotated[
    str | None,
    Parameter(name="--target", help="Select target name."),
]
AllOpt = Annotated[
    bool,
    Parameter(name="--all", help="Run command for all configured targets."),
]
ConfigOpt = Annotated[
    Path | None,
    Parameter(name="--config", help="Path to matey.toml config."),
]
DbmateBinOpt = Annotated[
    Path | None,
    Parameter(name="--dbmate-bin", help="Path to dbmate binary."),
]
UrlOpt = Annotated[
    str | None,
    Parameter(name="--url", help="Override selected target live database URL."),
]
StepsOpt = Annotated[
    int,
    Parameter(name="--steps", help="Number of migrations to rollback."),
]
BaseOpt = Annotated[
    str | None,
    Parameter(name="--base", help="Base ref for base-aware planning."),
]
TestUrlOpt = Annotated[
    str | None,
    Parameter(name="--test-url", help="Scratch test base URL override."),
]
CleanOpt = Annotated[
    bool,
    Parameter(name="--clean", help="Replay full migration chain from empty."),
]
KeepScratchOpt = Annotated[
    bool,
    Parameter(name="--keep-scratch", help="Keep scratch database after command."),
]
PathOpt = Annotated[
    Path | None,
    Parameter(name="--path", help="Write output to this path instead of stdout."),
]
OverwriteOpt = Annotated[
    bool,
    Parameter(name="--overwrite", help="Allow overwriting existing file when writing."),
]
SqlOpt = Annotated[
    bool,
    Parameter(name="--sql", help="Print expected SQL output."),
]
DiffOpt = Annotated[
    bool,
    Parameter(name="--diff", help="Print unified diff output."),
]
ProviderOpt = Annotated[
    TemplateProvider,
    Parameter(name="--provider", help="CI provider."),
]

@db_app.command(name="up", sort_key=20)
def up_command(
    target: TargetOpt = None,
    all_targets: AllOpt = False,
    config: ConfigOpt = None,
    dbmate_bin: DbmateBinOpt = None,
    url: UrlOpt = None,
) -> None:
    """Create DB if missing, then apply pending migrations."""
    selected = _select_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=True,
    )
    result = db_api.up(selected[0], url=url, dbmate_bin=dbmate_bin)
    _renderer.db_mutation("up", result)


@db_app.command(name="migrate", sort_key=30)
def migrate_command(
    target: TargetOpt = None,
    all_targets: AllOpt = False,
    config: ConfigOpt = None,
    dbmate_bin: DbmateBinOpt = None,
    url: UrlOpt = None,
) -> None:
    """Apply pending migrations (no create-if-needed)."""
    selected = _select_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=True,
    )
    result = db_api.migrate(selected[0], url=url, dbmate_bin=dbmate_bin)
    _renderer.db_mutation("migrate", result)


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
        _require_cmd_success(result, context=f"db status ({item.name})")
        _renderer.stdout_blob(result.stdout)

    _for_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=False,
        handler=_run,
    )


@db_app.command(name="new", sort_key=70)
def new_command(
    name: str,
    target: TargetOpt = None,
    all_targets: AllOpt = False,
    config: ConfigOpt = None,
    dbmate_bin: DbmateBinOpt = None,
) -> None:
    """Create a new migration file."""
    selected = _select_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=True,
    )
    result = db_api.new(selected[0], name=name, dbmate_bin=dbmate_bin)
    _require_cmd_success(result, context=f"db new ({selected[0].name})")
    _renderer.stdout_blob(result.stdout)
    _renderer.stderr_blob(result.stderr)


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
        result = db_api.drift(item, url=url, dbmate_bin=dbmate_bin)
        _renderer.db_drift(result)

    _for_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=False,
        handler=_run,
    )


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
    selected = _select_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=True,
    )
    result = db_api.down(selected[0], steps=steps, url=url, dbmate_bin=dbmate_bin)
    _renderer.db_mutation("down", result)


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
    mode = _plan_mode(sql=sql, diff=diff)
    def _run(item: TargetConfig) -> None:
        match mode:
            case "summary":
                result = db_api.plan(item, url=url, dbmate_bin=dbmate_bin)
                _renderer.db_plan(result)
            case "sql":
                _renderer.sql_blob(db_api.plan_sql(item, url=url, dbmate_bin=dbmate_bin))
            case "diff":
                _renderer.diff_blob(db_api.plan_diff(item, url=url, dbmate_bin=dbmate_bin))
            case _:
                raise AssertionError("invalid db plan mode")

    _for_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=False,
        handler=_run,
    )


@schema_app.command(name="status", sort_key=10)
def schema_status_command(
    target: TargetOpt = None,
    all_targets: AllOpt = False,
    config: ConfigOpt = None,
) -> None:
    """Show schema artifact health."""
    def _run(item: TargetConfig) -> None:
        result = schema_api.status(item)
        _renderer.schema_status(result)

    _for_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=False,
        handler=_run,
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
    """Compute schema replay plan."""
    mode = _plan_mode(sql=sql, diff=diff)
    def _run(item: TargetConfig) -> None:
        match mode:
            case "summary":
                _renderer.schema_plan(
                    schema_api.plan(
                        item,
                        base_ref=base,
                        clean=clean,
                        test_base_url=test_url,
                        keep_scratch=keep_scratch,
                        dbmate_bin=dbmate_bin,
                    )
                )
            case "sql":
                _renderer.sql_blob(
                    schema_api.plan_sql(
                        item,
                        base_ref=base,
                        clean=clean,
                        test_base_url=test_url,
                        keep_scratch=keep_scratch,
                        dbmate_bin=dbmate_bin,
                    )
                )
            case "diff":
                _renderer.diff_blob(
                    schema_api.plan_diff(
                        item,
                        base_ref=base,
                        clean=clean,
                        test_base_url=test_url,
                        keep_scratch=keep_scratch,
                        dbmate_bin=dbmate_bin,
                    )
                )
            case _:
                raise AssertionError("invalid schema plan mode")

    _for_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=False,
        handler=_run,
    )


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
    selected = _select_targets(
        config_path=config,
        target=target,
        all_targets=all_targets,
        require_single=True,
    )
    result = schema_api.apply(
        selected[0],
        base_ref=base,
        clean=clean,
        test_base_url=test_url,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
    )
    _renderer.schema_apply(result)


@template_app.command(name="config", sort_key=10)
def template_config_command(
    *targets: str,
    path: PathOpt = None,
    overwrite: OverwriteOpt = False,
) -> None:
    """Render matey config template."""
    try:
        content = render_config_template(tuple(targets))
    except ValueError as error:
        raise CliUsageError(str(error)) from error
    _emit_template(content=content, path=path, overwrite=overwrite)


@template_app.command(name="ci", sort_key=20)
def template_ci_command(
    provider: ProviderOpt,
    path: PathOpt = None,
    overwrite: OverwriteOpt = False,
) -> None:
    """Render CI template."""
    _default_path, content = render_ci_template(provider)
    _emit_template(content=content, path=path, overwrite=overwrite)


@app.command(name="dbmate", sort_key=90, help_flags=[])
def dbmate_passthrough_command(
    *args: str,
    dbmate_bin: DbmateBinOpt = None,
) -> None:
    """Run dbmate directly with verbatim arguments."""
    passthrough_args = args or ("--help",)
    result = dbmate_api.passthrough(*passthrough_args, dbmate_bin=dbmate_bin)
    _renderer.stdout_blob(result.stdout)
    _renderer.stderr_blob(result.stderr)
    if result.exit_code != 0:
        raise SystemExit(result.exit_code)


def _for_targets(
    *,
    config_path: Path | None,
    target: str | None,
    all_targets: bool,
    require_single: bool,
    handler: Callable[[TargetConfig], None],
) -> None:
    selected = _select_targets(
        config_path=config_path,
        target=target,
        all_targets=all_targets,
        require_single=require_single,
    )
    show_headers = len(selected) > 1
    for item in selected:
        if show_headers:
            _renderer.target_header(item.name)
        handler(item)


def _select_targets(
    *,
    config_path: Path | None,
    target: str | None,
    all_targets: bool,
    require_single: bool,
) -> tuple[TargetConfig, ...]:
    if require_single and all_targets:
        raise CliUsageError("This command requires exactly one target; do not pass --all.")

    config = _load_config(config_path)
    selected = config.select(target=target, all_targets=all_targets)
    if require_single and len(selected) != 1:
        raise CliUsageError("This command requires exactly one resolved target.")
    return selected


def _load_config(config_path: Path | None) -> Config:
    repo_root = _find_repo_root(Path.cwd().resolve())
    return Config.load(repo_root, config_path=config_path)


def _find_repo_root(start: Path) -> Path:
    current = start
    for candidate in (current, *current.parents):
        if (candidate / ".git").exists():
            return candidate
    return start


def _require_cmd_success(result: CmdResult, *, context: str) -> None:
    if result.exit_code == 0:
        return
    raise CliUsageError(
        f"{context} failed (exit_code={result.exit_code}): "
        f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
    )


def _emit_template(*, content: str, path: Path | None, overwrite: bool) -> None:
    if path is None:
        _renderer.template_content(content)
        return
    try:
        write_text_file(path, content, overwrite=overwrite)
    except FileExistsError as error:
        raise CliUsageError(str(error)) from error
    _renderer.template_written(str(path))


def _plan_mode(*, sql: bool, diff: bool) -> str:
    if sql and diff:
        raise CliUsageError("Cannot combine --sql and --diff.")
    if sql:
        return "sql"
    if diff:
        return "diff"
    return "summary"


app.command(db_app)
app.command(schema_app)
app.command(template_app)


_USER_ERRORS = (
    CliUsageError,
    ConfigError,
    db_api.DbError,
    DbmateError,
    GitRepoError,
    schema_api.SchemaError,
    ScratchError,
    TxError,
    CycloptsError,
)


def main(argv: Sequence[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    dbmate_exit = _maybe_run_dbmate_passthrough(args)
    if dbmate_exit is not None:
        return dbmate_exit
    try:
        app(args)
    except SystemExit as error:
        code = error.code
        return code if isinstance(code, int) else 1
    except _USER_ERRORS as error:
        _renderer.error(str(error))
        return 2
    except KeyboardInterrupt:
        _renderer.error("Interrupted.")
        return 130
    except Exception as error:
        _renderer.error(f"Unexpected error: {error}")
        traceback.print_exc()
        return 1
    else:
        return 0


def _maybe_run_dbmate_passthrough(args: list[str]) -> int | None:
    if not args or args[0] != "dbmate":
        return None

    dbmate_bin: Path | None = None
    index = 1
    while index < len(args):
        token = args[index]
        if token == "--":
            index += 1
            break
        if token == "--dbmate-bin":
            if index + 1 >= len(args):
                raise CliUsageError("--dbmate-bin requires a path value.")
            dbmate_bin = Path(args[index + 1])
            index += 2
            continue
        if token.startswith("--dbmate-bin="):
            dbmate_bin = Path(token.split("=", 1)[1])
            index += 1
            continue
        break

    passthrough_args = tuple(args[index:]) or ("--help",)
    result = dbmate_api.passthrough(*passthrough_args, dbmate_bin=dbmate_bin)
    _renderer.stdout_blob(result.stdout)
    _renderer.stderr_blob(result.stderr)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
