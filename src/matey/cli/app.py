from __future__ import annotations

import traceback
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version

from cyclopts import App
from cyclopts.exceptions import CycloptsError

from matey.bqemu import BigQueryEmulatorUrlError
from matey.data import DataError
from matey.dbmate import DbmateError
from matey.paths import PathBoundaryError, RelativePathError
from matey.repo import GitRepoError, SnapshotError
from matey.schema.codegen import CodegenError
from matey.scratch import ScratchError
from matey.sql import MigrationSqlError, SqlTextDecodeError
from matey.tx import TxError

from .commands import common, data, db, init, lint, schema
from .render import Renderer


def app_version() -> str:
    try:
        return version("matey")
    except PackageNotFoundError:
        return "0.0.0"


app = App(
    name="matey",
    help="matey: opinionated dbmate wrapper for repeatable migrations + schema safety.",
    version=app_version,
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
    sort_key=40,
)
schema_app = App(
    name="schema",
    help="Schema artifact workflows.",
    help_flags=["--help"],
    print_error=False,
    exit_on_error=False,
    sort_key=30,
)
data_app = App(
    name="data",
    help="Live data workflows.",
    help_flags=["--help"],
    print_error=False,
    exit_on_error=False,
    sort_key=45,
)
renderer = Renderer.create()
db.register_db_commands(
    db_app=db_app,
    root_app=app,
    renderer=renderer,
)
schema.register_schema_commands(
    schema_app=schema_app,
    renderer=renderer,
)
init.register_init_command(
    root_app=app,
    renderer=renderer,
)
lint.register_lint_command(
    root_app=app,
)
data.register_data_commands(
    data_app=data_app,
    renderer=renderer,
)
app.command(db_app)
app.command(schema_app)
app.command(data_app)

_USER_ERRORS = (
    common.CliUsageError,
    db.db_api.DbError,
    schema.schema_api.SchemaError,
    common.ConfigError,
    DbmateError,
    GitRepoError,
    BigQueryEmulatorUrlError,
    PathBoundaryError,
    RelativePathError,
    MigrationSqlError,
    SnapshotError,
    ScratchError,
    SqlTextDecodeError,
    TxError,
    CodegenError,
    DataError,
    CycloptsError,
)


def main(argv: Sequence[str] | None = None) -> int:
    from sys import argv as sys_argv

    args = list(sys_argv[1:] if argv is None else argv)
    try:
        dbmate_exit = maybe_run_dbmate_passthrough(args)
        if dbmate_exit is not None:
            return dbmate_exit
        app(args)
    except SystemExit as error:
        code = error.code
        return code if isinstance(code, int) else 1
    except _USER_ERRORS as error:
        renderer.error(str(error))
        return 2
    except KeyboardInterrupt:
        renderer.error("Interrupted.")
        return 130
    except Exception as error:
        renderer.error(f"Unexpected error: {error}")
        traceback.print_exc()
        return 1
    else:
        return 0


def maybe_run_dbmate_passthrough(args: list[str]) -> int | None:
    if not args or args[0] != "dbmate":
        return None
    return common.handle_dbmate_passthrough(
        argv=tuple(args),
        renderer=renderer,
    )


__all__ = [
    "app",
    "common",
    "data",
    "data_app",
    "db",
    "db_app",
    "init",
    "lint",
    "main",
    "schema",
    "schema_app",
]
