from __future__ import annotations

import traceback
from collections.abc import Sequence
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

from cyclopts import App
from cyclopts.exceptions import CycloptsError

from matey.dbmate import DbmateError
from matey.repo import GitRepoError
from matey.scratch import ScratchError
from matey.tx import TxError

from . import commands
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

renderer = Renderer.create()
commands.register_commands(
    db_app=db_app,
    schema_app=schema_app,
    template_app=template_app,
    root_app=app,
    renderer=renderer,
)
app.command(db_app)
app.command(schema_app)
app.command(template_app)

_USER_ERRORS = (
    commands.CliUsageError,
    commands.db_api.DbError,
    commands.dbmate_api.DbmateError,
    commands.schema_api.SchemaError,
    commands.ConfigError,
    DbmateError,
    GitRepoError,
    ScratchError,
    TxError,
    CycloptsError,
)


def main(argv: Sequence[str] | None = None) -> int:
    from sys import argv as sys_argv

    args = list(sys_argv[1:] if argv is None else argv)
    dbmate_exit = maybe_run_dbmate_passthrough(args)
    if dbmate_exit is not None:
        return dbmate_exit
    try:
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

    dbmate_bin: Path | None = None
    index = 1
    while index < len(args):
        token = args[index]
        if token == "--":
            index += 1
            break
        if token == "--dbmate-bin":
            if index + 1 >= len(args):
                raise commands.CliUsageError("--dbmate-bin requires a path value.")
            dbmate_bin = Path(args[index + 1])
            index += 2
            continue
        if token.startswith("--dbmate-bin="):
            dbmate_bin = Path(token.split("=", 1)[1])
            index += 1
            continue
        break

    passthrough_args = tuple(args[index:]) or ("--help",)
    result = commands.dbmate_api.passthrough(*passthrough_args, dbmate_bin=dbmate_bin)
    renderer.stdout_blob(result.stdout)
    renderer.stderr_blob(result.stderr)
    return result.exit_code


__all__ = ["app", "commands", "db_app", "main", "schema_app", "template_app"]
