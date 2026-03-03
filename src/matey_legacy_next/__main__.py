from __future__ import annotations

import typer

from matey.cli import app
from matey.errors import CliUsageError, MateyError


def main() -> int:
    try:
        app()
    except typer.Exit as error:
        return int(error.exit_code)
    except CliUsageError as error:
        typer.echo(str(error), err=True)
        return 2
    except MateyError as error:
        typer.echo(str(error), err=True)
        return 1
    except Exception as error:
        typer.echo(f"Unexpected error: {error}", err=True)
        return 1
    else:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
