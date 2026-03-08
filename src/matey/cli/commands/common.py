from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

import matey.dbmate as dbmate_api
from matey.dbmate import CmdResult
from matey.project import ConfigError, TargetConfig, Workspace

from ..render import Renderer


class CliUsageError(RuntimeError):
    pass


PathOpt = Annotated[
    str | None, Parameter(name="--path", help="Select target path relative to the workspace root.")
]
AllOpt = Annotated[
    bool,
    Parameter(
        name="--all",
        negative=(),
        help="Run command for all configured targets.",
    ),
]
WorkspaceOpt = Annotated[
    Path | None, Parameter(name="--workspace", help="Workspace root directory.")
]
UrlOpt = Annotated[
    str | None, Parameter(name="--url", help="Override selected target live database URL.")
]
StepsOpt = Annotated[int, Parameter(name="--steps", help="Number of migrations to rollback.")]
BaseOpt = Annotated[str | None, Parameter(name="--base", help="Base ref for base-aware planning.")]
TestUrlOpt = Annotated[
    str | None, Parameter(name="--test-url", help="Scratch test base URL override.")
]
CleanOpt = Annotated[
    bool,
    Parameter(
        name="--clean",
        negative=(),
        help="Replay full migration chain from empty.",
    ),
]
KeepScratchOpt = Annotated[
    bool,
    Parameter(
        name="--keep-scratch",
        negative=(),
        help="Keep scratch database after command.",
    ),
]
ForceOpt = Annotated[
    bool,
    Parameter(
        name="--force",
        negative=(),
        help="Allow init to overwrite existing init-managed artifacts.",
    ),
]
SqlOpt = Annotated[
    bool,
    Parameter(
        name="--sql",
        negative=(),
        help="Print expected SQL output.",
    ),
]
DiffOpt = Annotated[
    bool,
    Parameter(
        name="--diff",
        negative=(),
        help="Print unified diff output.",
    ),
]
EngineOpt = Annotated[
    str | None,
    Parameter(
        name="--engine",
        help="Target engine for zero-state init. Required for fresh targets unless an existing lockfile supplies one.",
    ),
]


def _parse_dbmate_passthrough_args(
    args: tuple[str, ...],
) -> tuple[Path | None, tuple[str, ...]] | None:
    if not args or args[0] != "dbmate":
        return None

    dbmate_bin: Path | None = None
    passthrough: list[str] = []

    def dbmate_bin_path(raw: str) -> Path:
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
            dbmate_bin = dbmate_bin_path(args[index + 1])
            index += 2
            continue
        if token.startswith("--dbmate-bin="):
            if dbmate_bin is not None:
                raise CliUsageError("dbmate passthrough received duplicate --dbmate-bin values.")
            dbmate_bin = dbmate_bin_path(token.split("=", 1)[1])
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


def run_targets(
    *,
    workspace_path: Path | None,
    path: str | None,
    all_targets: bool,
    renderer: Renderer,
    require_single: bool,
    body: Callable[[TargetConfig], None],
) -> None:
    workspace = Workspace.discover(
        start=Path.cwd().resolve(),
        workspace=workspace_path,
        allow_create_fallback=False,
    )
    selected = workspace.select(
        path=path,
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
    if result.exit_code != 0:
        raise CliUsageError(
            f"{context} failed (exit_code={result.exit_code}): "
            f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
        )
    renderer.stdout_blob(result.stdout)
    renderer.stderr_blob(result.stderr)


def plan_mode(*, sql: bool, diff: bool) -> str:
    if sql and diff:
        raise CliUsageError("Cannot combine --sql and --diff.")
    if sql:
        return "sql"
    if diff:
        return "diff"
    return "summary"


__all__ = [
    "AllOpt",
    "BaseOpt",
    "CleanOpt",
    "CliUsageError",
    "ConfigError",
    "DiffOpt",
    "EngineOpt",
    "ForceOpt",
    "KeepScratchOpt",
    "PathOpt",
    "SqlOpt",
    "StepsOpt",
    "TargetConfig",
    "TestUrlOpt",
    "UrlOpt",
    "WorkspaceOpt",
    "dbmate_api",
    "handle_dbmate_passthrough",
    "plan_mode",
    "render_cmd_blob",
    "run_targets",
]
