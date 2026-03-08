from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Annotated

from cyclopts import Parameter

import matey.dbmate as dbmate_api
from matey.config import Config, ConfigError, TargetConfig
from matey.dbmate import CmdResult
from matey.repo import GitRepo, NotGitRepositoryError

from ..render import Renderer


class CliUsageError(RuntimeError):
    pass


TargetOpt = Annotated[str | None, Parameter(name="--target", help="Select target name.")]
AllOpt = Annotated[
    bool,
    Parameter(
        name="--all",
        negative=(),
        help="Run command for all configured targets.",
    ),
]
ConfigOpt = Annotated[Path | None, Parameter(name="--config", help="Path to matey.toml config.")]
DbmateBinOpt = Annotated[Path | None, Parameter(name="--dbmate-bin", help="Path to dbmate binary.")]
UrlOpt = Annotated[str | None, Parameter(name="--url", help="Override selected target live database URL.")]
StepsOpt = Annotated[int, Parameter(name="--steps", help="Number of migrations to rollback.")]
BaseOpt = Annotated[str | None, Parameter(name="--base", help="Base ref for base-aware planning.")]
TestUrlOpt = Annotated[str | None, Parameter(name="--test-url", help="Scratch test base URL override.")]
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
OverwriteOpt = Annotated[
    bool,
    Parameter(
        name="--overwrite",
        negative=(),
        help="Allow overwriting existing file when writing.",
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
EngineOpt = Annotated[str | None, Parameter(name="--engine", help="Target engine for zero-state init. Required for fresh targets unless an existing lockfile supplies one.")]


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
    "ConfigOpt",
    "DbmateBinOpt",
    "DiffOpt",
    "EngineOpt",
    "KeepScratchOpt",
    "OverwriteOpt",
    "SqlOpt",
    "StepsOpt",
    "TargetOpt",
    "TestUrlOpt",
    "UrlOpt",
    "dbmate_api",
    "find_repo_root",
    "find_repo_root_or_none",
    "handle_dbmate_passthrough",
    "load_config",
    "plan_mode",
    "render_cmd_blob",
    "require_cmd_success",
    "run_targets",
    "select_targets",
]
