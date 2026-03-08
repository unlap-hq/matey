from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal

from cyclopts import App, Parameter

from matey.lint import Linter
from matey.project import Workspace

from .common import AllOpt, CliUsageError, PathOpt, WorkspaceOpt

EngineOpt = Annotated[
    str | None,
    Parameter(
        name="--engine",
        help="Target engine override for uninitialized targets with no lockfile.",
    ),
]

FormatOpt = Annotated[
    Literal["text", "json"],
    Parameter(name="--format", help="Output format for lint findings."),
]
NoStyleOpt = Annotated[
    bool,
    Parameter(name="--no-style", negative=(), help="Skip sqlfluff style linting."),
]
NoSemanticOpt = Annotated[
    bool,
    Parameter(name="--no-semantic", negative=(), help="Skip matey semantic linting."),
]


def register_lint_command(*, root_app: App) -> None:
    @root_app.command(name="lint", sort_key=20)
    def lint_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        all_targets: AllOpt = False,
        engine: EngineOpt = None,
        no_semantic: NoSemanticOpt = False,
        no_style: NoStyleOpt = False,
        format: FormatOpt = "text",
    ) -> None:
        """Run matey semantic lint plus sqlfluff style lint on migration files."""
        if no_style and no_semantic:
            raise CliUsageError("Lint must run at least one of semantic or style checks.")

        workspace_obj = Workspace.discover(
            start=Path.cwd().resolve(),
            workspace=workspace,
            allow_create_fallback=False,
        )
        raise SystemExit(
            Linter(
                workspace=workspace_obj,
                format=format,
                semantic=not no_semantic,
                style=not no_style,
                engine_override=engine,
            ).run(path=path, all_targets=all_targets)
        )


__all__ = ["register_lint_command"]
