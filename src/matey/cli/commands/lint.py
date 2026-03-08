from __future__ import annotations

import json
from dataclasses import asdict
from typing import Annotated, Literal

from cyclopts import App, Parameter

from matey.lint import LintResult
from matey.lint import lint_paths as lint_style_paths
from matey.lint import lint_target as lint_semantic_target

from .common import AllOpt, CliUsageError, PathOpt, WorkspaceOpt, select_targets

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

        targets = select_targets(
            workspace_path=workspace,
            path=path,
            all_targets=all_targets,
            require_single=False,
        )
        results = tuple(
            _lint_one_target(
                item,
                engine=engine,
                no_style=no_style,
                no_semantic=no_semantic,
            )
            for item in targets
        )
        _emit_results(results, format=format)
        if any(finding.level == "error" for result in results for finding in result.findings):
            raise SystemExit(1)


def _lint_one_target(
    target,
    *,
    engine: str | None,
    no_style: bool,
    no_semantic: bool,
) -> LintResult:
    semantic = (
        LintResult(target_name=target.name, findings=())
        if no_semantic
        else lint_semantic_target(target, engine=engine)
    )
    findings = list(semantic.findings)
    if not no_style and _should_run_style(semantic.findings):
        paths = tuple(sorted(target.migrations.rglob("*.sql"), key=lambda path: path.as_posix()))
        findings.extend(
            lint_style_paths(
                target_name=target.name,
                paths=paths,
                target_root=target.dir,
                engine=engine or target.engine,
            )
        )
    return LintResult(
        target_name=target.name,
        findings=tuple(sorted(findings, key=lambda item: (item.path, item.line or 0, item.code, item.message))),
    )


def _should_run_style(findings) -> bool:
    return not any(finding.code == "L004" for finding in findings)


def _emit_results(results: tuple[LintResult, ...], *, format: str) -> None:
    if format == "json":
        print(json.dumps([asdict(result) for result in results], indent=2))
        return
    for index, result in enumerate(results):
        if index:
            print()
        print(f"target: {result.target_name}")
        for finding in result.findings:
            line = f":{finding.line}" if finding.line is not None else ""
            print(f"{finding.level.upper():<5} {finding.code} {finding.path}{line} {finding.message}")


__all__ = ["register_lint_command"]
