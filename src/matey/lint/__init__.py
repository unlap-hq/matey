from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Literal

from matey.project import TargetConfig, Workspace


@dataclass(frozen=True, slots=True)
class LintFinding:
    target_name: str
    path: str
    code: str
    level: Literal["error", "warning"]
    message: str
    line: int | None = None


@dataclass(frozen=True, slots=True)
class LintResult:
    target_name: str
    findings: tuple[LintFinding, ...]


@dataclass(slots=True)
class Linter:
    workspace: Workspace
    format: Literal["text", "json"]
    semantic: bool
    style: bool
    engine_override: str | None

    def run(self, *, path: str | None, all_targets: bool) -> int:
        targets = self.workspace.select(path=path, all_targets=all_targets, require_single=False)
        results = tuple(self._lint_one_target(target) for target in targets)
        self._emit_results(results)
        return 1 if any(f.level == "error" for result in results for f in result.findings) else 0

    def _lint_one_target(self, target: TargetConfig) -> LintResult:
        from .semantic import lint_target as lint_semantic_target
        from .sqlfluff import lint_paths as lint_style_paths

        semantic = (
            LintResult(target_name=target.name, findings=())
            if not self.semantic
            else lint_semantic_target(target, engine=self.engine_override)
        )
        findings = list(semantic.findings)
        if self.style and self._should_run_style(semantic.findings):
            paths = tuple(
                sorted(target.migrations.rglob("*.sql"), key=lambda item: item.as_posix())
            )
            findings.extend(
                lint_style_paths(
                    target_name=target.name,
                    paths=paths,
                    target_root=target.root,
                    engine=self.engine_override or target.engine,
                )
            )
        return LintResult(
            target_name=target.name,
            findings=tuple(
                sorted(
                    findings, key=lambda item: (item.path, item.line or 0, item.code, item.message)
                )
            ),
        )

    def _emit_results(self, results: tuple[LintResult, ...]) -> None:
        if self.format == "json":
            print(json.dumps([asdict(result) for result in results], indent=2))
            return
        for index, result in enumerate(results):
            if index:
                print()
            print(f"target: {result.target_name}")
            for finding in result.findings:
                line = f":{finding.line}" if finding.line is not None else ""
                print(
                    f"{finding.level.upper():<5} {finding.code} {finding.path}{line} {finding.message}"
                )

    @staticmethod
    def _should_run_style(findings: tuple[LintFinding, ...]) -> bool:
        return not any(finding.code == "L004" for finding in findings)


__all__ = ["LintFinding", "LintResult", "Linter"]
