from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


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


__all__ = ["LintFinding", "LintResult"]
