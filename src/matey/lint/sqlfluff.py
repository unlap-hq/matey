from __future__ import annotations

from pathlib import Path

from sqlfluff.core import Linter
from sqlfluff.core.config import FluffConfig

from matey.sql import is_bigquery_family
from matey.sql.policy import normalize_engine

from . import LintFinding


def lint_paths(
    *,
    target_name: str,
    paths: tuple[Path, ...],
    target_root: Path,
    engine: str | None,
) -> tuple[LintFinding, ...]:
    if not paths:
        return ()
    dialect = _sqlfluff_dialect(engine)
    findings: list[LintFinding] = []
    config = FluffConfig.from_kwargs(
        dialect=dialect,
        require_dialect=dialect is not None,
    )
    linter = Linter(config=config)
    for filepath in paths:
        linted = linter.lint_string(
            filepath.read_text(encoding="utf-8"),
            fname=str(filepath),
            config=config,
        )
        try:
            display_path = filepath.relative_to(target_root).as_posix()
        except ValueError:
            display_path = filepath.as_posix()
        findings.extend(
            LintFinding(
                target_name=target_name,
                path=display_path,
                code=f"SF.{violation.rule_code()}",
                level="warning" if getattr(violation, "warning", False) else "error",
                message=violation.desc(),
                line=violation.line_no,
            )
            for violation in linted.get_violations()
        )
    return tuple(findings)


def _sqlfluff_dialect(engine: str | None) -> str | None:
    normalized = normalize_engine(engine)
    if not normalized:
        return None
    if is_bigquery_family(normalized):
        return "bigquery"
    return normalized


__all__ = ["lint_paths"]
