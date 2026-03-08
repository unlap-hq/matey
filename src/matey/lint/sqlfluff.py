from __future__ import annotations

from pathlib import Path

from sqlfluff.core import Linter

from matey.sql import is_bigquery_family
from matey.sql.policy import normalize_engine

from .model import LintFinding


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
    linter = Linter(dialect=dialect)
    result = linter.lint_paths(tuple(str(path) for path in paths), processes=1)
    findings: list[LintFinding] = []
    for record in result.as_records():
        filepath = Path(record["filepath"])
        try:
            display_path = filepath.relative_to(target_root).as_posix()
        except ValueError:
            display_path = filepath.as_posix()
        findings.extend(
            LintFinding(
                target_name=target_name,
                path=display_path,
                code=f"SF.{violation['code']}",
                level="warning" if violation.get("warning") else "error",
                message=violation["description"],
                line=violation.get("start_line_no"),
            )
            for violation in record["violations"]
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
