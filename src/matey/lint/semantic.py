from __future__ import annotations

from collections import defaultdict
from pathlib import Path

from sqlglot import exp, parse
from sqlglot.errors import ParseError

from matey.config import TargetConfig
from matey.lockfile import DiagnosticCode, build_lock_state
from matey.lockfile.parse import migration_version
from matey.repo import Snapshot, SnapshotError
from matey.sql import (
    SqlError,
    SqlProgram,
    SqlTextDecodeError,
    decode_sql_text,
    has_executable_sql,
    is_bigquery_family,
)
from matey.sql.policy import normalize_engine
from matey.sql.source import _DOWN_MARKER, _UP_MARKER

from .model import LintFinding, LintResult


def lint_target(
    target: TargetConfig,
    *,
    engine: str | None = None,
) -> LintResult:
    findings: list[LintFinding] = []
    try:
        snapshot = Snapshot.from_worktree(target)
    except SnapshotError as error:
        return LintResult(
            target_name=target.name,
            findings=(
                LintFinding(
                    target_name=target.name,
                    path=target.dir.as_posix(),
                    code="L201",
                    level="error",
                    message=str(error),
                ),
            ),
        )

    state = build_lock_state(snapshot)
    findings.extend(_lock_findings(target=target, state=state, snapshot=snapshot))

    resolved_engine = normalize_engine(state.lock.engine if state.lock is not None else (engine or target.engine))
    if state.lock is not None and engine is not None:
        provided_engine = normalize_engine(engine)
        if provided_engine and provided_engine != state.lock.engine:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=target.lockfile.relative_to(target.dir).as_posix(),
                    code="L209",
                    level="error",
                    message=(
                        f"Provided engine {provided_engine!r} does not match lock engine "
                        f"{state.lock.engine!r}."
                    ),
                )
            )
    if not resolved_engine:
        findings.append(
            LintFinding(
                target_name=target.name,
                path=target.lockfile.relative_to(target.dir).as_posix(),
                code="L200",
                level="error",
                message="Target is uninitialized and engine is unknown. Pass --engine or initialize the target first.",
            )
        )
    findings.extend(_artifact_state_findings(target=target, snapshot=snapshot, state=state, engine=resolved_engine))

    paths = tuple(sorted(snapshot.migrations))
    findings.extend(_migration_structure_findings(target=target, paths=paths))

    for path in paths:
        findings.extend(
            _migration_content_findings(
                target=target,
                path=path,
                payload=snapshot.migrations[path],
                engine=resolved_engine,
            )
        )

    return LintResult(target_name=target.name, findings=tuple(findings))


def _lock_findings(*, target: TargetConfig, state, snapshot: Snapshot) -> tuple[LintFinding, ...]:
    findings: list[LintFinding] = []
    lock_present = snapshot.lock_toml is not None
    for diagnostic in state.diagnostics:
        path = diagnostic.path
        if diagnostic.code is DiagnosticCode.INPUT_ORPHAN_CHECKPOINT:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L202",
                    level="warning",
                    message=diagnostic.detail,
                )
            )
            continue
        if diagnostic.code is DiagnosticCode.INPUT_SCHEMA_MISSING and lock_present:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L203",
                    level="error",
                    message=diagnostic.detail,
                )
            )
            continue
        code = "L204" if _is_coherence_diagnostic(diagnostic.code) else "L201"
        findings.append(
            LintFinding(
                target_name=target.name,
                path=path,
                code=code,
                level="error",
                message=diagnostic.detail,
            )
        )
    return tuple(findings)


def _artifact_state_findings(
    *,
    target: TargetConfig,
    snapshot: Snapshot,
    state,
    engine: str,
) -> tuple[LintFinding, ...]:
    findings: list[LintFinding] = []
    if state.lock is None:
        if snapshot.checkpoints:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=target.checkpoints.relative_to(target.dir).as_posix(),
                    code="L205",
                    level="warning",
                    message="Target has checkpoint files but no initialized lock state.",
                )
            )
        if snapshot.schema_sql is not None:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=target.schema.relative_to(target.dir).as_posix(),
                    code="L206",
                    level="error",
                    message="Target has schema.sql but no schema.lock.toml.",
                )
            )
        if snapshot.migrations and not engine:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=target.migrations.relative_to(target.dir).as_posix(),
                    code="L208",
                    level="error",
                    message="Target has migrations but no initialized lock state and no engine override.",
                )
            )
    return tuple(findings)


def _is_coherence_diagnostic(code: DiagnosticCode) -> bool:
    return code.name.startswith("COHERENCE_")


def _migration_structure_findings(*, target: TargetConfig, paths: tuple[str, ...]) -> tuple[LintFinding, ...]:
    findings: list[LintFinding] = []
    versions: dict[str, str] = {}
    basenames: dict[str, list[str]] = defaultdict(list)
    seen_version = ""
    for path in paths:
        basename = Path(path).name
        basenames[basename].append(path)
        version = migration_version(basename)
        previous = versions.get(version)
        if previous is not None:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L002",
                    level="error",
                    message=f"Duplicate migration version {version!r}; already used by {previous!r}.",
                )
            )
        else:
            versions[version] = path
        if seen_version and version < seen_version:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L003",
                    level="error",
                    message=f"Migration version {version!r} is out of order after {seen_version!r}.",
                )
            )
        seen_version = version
    for basename, group in basenames.items():
        if len(group) < 2:
            continue
        findings.append(
            LintFinding(
                target_name=target.name,
                path=group[0],
                code="L006",
                level="warning",
                message=f"Multiple migrations share basename {basename!r}: {', '.join(group)}.",
            )
        )
    return tuple(findings)


def _migration_content_findings(
    *,
    target: TargetConfig,
    path: str,
    payload: bytes,
    engine: str,
) -> tuple[LintFinding, ...]:
    findings: list[LintFinding] = []
    try:
        program = SqlProgram(
            decode_sql_text(payload, label=f"migration {path}"),
            engine=engine,
        )
    except SqlTextDecodeError:
        return (
            LintFinding(
                target_name=target.name,
                path=path,
                code="L004",
                level="error",
                message=f"Unable to decode migration {path!r} as UTF-8.",
            ),
        )

    directive_error = _directive_structure_error(program._text)
    if directive_error is not None:
        findings.append(
            LintFinding(
                target_name=target.name,
                path=path,
                code="L001",
                level="error",
                message=directive_error,
            )
        )
    if _has_transaction_false(program._text):
        findings.append(
            LintFinding(
                target_name=target.name,
                path=path,
                code="L106",
                level="warning",
                message="Migration uses transaction:false.",
            )
        )

    try:
        if not has_executable_sql(program.up_sql, engine=engine):
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L005",
                    level="error",
                    message="Migration has no executable up SQL.",
                )
            )

        if not program.down_sql.strip():
            findings.extend(
                (
                    LintFinding(
                        target_name=target.name,
                        path=path,
                        code="L101",
                        level="warning",
                        message="Migration has no down section.",
                    ),
                    LintFinding(
                        target_name=target.name,
                        path=path,
                        code="L103",
                        level="warning",
                        message="Migration is irreversible: no executable down SQL.",
                    ),
                )
            )
        elif not program.has_executable_down():
            findings.extend(
                (
                    LintFinding(
                        target_name=target.name,
                        path=path,
                        code="L102",
                        level="warning",
                        message="Migration down section exists but has no executable SQL.",
                    ),
                    LintFinding(
                        target_name=target.name,
                        path=path,
                        code="L103",
                        level="warning",
                        message="Migration is irreversible: no executable down SQL.",
                    ),
                )
            )

        findings.extend(
            _write_violation_finding(
                target_name=target.name,
                path=path,
                engine=engine,
                code="L104",
                violation=violation,
            )
            for violation in program.section_write_violations("up")
        )
        findings.extend(
            _write_violation_finding(
                target_name=target.name,
                path=path,
                engine=engine,
                code="L105",
                violation=violation,
            )
            for violation in program.section_write_violations("down")
        )

        if engine == "bigquery-emulator":
            findings.extend(_bigquery_emulator_findings(target=target, path=path, sql=program.up_sql))
    except SqlError as error:
        findings.append(
            LintFinding(
                target_name=target.name,
                path=path,
                code="L001",
                level="error",
                message=f"SQL analysis failed: {error}",
            )
        )

    return tuple(findings)


def _write_violation_finding(
    *,
    target_name: str,
    path: str,
    engine: str,
    code: str,
    violation,
) -> LintFinding:
    if is_bigquery_family(engine):
        message = f"BigQuery-family qualified writes are not allowed. Statement: {violation.excerpt()!r}"
    elif engine in {"mysql", "clickhouse"}:
        message = f"{engine} cross-database writes are not allowed. Statement: {violation.excerpt()!r}"
    else:
        message = violation.excerpt()
    return LintFinding(
        target_name=target_name,
        path=path,
        code=code,
        level="error",
        message=message,
    )


def _directive_structure_error(text: str) -> str | None:
    seen_up = False
    seen_down = False
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r\n")
        if _UP_MARKER.fullmatch(line):
            if seen_down:
                return "dbmate migrate:up marker appears after a down marker."
            if seen_up:
                return "dbmate migrate:up marker appears more than once."
            seen_up = True
            continue
        if _DOWN_MARKER.fullmatch(line):
            if not seen_up:
                return "dbmate migrate:down marker appears before an up marker."
            if seen_down:
                return "dbmate migrate:down marker appears more than once."
            seen_down = True
    return None


def _bigquery_emulator_findings(*, target: TargetConfig, path: str, sql: str) -> tuple[LintFinding, ...]:
    try:
        expressions = parse(sql, read="bigquery")
    except ParseError:
        return ()
    findings: list[LintFinding] = []
    for expr in expressions:
        if not isinstance(expr, exp.Create):
            continue
        kind = str(expr.args.get("kind") or "").upper()
        if kind in {"FUNCTION", "PROCEDURE"}:
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L301",
                    level="error",
                    message=f"bigquery-emulator does not support dumping {kind.lower()} objects safely.",
                )
            )
            continue
        if kind == "VIEW" and _is_materialized_view(expr):
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L301",
                    level="error",
                    message="bigquery-emulator does not support dumping materialized views safely.",
                )
            )
            continue
        if kind == "VIEW":
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L301",
                    level="error",
                    message="bigquery-emulator does not support dumping views safely.",
                )
            )
            continue
        if kind == "TABLE" and _has_bigquery_emulator_unsupported_table_features(expr):
            findings.append(
                LintFinding(
                    target_name=target.name,
                    path=path,
                    code="L301",
                    level="error",
                    message="bigquery-emulator does not support dumping partitioning, clustering, or column defaults safely.",
                )
            )
    return tuple(findings)


def _is_materialized_view(expr: exp.Create) -> bool:
    properties = expr.args.get("properties")
    if properties is None:
        return False
    return any(isinstance(prop, exp.MaterializedProperty) for prop in properties.expressions)


def _has_bigquery_emulator_unsupported_table_features(expr: exp.Create) -> bool:
    properties = expr.args.get("properties")
    if properties is not None and any(
        isinstance(prop, (exp.PartitionedByProperty, exp.Cluster))
        for prop in properties.expressions
    ):
        return True
    return any(
        any(isinstance(constraint.kind, exp.DefaultColumnConstraint) for constraint in column.args.get("constraints", []))
        for column in expr.find_all(exp.ColumnDef)
    )


def _has_transaction_false(text: str) -> bool:
    for raw_line in text.splitlines():
        line = raw_line.rstrip("\r\n")
        if (_UP_MARKER.fullmatch(line) or _DOWN_MARKER.fullmatch(line)) and "transaction:false" in line.lower():
            return True
    return False
__all__ = ["lint_target"]
