from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

from sqlglot import errors as sqlglot_errors
from sqlglot import exp, parse_one

from .lex import (
    compact,
    normalize_identifier_quotes,
    normalize_punctuation_spacing,
    split_sql_statements,
    strip_meta_lines,
)

_MYSQL_CONDITIONAL_COMMENT_PATTERN = re.compile(r"/\*![0-9]{5}\s+(.*?)\*/", re.IGNORECASE)
_MYSQL_EMPTY_CONDITIONAL_PATTERN = re.compile(r"/\*![0-9]{5}\s*\*/", re.IGNORECASE)
_MUTATING_VERB_PATTERN = re.compile(
    r"^(CREATE|ALTER|DROP|INSERT|MERGE|UPDATE|DELETE|TRUNCATE|RENAME|REPLACE)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class WriteViolation:
    section: Literal["up", "down"]
    target: str
    statement: str
    reason: str

    def excerpt(self, *, limit: int = 120) -> str:
        folded = compact(self.statement)
        return folded if len(folded) <= limit else f"{folded[:limit - 3]}..."


@dataclass(frozen=True, slots=True)
class _TargetIdentity:
    catalog: str | None
    db: str | None


@dataclass(frozen=True, slots=True)
class _ParsedStatement:
    sql: str
    expr: exp.Expression | None
    parse_error: str | None


def engine_from_url(context_url: str | None) -> str:
    if not context_url:
        return ""
    if context_url.startswith("sqlite3:"):
        return "sqlite"
    parsed = urlsplit(context_url)
    return _normalize_engine(parsed.scheme.split("+", 1)[0])


def bigquery_target_from_url(context_url: str | None) -> tuple[str, str] | None:
    if not context_url:
        return None
    parsed = urlsplit(context_url)
    if _normalize_engine(parsed.scheme.split("+", 1)[0]) != "bigquery":
        return None
    project = parsed.netloc.strip()
    if not project:
        return None
    parts = [segment for segment in parsed.path.split("/") if segment]
    if not parts:
        return None
    return project, parts[-1]


def section_write_violations(
    sql: str,
    *,
    engine: str,
    section: Literal["up", "down"],
) -> tuple[WriteViolation, ...]:
    effective_engine = _normalize_engine(engine)
    if effective_engine not in {"bigquery", "mysql", "clickhouse"}:
        return ()

    violations: list[WriteViolation] = []
    for statement in parsed_statements(sql, effective_engine):
        folded = compact(statement.sql)
        if not folded or _is_nonsemantic_statement(folded, effective_engine):
            continue

        if statement.expr is None:
            if _looks_mutating(folded):
                violations.append(
                    WriteViolation(
                        section=section,
                        target="<unknown>",
                        statement=folded,
                        reason="unsupported mutating syntax",
                    )
                )
            continue

        target = _write_target(statement.expr)
        if target is None:
            if _looks_mutating(folded):
                violations.append(
                    WriteViolation(
                        section=section,
                        target="<unknown>",
                        statement=folded,
                        reason="unsupported mutating syntax",
                    )
                )
            continue

        if _is_qualified_target(target, effective_engine):
            violations.append(
                WriteViolation(
                    section=section,
                    target=target.sql(dialect=_dialect_for_engine(effective_engine)),
                    statement=folded,
                    reason="qualified write target",
                )
            )
    return tuple(violations)


def schema_fingerprint(text: str, *, engine: str, context_url: str | None) -> str:
    effective_engine = _normalize_engine(engine or engine_from_url(context_url))
    default_identity = _target_identity_from_url(context_url, effective_engine)
    normalized: list[str] = []
    for statement in parsed_statements(text, effective_engine):
        folded = compact(statement.sql)
        if not folded:
            continue
        if _is_nonsemantic_statement(folded, effective_engine):
            continue
        if statement.expr is None:
            fallback = _fallback_compare_statement(folded, engine=effective_engine)
            if fallback:
                normalized.append(fallback)
            continue
        canonical = _canonical_statement(
            statement.expr,
            engine=effective_engine,
            default_identity=default_identity,
        )
        if canonical:
            normalized.append(canonical)
    return "\n".join(normalized)


def anchor_statements(text: str, *, engine: str, target_url: str) -> tuple[str, ...]:
    effective_engine = _normalize_engine(engine or engine_from_url(target_url))
    target_identity = _target_identity_from_url(target_url, effective_engine)
    prepared_sql = prepare_sql_text(text, effective_engine)

    if effective_engine in {"sqlite", "postgres"}:
        statements: list[str] = []
        for raw in split_sql_statements(prepared_sql):
            statement = raw.strip()
            if statement:
                statements.append(statement)
        return tuple(statements)

    if effective_engine == "mysql":
        statements: list[str] = []
        for raw in split_sql_statements(prepared_sql):
            statement = raw.strip()
            if not statement:
                continue
            if _is_nonsemantic_statement(compact(statement), effective_engine):
                continue
            statements.append(statement)
        return tuple(statements)

    source_identity: _TargetIdentity | None = None
    rendered: list[str] = []
    for statement in parsed_statements(prepared_sql, effective_engine):
        folded = compact(statement.sql)
        if not folded or _is_nonsemantic_statement(folded, effective_engine):
            continue
        if statement.expr is None:
            raise ValueError(
                f"{effective_engine} anchor statement could not be parsed safely: {folded!r}"
            )

        expr = statement.expr.copy()
        if _is_schema_creation(expr):
            candidate = _statement_source_identity(expr, effective_engine, target_identity)
            source_identity = _merge_source_identity(source_identity, candidate, folded)
            continue

        target = _write_target(expr)
        if target is not None:
            candidate = _target_identity(target, effective_engine, target_identity)
            source_identity = _merge_source_identity(source_identity, candidate, folded)
        elif _looks_mutating(folded):
            raise ValueError(
                f"{effective_engine} scratch replay cannot safely retarget mutating statement: {folded!r}"
            )

        if (
            effective_engine in {"bigquery", "clickhouse"}
            and target_identity is not None
            and source_identity is not None
        ):
            expr = _retarget_expression(
                expr,
                engine=effective_engine,
                source_identity=source_identity,
                target_identity=target_identity,
            )
        rendered_statement = expr.sql(dialect=_dialect_for_engine(effective_engine))
        rendered_statement = compact(rendered_statement)
        if rendered_statement:
            rendered.append(rendered_statement)
    return tuple(rendered)


def parsed_statements(text: str, engine: str) -> tuple[_ParsedStatement, ...]:
    prepared = prepare_sql_text(text, engine)
    rows: list[_ParsedStatement] = []
    dialect = _dialect_for_engine(engine)
    for raw in split_sql_statements(prepared):
        statement = raw.strip()
        if not statement:
            continue
        try:
            expr = parse_one(statement, read=dialect)
            rows.append(_ParsedStatement(sql=statement, expr=expr, parse_error=None))
        except sqlglot_errors.ParseError as error:
            rows.append(_ParsedStatement(sql=statement, expr=None, parse_error=str(error)))
    return tuple(rows)


def prepare_sql_text(text: str, engine: str) -> str:
    prepared = strip_meta_lines(text)
    if engine == "mysql":
        prepared = _MYSQL_CONDITIONAL_COMMENT_PATTERN.sub(r"\1", prepared)
        prepared = _MYSQL_EMPTY_CONDITIONAL_PATTERN.sub("", prepared)
        lines: list[str] = []
        for raw in prepared.splitlines():
            folded = compact(raw)
            upper = folded.upper()
            if upper.startswith("SET @@GLOBAL.GTID_PURGED"):
                continue
            if upper.startswith(
                (
                    "SET SQL_LOG_BIN",
                    "SET @@SESSION.SQL_LOG_BIN",
                    "SET @@GLOBAL.SQL_LOG_BIN",
                )
            ):
                continue
            lines.append(raw)
        prepared = "\n".join(lines)
    return prepared


def _canonical_statement(
    expr: exp.Expression,
    *,
    engine: str,
    default_identity: _TargetIdentity | None,
) -> str:
    if _is_schema_creation(expr):
        return _canonical_schema_creation(expr, engine)

    statement_target = _statement_source_identity(expr, engine, default_identity)
    canonical = expr.copy()
    if statement_target is not None:
        canonical = _canonicalize_expression(
            canonical,
            engine=engine,
            source_identity=statement_target,
        )
    rendered = canonical.sql(dialect=_dialect_for_engine(engine))
    rendered = compact(rendered)
    if not rendered:
        return ""
    return _fallback_compare_statement(rendered, engine=engine)


def _canonical_schema_creation(expr: exp.Expression, engine: str) -> str:
    qualifier = " IF NOT EXISTS" if bool(expr.args.get("exists")) else ""
    if engine == "bigquery":
        return f"CREATE DATASET{qualifier} __dataset__"
    return f"CREATE DATABASE{qualifier} __db__"


def _canonicalize_expression(
    expr: exp.Expression,
    *,
    engine: str,
    source_identity: _TargetIdentity,
) -> exp.Expression:
    for table in expr.find_all(exp.Table):
        if not _matches_identity(
            table,
            engine=engine,
            identity=source_identity,
            default_identity=source_identity,
        ):
            continue
        _rewrite_table(table, engine=engine, identity=None)
    return expr


def _retarget_expression(
    expr: exp.Expression,
    *,
    engine: str,
    source_identity: _TargetIdentity,
    target_identity: _TargetIdentity,
) -> exp.Expression:
    for table in expr.find_all(exp.Table):
        if not _matches_identity(
            table,
            engine=engine,
            identity=source_identity,
            default_identity=source_identity,
        ):
            continue
        _rewrite_table(table, engine=engine, identity=target_identity)
    return expr


def _rewrite_table(table: exp.Table, *, engine: str, identity: _TargetIdentity | None) -> None:
    if engine == "bigquery":
        if identity is None:
            table.set("catalog", None)
            table.set("db", None)
            return
        table.set(
            "catalog",
            exp.to_identifier(identity.catalog) if identity.catalog is not None else None,
        )
        table.set(
            "db",
            exp.to_identifier(identity.db) if identity.db is not None else None,
        )
        return

    if identity is None:
        table.set("db", None)
        return
    table.set("db", exp.to_identifier(identity.db) if identity.db is not None else None)


def _merge_source_identity(
    current: _TargetIdentity | None,
    candidate: _TargetIdentity | None,
    statement: str,
) -> _TargetIdentity | None:
    if candidate is None:
        return current
    if current is None or current == candidate:
        return candidate
    raise ValueError(
        "Generated checkpoint SQL writes to multiple targets; cannot retarget safely. "
        f"Saw {candidate} after {current} while processing {statement!r}"
    )


def _statement_source_identity(
    expr: exp.Expression,
    engine: str,
    default_identity: _TargetIdentity | None,
) -> _TargetIdentity | None:
    target = _write_target(expr)
    if target is not None:
        return _target_identity(target, engine, default_identity)
    if _is_schema_creation(expr):
        schema_target = _schema_creation_target(expr)
        if schema_target is not None:
            return _target_identity(schema_target, engine, default_identity)
    return None


def _target_identity(
    table: exp.Table,
    engine: str,
    default_identity: _TargetIdentity | None,
) -> _TargetIdentity | None:
    catalog = _identifier_value(table.args.get("catalog"))
    db = _identifier_value(table.args.get("db"))
    if engine == "bigquery":
        if db is None and catalog is None:
            return None
        if db is None and catalog is not None:
            return _TargetIdentity(
                catalog=default_identity.catalog if default_identity else None,
                db=catalog,
            )
        if catalog is None:
            return _TargetIdentity(
                catalog=default_identity.catalog if default_identity else None,
                db=db,
            )
        return _TargetIdentity(catalog=catalog, db=db)
    if engine in {"mysql", "clickhouse", "postgres"}:
        if db is None:
            return None
        return _TargetIdentity(catalog=None, db=db)
    return None


def _matches_identity(
    table: exp.Table,
    *,
    engine: str,
    identity: _TargetIdentity,
    default_identity: _TargetIdentity | None,
) -> bool:
    return _target_identity(table, engine, default_identity) == identity


def _is_qualified_target(table: exp.Table, engine: str) -> bool:
    catalog = _identifier_value(table.args.get("catalog"))
    db = _identifier_value(table.args.get("db"))
    if engine == "bigquery":
        return bool(catalog or db)
    if engine in {"mysql", "clickhouse", "postgres"}:
        return bool(db)
    return False


def _write_target(expr: exp.Expression) -> exp.Table | None:
    if isinstance(expr, exp.Create):
        return _create_target(expr)
    if isinstance(expr, (exp.Drop, exp.Alter, exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        return _table_from_target(expr.args.get("this"))
    truncate = getattr(exp, "TruncateTable", None)
    if truncate is not None and isinstance(expr, truncate):
        return _table_from_target(expr.args.get("this"))
    return None


def _create_target(expr: exp.Create) -> exp.Table | None:
    target = expr.args.get("this")
    if isinstance(target, exp.Index):
        table = target.args.get("table")
        return table if isinstance(table, exp.Table) else None
    return _table_from_target(target)


def _schema_creation_target(expr: exp.Expression) -> exp.Table | None:
    if not isinstance(expr, exp.Create):
        return None
    if str(expr.args.get("kind", "")).upper() not in {"SCHEMA", "DATABASE"}:
        return None
    return _table_from_target(expr.args.get("this"))


def _table_from_target(target: exp.Expression | None) -> exp.Table | None:
    if isinstance(target, exp.Schema):
        target = target.args.get("this")
    return target if isinstance(target, exp.Table) else None


def _is_schema_creation(expr: exp.Expression) -> bool:
    return _schema_creation_target(expr) is not None


def _fallback_compare_statement(statement: str, *, engine: str) -> str:
    result = compact(statement)
    result = normalize_identifier_quotes(result)
    if engine == "mysql":
        if result.upper().startswith("CREATE TABLE"):
            close = result.rfind(")")
            if close != -1:
                result = result[: close + 1]
        result = re.sub(r"\s+AUTO_INCREMENT=\d+\b", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\s+ROW_FORMAT=\w+\b", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\s+DEFAULT\s+CHARSET=\w+\b", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\s+CHARSET=\w+\b", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\s+COLLATE=\w+\b", "", result, flags=re.IGNORECASE)
    if engine == "clickhouse":
        result = re.sub(r"\s+SETTINGS\s+.+$", "", result, flags=re.IGNORECASE)
    return normalize_punctuation_spacing(compact(result))


def _identifier_value(node: exp.Expression | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, exp.Identifier):
        return node.name
    return str(node)


def _dialect_for_engine(engine: str) -> str:
    effective = _normalize_engine(engine)
    return "postgres" if effective == "postgres" else effective


def _normalize_engine(engine: str | None) -> str:
    if not engine:
        return ""
    lowered = engine.lower()
    if lowered == "postgresql":
        return "postgres"
    return lowered


def _target_identity_from_url(context_url: str | None, engine: str) -> _TargetIdentity | None:
    if not context_url:
        return None
    parsed = urlsplit(context_url)
    path = parsed.path.strip("/")
    if engine == "bigquery":
        if not parsed.netloc:
            return None
        parts = [segment for segment in path.split("/") if segment]
        if not parts:
            return None
        return _TargetIdentity(catalog=parsed.netloc.strip(), db=parts[-1])
    if engine in {"mysql", "clickhouse", "postgres"}:
        parts = [segment for segment in path.split("/") if segment]
        if not parts:
            return None
        return _TargetIdentity(catalog=None, db=parts[-1])
    return None


def _is_nonsemantic_statement(statement: str, engine: str) -> bool:
    upper = statement.upper()
    if upper.startswith("--"):
        return True
    if engine == "postgres":
        return upper.startswith(("SET ", "SELECT PG_CATALOG.SET_CONFIG"))
    if engine == "mysql":
        return (
            upper.startswith(("SET ", "LOCK TABLES", "UNLOCK TABLES", "START TRANSACTION"))
            or upper == "COMMIT"
        )
    if engine == "clickhouse":
        return upper.startswith("SET ")
    return False


def _looks_mutating(statement: str) -> bool:
    return _MUTATING_VERB_PATTERN.match(statement) is not None


__all__ = [
    "WriteViolation",
    "anchor_statements",
    "bigquery_target_from_url",
    "engine_from_url",
    "schema_fingerprint",
    "section_write_violations",
]
