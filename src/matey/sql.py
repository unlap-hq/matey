from __future__ import annotations

import difflib
import re
from dataclasses import dataclass
from functools import cached_property
from typing import Literal
from urllib.parse import urlsplit

from sqlglot import errors as sqlglot_errors
from sqlglot import exp, parse_one

_UP_MARKER = "-- migrate:up"
_DOWN_MARKER = "-- migrate:down"
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)
_MYSQL_CONDITIONAL_COMMENT_PATTERN = re.compile(r"/\*![0-9]{5}\s+(.*?)\*/", re.IGNORECASE)
_MYSQL_EMPTY_CONDITIONAL_PATTERN = re.compile(r"/\*![0-9]{5}\s*\*/", re.IGNORECASE)
_MUTATING_VERB_PATTERN = re.compile(
    r"^(CREATE|ALTER|DROP|INSERT|MERGE|UPDATE|DELETE|TRUNCATE|RENAME|REPLACE)\b",
    re.IGNORECASE,
)
_BIGQUERY_MULTI_REGION = {"us", "eu"}


@dataclass(frozen=True, slots=True)
class WriteViolation:
    section: Literal["up", "down"]
    target: str
    statement: str
    reason: str

    def excerpt(self, *, limit: int = 120) -> str:
        compact = _compact(self.statement)
        return compact if len(compact) <= limit else f"{compact[:limit - 3]}..."


@dataclass(frozen=True, slots=True)
class _TargetIdentity:
    catalog: str | None
    db: str | None


@dataclass(frozen=True, slots=True)
class _ParsedStatement:
    sql: str
    expr: exp.Expression | None
    parse_error: str | None


class SqlProgram:
    def __init__(self, text: str, *, engine: str) -> None:
        self._text = text
        self._engine = _normalize_engine(engine)

    @property
    def engine(self) -> str:
        return self._engine

    @property
    def up_sql(self) -> str:
        return self._sections[0]

    @property
    def down_sql(self) -> str:
        return self._sections[1]

    @cached_property
    def _sections(self) -> tuple[str, str]:
        return split_migration_sections(self._text)

    def has_executable_down(self) -> bool:
        return has_executable_sql(self.down_sql)

    def section_write_violations(
        self,
        section: Literal["up", "down"],
    ) -> tuple[WriteViolation, ...]:
        sql = self.up_sql if section == "up" else self.down_sql
        if section == "down" and not has_executable_sql(sql):
            return ()
        return _section_write_violations(sql, engine=self._engine, section=section)

    def migration_write_violations(self) -> tuple[WriteViolation, ...]:
        return self.section_write_violations("up") + self.section_write_violations("down")

    def schema_fingerprint(self, *, context_url: str | None = None) -> str:
        return _schema_fingerprint(self._text, engine=self._engine, context_url=context_url)

    def schema_equals(
        self,
        other: SqlProgram,
        *,
        left_context_url: str | None = None,
        right_context_url: str | None = None,
    ) -> bool:
        return self.schema_fingerprint(context_url=left_context_url) == other.schema_fingerprint(
            context_url=right_context_url
        )

    def schema_diff(
        self,
        other: SqlProgram,
        *,
        left_label: str,
        right_label: str,
        left_context_url: str | None = None,
        right_context_url: str | None = None,
    ) -> str:
        return unified_sql_diff(
            left_sql=self.schema_fingerprint(context_url=left_context_url),
            right_sql=other.schema_fingerprint(context_url=right_context_url),
            left_label=left_label,
            right_label=right_label,
        )

    def anchor_statements(self, *, target_url: str) -> tuple[str, ...]:
        return _anchor_statements(self._text, engine=self._engine, target_url=target_url)


def ensure_newline(text: str) -> str:
    return text if text.endswith("\n") else f"{text}\n"


def normalize_sql(text: str) -> str:
    lines = [line.rstrip() for line in text.splitlines()]
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines).strip()


def unified_sql_diff(
    *,
    left_sql: str,
    right_sql: str,
    left_label: str,
    right_label: str,
) -> str:
    left_lines = ensure_newline(left_sql).splitlines(keepends=True)
    right_lines = ensure_newline(right_sql).splitlines(keepends=True)
    return "".join(
        difflib.unified_diff(
            left_lines,
            right_lines,
            fromfile=left_label,
            tofile=right_label,
            lineterm="",
        )
    )


def split_migration_sections(text: str) -> tuple[str, str]:
    up_lines: list[str] = []
    down_lines: list[str] = []
    current = up_lines
    for line in text.splitlines(keepends=True):
        stripped = line.strip().lower()
        if stripped == _UP_MARKER:
            current = up_lines
            continue
        if stripped == _DOWN_MARKER:
            current = down_lines
            continue
        current.append(line)
    return "".join(up_lines), "".join(down_lines)


def migration_sections(text: str) -> tuple[str, str]:
    return split_migration_sections(text)


def has_executable_sql(sql: str) -> bool:
    without_blocks = _BLOCK_COMMENT_PATTERN.sub("", sql)
    significant_lines: list[str] = []
    for line in without_blocks.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("--"):
            continue
        significant_lines.append(stripped)

    if not significant_lines:
        return False

    compact = "".join(significant_lines).replace(";", "").strip()
    return bool(compact)


def split_sql_statements(text: str) -> tuple[str, ...]:
    statements: list[str] = []
    buffer: list[str] = []
    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False
    dollar_tag: str | None = None
    i = 0
    while i < len(text):
        char = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if dollar_tag is not None:
            if text.startswith(dollar_tag, i):
                buffer.append(dollar_tag)
                i += len(dollar_tag)
                dollar_tag = None
                continue
            buffer.append(char)
            i += 1
            continue

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            i += 1
            continue

        if in_block_comment:
            if char == "*" and nxt == "/":
                i += 2
                in_block_comment = False
                continue
            i += 1
            continue

        if in_single:
            buffer.append(char)
            if char == "'" and nxt == "'":
                buffer.append(nxt)
                i += 2
                continue
            if char == "'" and (i == 0 or text[i - 1] != "\\"):
                in_single = False
            i += 1
            continue

        if in_double:
            buffer.append(char)
            if char == '"' and nxt == '"':
                buffer.append(nxt)
                i += 2
                continue
            if char == '"' and (i == 0 or text[i - 1] != "\\"):
                in_double = False
            i += 1
            continue

        if in_backtick:
            buffer.append(char)
            if char == "`":
                in_backtick = False
            i += 1
            continue

        if char == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue
        if char == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue

        dollar_match = _match_dollar_quote(text, i)
        if dollar_match is not None:
            dollar_tag = dollar_match
            buffer.append(dollar_tag)
            i += len(dollar_tag)
            continue

        if char == "'":
            in_single = True
            buffer.append(char)
            i += 1
            continue
        if char == '"':
            in_double = True
            buffer.append(char)
            i += 1
            continue
        if char == "`":
            in_backtick = True
            buffer.append(char)
            i += 1
            continue

        if char == ";":
            statement = "".join(buffer).strip()
            if statement:
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(char)
        i += 1

    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return tuple(statements)


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


def engine_from_url(context_url: str | None) -> str:
    if not context_url:
        return ""
    if context_url.startswith("sqlite3:"):
        return "sqlite"
    parsed = urlsplit(context_url)
    return _normalize_engine(parsed.scheme.split("+", 1)[0])


def compare_schema_sql(
    left_sql: str,
    right_sql: str,
    *,
    engine: str,
    left_context_url: str | None = None,
    right_context_url: str | None = None,
    ) -> bool:
    return SqlProgram(left_sql, engine=engine).schema_equals(
        SqlProgram(right_sql, engine=engine),
        left_context_url=left_context_url,
        right_context_url=right_context_url,
    )


def normalize_sql_for_compare(
    text: str,
    *,
    context_url: str | None = None,
    engine: str | None = None,
) -> str:
    effective_engine = _normalize_engine(engine or engine_from_url(context_url))
    return SqlProgram(text, engine=effective_engine).schema_fingerprint(context_url=context_url)


def qualified_write_targets(
    sql: str,
    *,
    engine: str | None,
) -> tuple[WriteViolation, ...]:
    effective_engine = _normalize_engine(engine)
    return _section_write_violations(sql, engine=effective_engine, section="up")


def schema_sql_diff(
    left_sql: str,
    right_sql: str,
    *,
    engine: str,
    left_label: str,
    right_label: str,
    left_context_url: str | None = None,
    right_context_url: str | None = None,
) -> str:
    return SqlProgram(left_sql, engine=engine).schema_diff(
        SqlProgram(right_sql, engine=engine),
        left_label=left_label,
        right_label=right_label,
        left_context_url=left_context_url,
        right_context_url=right_context_url,
    )


def _match_dollar_quote(text: str, start: int) -> str | None:
    if text[start] != "$":
        return None
    end = start + 1
    while end < len(text) and (text[end].isalnum() or text[end] == "_"):
        end += 1
    if end < len(text) and text[end] == "$":
        return text[start : end + 1]
    return None


def _section_write_violations(
    sql: str,
    *,
    engine: str,
    section: Literal["up", "down"],
) -> tuple[WriteViolation, ...]:
    effective_engine = _normalize_engine(engine)
    if effective_engine not in {"bigquery", "mysql", "clickhouse"}:
        return ()

    violations: list[WriteViolation] = []
    for statement in _parsed_statements(sql, effective_engine):
        compact = _compact(statement.sql)
        if not compact or _is_nonsemantic_statement(compact, effective_engine):
            continue

        if statement.expr is None:
            if _looks_mutating(compact):
                violations.append(
                    WriteViolation(
                        section=section,
                        target="<unknown>",
                        statement=compact,
                        reason="unsupported mutating syntax",
                    )
                )
            continue

        target = _write_target(statement.expr)
        if target is None:
            if _looks_mutating(compact):
                violations.append(
                    WriteViolation(
                        section=section,
                        target="<unknown>",
                        statement=compact,
                        reason="unsupported mutating syntax",
                    )
                )
            continue

        if _is_qualified_target(target, effective_engine):
            violations.append(
                WriteViolation(
                    section=section,
                    target=_target_sql(target, effective_engine),
                    statement=compact,
                    reason="qualified write target",
                )
            )
    return tuple(violations)


def _schema_fingerprint(text: str, *, engine: str, context_url: str | None) -> str:
    effective_engine = _normalize_engine(engine or engine_from_url(context_url))
    default_identity = _target_identity_from_url(context_url, effective_engine)
    normalized: list[str] = []
    for statement in _parsed_statements(text, effective_engine):
        compact = _compact(statement.sql)
        if not compact:
            continue
        if _is_nonsemantic_statement(compact, effective_engine):
            continue
        if statement.expr is None:
            fallback = _fallback_compare_statement(compact, engine=effective_engine)
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


def _anchor_statements(text: str, *, engine: str, target_url: str) -> tuple[str, ...]:
    effective_engine = _normalize_engine(engine or engine_from_url(target_url))
    target_identity = _target_identity_from_url(target_url, effective_engine)
    prepared_sql = _prepare_sql_text(text, effective_engine)

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
            if _is_nonsemantic_statement(_compact(statement), effective_engine):
                continue
            statements.append(statement)
        return tuple(statements)

    source_identity: _TargetIdentity | None = None
    rendered: list[str] = []
    for statement in _parsed_statements(prepared_sql, effective_engine):
        compact = _compact(statement.sql)
        if not compact or _is_nonsemantic_statement(compact, effective_engine):
            continue
        if statement.expr is None:
            raise ValueError(f"{effective_engine} anchor statement could not be parsed safely: {compact!r}")

        expr = statement.expr.copy()
        if _is_schema_creation(expr):
            candidate = _statement_source_identity(expr, effective_engine, target_identity)
            source_identity = _merge_source_identity(source_identity, candidate, compact)
            continue

        target = _write_target(expr)
        if target is not None:
            candidate = _target_identity(target, effective_engine, target_identity)
            source_identity = _merge_source_identity(source_identity, candidate, compact)
        elif _looks_mutating(compact):
            raise ValueError(
                f"{effective_engine} scratch replay cannot safely retarget mutating statement: {compact!r}"
            )

        if effective_engine in {"bigquery", "clickhouse"} and target_identity is not None and source_identity is not None:
            expr = _retarget_expression(
                expr,
                engine=effective_engine,
                source_identity=source_identity,
                target_identity=target_identity,
            )
        rendered_statement = _render_statement(expr, effective_engine)
        if rendered_statement:
            rendered.append(rendered_statement)
    return tuple(rendered)


def _parsed_statements(text: str, engine: str) -> tuple[_ParsedStatement, ...]:
    prepared = _prepare_sql_text(text, engine)
    rows: list[_ParsedStatement] = []
    dialect = _dialect_for_engine(engine)
    for raw in split_sql_statements(prepared):
        compact = raw.strip()
        if not compact:
            continue
        try:
            expr = parse_one(compact, read=dialect)
            rows.append(_ParsedStatement(sql=compact, expr=expr, parse_error=None))
        except sqlglot_errors.ParseError as error:
            rows.append(_ParsedStatement(sql=compact, expr=None, parse_error=str(error)))
    return tuple(rows)


def _prepare_sql_text(text: str, engine: str) -> str:
    prepared = _strip_meta_lines(text)
    if engine == "mysql":
        prepared = _MYSQL_CONDITIONAL_COMMENT_PATTERN.sub(r"\1", prepared)
        prepared = _MYSQL_EMPTY_CONDITIONAL_PATTERN.sub("", prepared)
        lines: list[str] = []
        for raw in prepared.splitlines():
            compact = " ".join(raw.split())
            upper = compact.upper()
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
    rendered = _render_statement(canonical, engine)
    if not rendered:
        return ""
    return _fallback_compare_statement(rendered, engine=engine)


def _canonical_schema_creation(expr: exp.Expression, engine: str) -> str:
    if engine == "bigquery":
        qualifier = " IF NOT EXISTS" if bool(expr.args.get("exists")) else ""
        return f"CREATE DATASET{qualifier} __dataset__"
    qualifier = " IF NOT EXISTS" if bool(expr.args.get("exists")) else ""
    return f"CREATE DATABASE{qualifier} __db__"


def _canonicalize_expression(
    expr: exp.Expression,
    *,
    engine: str,
    source_identity: _TargetIdentity,
) -> exp.Expression:
    for table in expr.find_all(exp.Table):
        if not _matches_identity(table, engine=engine, identity=source_identity, default_identity=source_identity):
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
        if not _matches_identity(table, engine=engine, identity=source_identity, default_identity=source_identity):
            continue
        _rewrite_table(table, engine=engine, identity=target_identity)
    return expr


def _rewrite_table(table: exp.Table, *, engine: str, identity: _TargetIdentity | None) -> None:
    if engine == "bigquery":
        if identity is None:
            table.set("catalog", None)
            table.set("db", None)
            return
        if identity.catalog is None:
            table.set("catalog", None)
        else:
            table.set("catalog", exp.to_identifier(identity.catalog))
        if identity.db is None:
            table.set("db", None)
        else:
            table.set("db", exp.to_identifier(identity.db))
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
            return _TargetIdentity(catalog=default_identity.catalog if default_identity else None, db=catalog)
        if catalog is None:
            return _TargetIdentity(catalog=default_identity.catalog if default_identity else None, db=db)
        return _TargetIdentity(catalog=catalog, db=db)
    if engine in {"mysql", "clickhouse"}:
        if db is None:
            return None
        return _TargetIdentity(catalog=None, db=db)
    if engine == "postgres":
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


def _target_sql(table: exp.Table, engine: str) -> str:
    return table.sql(dialect=_dialect_for_engine(engine))


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


def _render_statement(expr: exp.Expression, engine: str) -> str:
    rendered = expr.sql(dialect=_dialect_for_engine(engine))
    return _compact(rendered)


def _fallback_compare_statement(statement: str, *, engine: str) -> str:
    result = _compact(statement)
    result = _normalize_identifier_quotes(result)
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
    return _normalize_punctuation_spacing(_compact(result))


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
    if engine in {"mysql", "clickhouse"}:
        parts = [segment for segment in path.split("/") if segment]
        if not parts:
            return None
        return _TargetIdentity(catalog=None, db=parts[-1])
    if engine == "postgres":
        parts = [segment for segment in path.split("/") if segment]
        if not parts:
            return None
        return _TargetIdentity(catalog=None, db=parts[-1])
    return None


def _strip_meta_lines(text: str) -> str:
    lines: list[str] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if stripped.startswith("\\"):
            continue
        lines.append(raw)
    return "\n".join(lines)


def _normalize_identifier_quotes(statement: str) -> str:
    without_backticks = statement.replace("`", "")
    return re.sub(r'"([A-Za-z_][A-Za-z0-9_$]*)"', r"\1", without_backticks)


def _normalize_punctuation_spacing(statement: str) -> str:
    result = re.sub(r"\(\s+", "(", statement)
    result = re.sub(r"\s+\)", ")", result)
    result = re.sub(r"\s*,\s*", ", ", result)
    return result.strip()


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


def _compact(statement: str) -> str:
    return " ".join(statement.split())


__all__ = [
    "SqlProgram",
    "WriteViolation",
    "bigquery_target_from_url",
    "compare_schema_sql",
    "engine_from_url",
    "ensure_newline",
    "has_executable_sql",
    "migration_sections",
    "normalize_sql",
    "normalize_sql_for_compare",
    "qualified_write_targets",
    "schema_sql_diff",
    "split_migration_sections",
    "split_sql_statements",
    "unified_sql_diff",
]
