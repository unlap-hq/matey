from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import urlsplit

from sqlglot import errors as sqlglot_errors
from sqlglot import exp, parse

from matey.bqemu import parse_bigquery_emulator_url

from .policy import EnginePolicy, normalize_engine, policy_for_engine
from .source import SqlTextDecodeError, aligned_source_statements

_MYSQL_CONDITIONAL_COMMENT_PATTERN = re.compile(r"/\*![0-9]{5}\s+(.*?)\*/", re.IGNORECASE)
_MYSQL_EMPTY_CONDITIONAL_PATTERN = re.compile(r"/\*![0-9]{5}\s*\*/", re.IGNORECASE)
_MUTATING_VERB_PATTERN = re.compile(
    r"^(CREATE|ALTER|DROP|INSERT|MERGE|UPDATE|DELETE|TRUNCATE|RENAME|REPLACE)\b",
    re.IGNORECASE,
)


class SqlError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class WriteViolation:
    section: Literal["up", "down"]
    target: str
    statement: str
    reason: str

    def excerpt(self, *, limit: int = 120) -> str:
        folded = _compact(self.statement)
        return folded if len(folded) <= limit else f"{folded[: limit - 3]}..."


@dataclass(frozen=True, slots=True)
class _TargetIdentity:
    catalog: str | None
    db: str | None


def engine_from_url(context_url: str | None) -> str:
    if not context_url:
        return ""
    if context_url.startswith("sqlite3:"):
        return "sqlite"
    parsed = urlsplit(context_url)
    return normalize_engine(parsed.scheme.split("+", 1)[0])


def bigquery_target_from_url(context_url: str | None) -> tuple[str, str] | None:
    identity = _target_identity_from_url(
        context_url,
        policy_for_engine(engine_from_url(context_url) or "bigquery"),
    )
    if identity is None or identity.catalog is None or identity.db is None:
        return None
    return identity.catalog, identity.db


def has_executable_sql(sql: str, *, engine: str) -> bool:
    policy = policy_for_engine(engine)
    return any(not _is_nonsemantic_expr(expr, policy) for expr in _parse_expressions(sql, policy))


def section_write_violations(
    sql: str,
    *,
    engine: str,
    section: Literal["up", "down"],
) -> tuple[WriteViolation, ...]:
    policy = policy_for_engine(engine)
    if not policy.guarded_writes:
        return ()

    violations: list[WriteViolation] = []
    for expr in _parse_expressions(sql, policy):
        if _is_nonsemantic_expr(expr, policy):
            continue
        statement = _render(expr, policy)
        target = _write_target(expr)
        if target is None:
            if _looks_mutating_expr(expr, statement):
                violations.append(
                    WriteViolation(
                        section=section,
                        target="<unknown>",
                        statement=statement,
                        reason="unsupported mutating syntax",
                    )
                )
            continue
        if _is_qualified_target(target, policy):
            violations.append(
                WriteViolation(
                    section=section,
                    target=target.sql(dialect=policy.dialect),
                    statement=statement,
                    reason="qualified write target",
                )
            )
    return tuple(violations)


def schema_fingerprint(text: str, *, engine: str, context_url: str | None) -> str:
    policy = policy_for_engine(engine or engine_from_url(context_url))
    default_identity = _target_identity_from_url(context_url, policy)
    normalized: list[str] = []
    for expr in _parse_expressions(text, policy):
        if _is_nonsemantic_expr(expr, policy) or _is_internal_migration_expr(expr):
            continue
        if isinstance(expr, exp.Command):
            raise SqlError(
                f"{policy.name or 'unknown'} schema statement is not supported safely: {_render(expr, policy)!r}"
            )
        canonical = _canonical_statement(
            expr,
            policy=policy,
            default_identity=default_identity,
        )
        if canonical:
            normalized.append(canonical)
    return "\n".join(normalized)


def anchor_statements(text: str, *, engine: str, target_url: str) -> tuple[str, ...]:
    policy = policy_for_engine(engine or engine_from_url(target_url))
    target_identity = _target_identity_from_url(target_url, policy)
    if policy.name in {"sqlite", "postgres"}:
        return _validated_source_anchor_statements(text, policy)
    source_identity: _TargetIdentity | None = None
    rendered: list[str] = []
    for expr in _parse_expressions(text, policy):
        if _is_nonsemantic_expr(expr, policy):
            continue

        statement = _render(expr, policy)
        if _is_schema_creation(expr):
            candidate = _statement_target_identity(expr, policy, target_identity)
            source_identity = _merge_source_identity(source_identity, candidate, statement)
            continue

        target = _write_target(expr)
        if target is not None:
            candidate = _target_identity(target, policy, target_identity)
            source_identity = _merge_source_identity(source_identity, candidate, statement)
        elif _looks_mutating_expr(expr, statement):
            raise SqlError(
                f"{policy.name or 'unknown'} scratch replay cannot safely retarget mutating statement: {statement!r}"
            )

        output = expr.copy()
        if (
            policy.checkpoint_retarget
            and target_identity is not None
            and source_identity is not None
        ):
            output = _retarget_expression(
                output,
                policy=policy,
                source_identity=source_identity,
                target_identity=target_identity,
            )
        rendered_statement = _render(output, policy)
        if rendered_statement:
            rendered.append(rendered_statement)
    return tuple(rendered)


def _validated_source_anchor_statements(text: str, policy: EnginePolicy) -> tuple[str, ...]:
    expressions = tuple(
        expr for expr in _parse_expressions(text, policy) if not isinstance(expr, exp.Semicolon)
    )
    if policy.name == "sqlite" and any(
        _render(expr, policy).upper().startswith("CREATE TRIGGER") for expr in expressions
    ):
        raise SqlError(
            "sqlite trigger bodies are not supported safely in source-preserving replay."
        )
    try:
        # Postgres/sqlite replay intentionally preserves validated source text
        # instead of re-rendering SQL through sqlglot, because re-rendering can
        # introduce harmless but noisy normalization and, in edge cases, backend-
        # specific behavior drift.
        source_statements = aligned_source_statements(
            _prepare_sql_text(text, policy),
            expected_count=len(expressions),
            label=policy.name or "unknown",
        )
    except SqlTextDecodeError as error:
        raise SqlError(str(error)) from error
    return tuple(
        statement
        for statement, expr in zip(source_statements, expressions, strict=True)
        if not _is_nonsemantic_expr(expr, policy)
    )


def _parse_expressions(text: str, policy: EnginePolicy) -> tuple[exp.Expression, ...]:
    prepared = _prepare_sql_text(text, policy)
    try:
        expressions = parse(prepared, read=policy.dialect or None)
    except sqlglot_errors.ParseError as error:
        raise SqlError(f"{policy.name or 'unknown'} SQL parse failed: {error}") from error
    return tuple(expr for expr in expressions if expr is not None)


def _prepare_sql_text(text: str, policy: EnginePolicy) -> str:
    prepared = _strip_meta_lines(text)
    if policy.name != "mysql":
        return prepared

    prepared = _MYSQL_CONDITIONAL_COMMENT_PATTERN.sub(r"\1", prepared)
    prepared = _MYSQL_EMPTY_CONDITIONAL_PATTERN.sub("", prepared)
    lines: list[str] = []
    for raw in prepared.splitlines():
        folded = _compact(raw).upper()
        if folded.startswith("SET @@GLOBAL.GTID_PURGED"):
            continue
        if folded.startswith(
            (
                "SET SQL_LOG_BIN",
                "SET @@SESSION.SQL_LOG_BIN",
                "SET @@GLOBAL.SQL_LOG_BIN",
            )
        ):
            continue
        lines.append(raw)
    return "\n".join(lines)


def _strip_meta_lines(text: str) -> str:
    lines: list[str] = []
    for raw in text.splitlines():
        if raw.strip().startswith("\\"):
            continue
        lines.append(raw)
    return "\n".join(lines)


def _canonical_statement(
    expr: exp.Expression,
    *,
    policy: EnginePolicy,
    default_identity: _TargetIdentity | None,
) -> str:
    if _is_schema_creation(expr):
        return _canonical_schema_creation(expr, policy)

    canonical = expr.copy()
    _strip_compare_noise(canonical, policy)
    statement_target = _statement_target_identity(canonical, policy, default_identity)
    if statement_target is not None and policy.target_kind != "none":
        canonical = _canonicalize_expression(
            canonical,
            policy=policy,
            source_identity=statement_target,
        )
    if policy.target_kind != "bigquery":
        canonical = _unquote_simple_identifiers(canonical)
    return _render(canonical, policy)


def _canonical_schema_creation(expr: exp.Expression, policy: EnginePolicy) -> str:
    qualifier = " IF NOT EXISTS" if bool(expr.args.get("exists")) else ""
    if policy.target_kind == "bigquery":
        return f"CREATE DATASET{qualifier} __dataset__"
    if policy.target_kind == "database":
        return f"CREATE DATABASE{qualifier} __db__"
    return _render(expr.copy(), policy)


def _canonicalize_expression(
    expr: exp.Expression,
    *,
    policy: EnginePolicy,
    source_identity: _TargetIdentity,
) -> exp.Expression:
    for table in expr.find_all(exp.Table):
        if not _matches_identity(
            table,
            policy=policy,
            identity=source_identity,
            default_identity=source_identity,
        ):
            continue
        _rewrite_table(table, policy=policy, identity=None)
    return expr


def _retarget_expression(
    expr: exp.Expression,
    *,
    policy: EnginePolicy,
    source_identity: _TargetIdentity,
    target_identity: _TargetIdentity,
) -> exp.Expression:
    for table in expr.find_all(exp.Table):
        if not _matches_identity(
            table,
            policy=policy,
            identity=source_identity,
            default_identity=source_identity,
        ):
            continue
        _rewrite_table(table, policy=policy, identity=target_identity)
    return expr


def _rewrite_table(
    table: exp.Table,
    *,
    policy: EnginePolicy,
    identity: _TargetIdentity | None,
) -> None:
    if policy.target_kind == "bigquery":
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
    if policy.target_kind == "database":
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
    raise SqlError(
        "Generated checkpoint SQL writes to multiple targets; cannot retarget safely. "
        f"Saw {candidate} after {current} while processing {statement!r}"
    )


def _statement_target_identity(
    expr: exp.Expression,
    policy: EnginePolicy,
    default_identity: _TargetIdentity | None,
) -> _TargetIdentity | None:
    target = _write_target(expr)
    if target is not None:
        return _target_identity(target, policy, default_identity)
    if _is_schema_creation(expr):
        schema_target = _schema_creation_target(expr)
        if schema_target is not None:
            return _target_identity(schema_target, policy, default_identity)
    return None


def _target_identity(
    table: exp.Table,
    policy: EnginePolicy,
    default_identity: _TargetIdentity | None,
) -> _TargetIdentity | None:
    catalog = _identifier_value(table.args.get("catalog"))
    db = _identifier_value(table.args.get("db"))
    if policy.target_kind == "bigquery":
        if db is not None and catalog is None and "." in db:
            catalog, db = db.split(".", 1)
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
    if policy.target_kind == "database":
        if db is None:
            return None
        return _TargetIdentity(catalog=None, db=db)
    return None


def _matches_identity(
    table: exp.Table,
    *,
    policy: EnginePolicy,
    identity: _TargetIdentity,
    default_identity: _TargetIdentity | None,
) -> bool:
    return _target_identity(table, policy, default_identity) == identity


def _is_qualified_target(table: exp.Table, policy: EnginePolicy) -> bool:
    catalog = _identifier_value(table.args.get("catalog"))
    db = _identifier_value(table.args.get("db"))
    if policy.target_kind == "bigquery":
        return bool(catalog or db)
    if policy.target_kind == "database":
        return bool(db)
    return False


def _write_target(expr: exp.Expression) -> exp.Table | None:
    if isinstance(expr, exp.Create):
        return _create_target(expr)
    if isinstance(expr, (exp.Drop, exp.Alter, exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        return _table_from_target(expr.args.get("this"))
    truncate = getattr(exp, "TruncateTable", None)
    if truncate is not None and isinstance(expr, truncate):
        expressions = expr.args.get("expressions") or ()
        return expressions[0] if expressions and isinstance(expressions[0], exp.Table) else None
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


def _identifier_value(node: exp.Expression | None) -> str | None:
    if node is None:
        return None
    if isinstance(node, exp.Identifier):
        return node.name
    return str(node)


def _unquote_simple_identifiers(expr: exp.Expression) -> exp.Expression:
    for identifier in expr.find_all(exp.Identifier):
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$]*", identifier.name):
            identifier.set("quoted", False)
    return expr


def _target_identity_from_url(
    context_url: str | None,
    policy: EnginePolicy,
) -> _TargetIdentity | None:
    if not context_url:
        return None
    parsed = urlsplit(context_url)
    path = parsed.path.strip("/")
    if policy.target_kind == "bigquery":
        if policy.name == "bigquery-emulator":
            try:
                _hostport, project, _location, dataset = parse_bigquery_emulator_url(context_url)
            except ValueError:
                return None
            return _TargetIdentity(catalog=project, db=dataset)
        parts = [segment for segment in path.split("/") if segment]
        if not parsed.netloc or not parts:
            return None
        return _TargetIdentity(catalog=parsed.netloc.strip(), db=parts[-1])
    if policy.target_kind == "database":
        parts = [segment for segment in path.split("/") if segment]
        if not parts:
            return None
        return _TargetIdentity(catalog=None, db=parts[-1])
    return None


def _is_nonsemantic_expr(expr: exp.Expression, policy: EnginePolicy) -> bool:
    if isinstance(expr, exp.Semicolon):
        return True

    if _starts_with_nonsemantic_sql(expr, policy):
        return True
    if _is_nonsemantic_command(expr, policy):
        return True
    if policy.name == "mysql" and isinstance(expr, (exp.Transaction, exp.Commit)):
        return True
    return policy.name == "postgres" and _is_postgres_set_config(expr)


def _is_postgres_set_config(expr: exp.Expression) -> bool:
    if not isinstance(expr, exp.Select) or len(expr.expressions) != 1:
        return False
    value = expr.expressions[0]
    if isinstance(value, exp.Anonymous):
        return value.name.lower() == "set_config"
    if isinstance(value, exp.Dot):
        return (
            isinstance(value.this, exp.Identifier)
            and value.this.name.lower() == "pg_catalog"
            and isinstance(value.expression, exp.Anonymous)
            and value.expression.name.lower() == "set_config"
        )
    return False


def _is_internal_migration_expr(expr: exp.Expression) -> bool:
    target = _write_target(expr)
    if target is None:
        return False
    return _identifier_value(target.args.get("this")) == "schema_migrations"


def _looks_mutating_expr(expr: exp.Expression, statement: str) -> bool:
    if _write_target(expr) is not None:
        return True
    if isinstance(expr, exp.Command):
        command = expr.args.get("this")
        prefix = str(command).upper().strip() if command is not None else statement
        return _MUTATING_VERB_PATTERN.match(prefix) is not None
    truncate = getattr(exp, "TruncateTable", None)
    return truncate is not None and isinstance(expr, truncate)


def _render(expr: exp.Expression, policy: EnginePolicy) -> str:
    return expr.sql(dialect=policy.dialect or None)


def _compact(text: str) -> str:
    return " ".join(text.split())


def _strip_compare_noise(expr: exp.Expression, policy: EnginePolicy) -> None:
    if not isinstance(expr, exp.Create):
        return
    properties = expr.args.get("properties")
    if not isinstance(properties, exp.Properties):
        return

    keep: list[exp.Expression] = []
    for prop in properties.expressions:
        if policy.name == "mysql":
            if isinstance(
                prop,
                (
                    exp.AutoIncrementProperty,
                    exp.RowFormatProperty,
                    exp.CharacterSetProperty,
                    exp.CollateProperty,
                ),
            ):
                continue
            if (
                isinstance(prop, exp.EngineProperty)
                and (_identifier_value(prop.args.get("this")) or "").lower() == "innodb"
            ):
                continue
        if policy.name == "clickhouse" and isinstance(prop, exp.SettingsProperty):
            continue
        keep.append(prop)

    if keep:
        properties.set("expressions", keep)
    else:
        expr.set("properties", None)


def _starts_with_nonsemantic_sql(expr: exp.Expression, policy: EnginePolicy) -> bool:
    if not policy.nonsemantic_sql_prefixes:
        return False
    return _render(expr, policy).upper().startswith(policy.nonsemantic_sql_prefixes)


def _is_nonsemantic_command(expr: exp.Expression, policy: EnginePolicy) -> bool:
    if not isinstance(expr, exp.Command) or not policy.nonsemantic_command_prefixes:
        return False
    command = expr.args.get("this")
    prefix = str(command).upper().strip() if command is not None else ""
    return prefix.startswith(policy.nonsemantic_command_prefixes)


__all__ = [
    "SqlError",
    "WriteViolation",
    "anchor_statements",
    "bigquery_target_from_url",
    "engine_from_url",
    "has_executable_sql",
    "schema_fingerprint",
    "section_write_violations",
]
