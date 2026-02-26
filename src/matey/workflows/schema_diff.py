from __future__ import annotations

import difflib
from pathlib import Path

from matey.domain import SchemaValidationError


def _normalize_sql(sql: str) -> str:
    normalized = sql.replace("\r\n", "\n").replace("\r", "\n")
    return normalized.strip() + "\n" if normalized.strip() else ""


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as error:
        raise SchemaValidationError(f"Schema file does not exist: {path}") from error


def read_schema_sql(path: Path) -> str:
    return _normalize_sql(_read_text(path))


def normalize_sql_text(sql: str) -> str:
    return _normalize_sql(sql)


def _schema_diff(expected: str, actual: str, *, expected_name: str, actual_name: str) -> str:
    if expected == actual:
        return ""
    diff_lines = difflib.unified_diff(
        expected.splitlines(keepends=True),
        actual.splitlines(keepends=True),
        fromfile=expected_name,
        tofile=actual_name,
    )
    return "".join(diff_lines)


def schema_diff_text(expected: str, actual: str, *, expected_name: str, actual_name: str) -> str:
    return _schema_diff(
        _normalize_sql(expected),
        _normalize_sql(actual),
        expected_name=expected_name,
        actual_name=actual_name,
    )
