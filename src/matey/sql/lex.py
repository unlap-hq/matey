from __future__ import annotations

import difflib
import re

_UP_MARKER = "-- migrate:up"
_DOWN_MARKER = "-- migrate:down"
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)


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


def strip_meta_lines(text: str) -> str:
    lines: list[str] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if stripped.startswith("\\"):
            continue
        lines.append(raw)
    return "\n".join(lines)


def normalize_identifier_quotes(statement: str) -> str:
    without_backticks = statement.replace("`", "")
    return re.sub(r'"([A-Za-z_][A-Za-z0-9_$]*)"', r"\1", without_backticks)


def normalize_punctuation_spacing(statement: str) -> str:
    result = re.sub(r"\(\s+", "(", statement)
    result = re.sub(r"\s+\)", ")", result)
    result = re.sub(r"\s*,\s*", ", ", result)
    return result.strip()


def compact(statement: str) -> str:
    return " ".join(statement.split())


def _match_dollar_quote(text: str, start: int) -> str | None:
    if text[start] != "$":
        return None
    end = start + 1
    while end < len(text) and (text[end].isalnum() or text[end] == "_"):
        end += 1
    if end < len(text) and text[end] == "$":
        return text[start : end + 1]
    return None


__all__ = [
    "compact",
    "ensure_newline",
    "has_executable_sql",
    "normalize_identifier_quotes",
    "normalize_punctuation_spacing",
    "normalize_sql",
    "split_migration_sections",
    "split_sql_statements",
    "strip_meta_lines",
    "unified_sql_diff",
]
