from __future__ import annotations

import difflib
import re

_UP_MARKER = re.compile(r"^\s*--\s*migrate:up\b.*$", re.IGNORECASE)
_DOWN_MARKER = re.compile(r"^\s*--\s*migrate:down\b.*$", re.IGNORECASE)


class SqlTextDecodeError(ValueError):
    pass


def ensure_newline(text: str) -> str:
    return text if text.endswith("\n") else f"{text}\n"


def decode_sql_text(payload: bytes, *, label: str) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise SqlTextDecodeError(f"Unable to decode {label} as UTF-8.") from error


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
        if _is_migration_directive_line(line, marker=_UP_MARKER):
            current = up_lines
            continue
        if _is_migration_directive_line(line, marker=_DOWN_MARKER):
            current = down_lines
            continue
        current.append(line)
    return "".join(up_lines), "".join(down_lines)


def split_source_statements(text: str) -> tuple[str, ...]:
    """Split validated source text only for source-preserving replay.

    This is intentionally not a general SQL parser. The main SQL pipeline is
    `sqlglot`-first; this scanner exists only so postgres/sqlite replay can keep
    the original validated statement text instead of re-rendering SQL.
    """
    statements: list[str] = []
    buffer: list[str] = []
    in_single = False
    single_backslash_escapes = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    block_comment_depth = 0
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
            buffer.append(char)
            if char == "\n":
                in_line_comment = False
            i += 1
            continue

        if block_comment_depth:
            buffer.append(char)
            if char == "/" and nxt == "*":
                buffer.append(nxt)
                i += 2
                block_comment_depth += 1
                continue
            if char == "*" and nxt == "/":
                buffer.append(nxt)
                i += 2
                block_comment_depth -= 1
                continue
            i += 1
            continue

        if in_single:
            buffer.append(char)
            if char == "'" and nxt == "'":
                buffer.append(nxt)
                i += 2
                continue
            if char == "'" and not (single_backslash_escapes and _is_backslash_escaped(text, i)):
                in_single = False
                single_backslash_escapes = False
            i += 1
            continue

        if in_double:
            buffer.append(char)
            if char == '"' and nxt == '"':
                buffer.append(nxt)
                i += 2
                continue
            if char == '"':
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
            buffer.append(char)
            buffer.append(nxt)
            i += 2
            continue
        if char == "/" and nxt == "*":
            block_comment_depth = 1
            buffer.append(char)
            buffer.append(nxt)
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
            single_backslash_escapes = _has_escape_string_prefix(text, i)
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
            if statement and _has_sql_code(statement):
                statements.append(statement)
            buffer = []
            i += 1
            continue

        buffer.append(char)
        i += 1

    tail = "".join(buffer).strip()
    if tail and _has_sql_code(tail):
        statements.append(tail)
    return tuple(statements)


def aligned_source_statements(
    text: str,
    *,
    expected_count: int,
    label: str,
) -> tuple[str, ...]:
    statements = split_source_statements(text)
    if len(statements) != expected_count:
        raise SqlTextDecodeError(
            f"{label} anchor statements could not be aligned safely with source text."
        )
    return statements


def _is_migration_directive_line(line: str, *, marker: re.Pattern[str]) -> bool:
    return marker.fullmatch(line.rstrip("\r\n")) is not None


def _match_dollar_quote(text: str, start: int) -> str | None:
    if text[start] != "$":
        return None
    if start > 0 and _is_identifier_continue(text[start - 1]):
        return None
    if start + 1 < len(text) and text[start + 1] == "$":
        return "$$"
    end = start + 1
    if end >= len(text) or not _is_identifier_start(text[end]):
        return None
    end += 1
    while end < len(text) and _is_identifier_tag_continue(text[end]):
        end += 1
    if end < len(text) and text[end] == "$":
        return text[start : end + 1]
    return None


def _has_escape_string_prefix(text: str, quote_index: int) -> bool:
    if quote_index == 0:
        return False
    prefix_index = quote_index - 1
    if text[prefix_index] not in {"e", "E"}:
        return False
    if prefix_index == 0:
        return True
    previous = text[prefix_index - 1]
    return not (previous.isalnum() or previous == "_")


def _is_backslash_escaped(text: str, index: int) -> bool:
    count = 0
    cursor = index - 1
    while cursor >= 0 and text[cursor] == "\\":
        count += 1
        cursor -= 1
    return count % 2 == 1


def _has_sql_code(text: str) -> bool:
    in_single = False
    in_double = False
    in_backtick = False
    in_line_comment = False
    block_comment_depth = 0
    dollar_tag: str | None = None
    i = 0
    while i < len(text):
        char = text[i]
        nxt = text[i + 1] if i + 1 < len(text) else ""

        if dollar_tag is not None:
            return True

        if in_line_comment:
            if char == "\n":
                in_line_comment = False
            i += 1
            continue

        if block_comment_depth:
            if char == "/" and nxt == "*":
                block_comment_depth += 1
                i += 2
                continue
            if char == "*" and nxt == "/":
                block_comment_depth -= 1
                i += 2
                continue
            i += 1
            continue

        if in_single or in_double or in_backtick:
            return True

        if char == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue
        if char == "/" and nxt == "*":
            block_comment_depth = 1
            i += 2
            continue

        dollar_match = _match_dollar_quote(text, i)
        if dollar_match is not None:
            return True

        if char == "'":
            in_single = True
            i += 1
            continue
        if char == '"':
            in_double = True
            i += 1
            continue
        if char == "`":
            in_backtick = True
            i += 1
            continue
        if not char.isspace():
            return True
        i += 1
    return False


def _is_identifier_start(char: str) -> bool:
    return char.isalpha() or char == "_"


def _is_identifier_tag_continue(char: str) -> bool:
    return char.isalnum() or char == "_"


def _is_identifier_continue(char: str) -> bool:
    return char.isalnum() or char in {"_", "$"}


__all__ = [
    "SqlTextDecodeError",
    "decode_sql_text",
    "ensure_newline",
    "split_migration_sections",
    "unified_sql_diff",
]
