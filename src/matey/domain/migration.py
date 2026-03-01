from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

_SECTION_MARKER_PATTERN = re.compile(r"^\s*--\s*migrate:(up|down)\b.*$", re.IGNORECASE | re.MULTILINE)
_DOWN_MARKER_PATTERN = re.compile(r"^\s*--\s*migrate:down\b.*$", re.IGNORECASE | re.MULTILINE)
_LINE_COMMENT_ONLY_PATTERN = re.compile(r"^\s*--.*$", re.MULTILINE)
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)


@dataclass(frozen=True)
class MigrationFile:
    version: str
    filename: str
    rel_path: str


@dataclass(frozen=True)
class DownSectionState:
    marker_present: bool
    has_executable_sql: bool


def _version_from_filename(filename: str) -> str:
    stem = Path(filename).stem
    if "_" in stem:
        prefix, _ = stem.split("_", 1)
        return prefix or stem
    return stem


def parse_migration_files(file_paths: Iterable[str]) -> tuple[MigrationFile, ...]:
    rows: list[MigrationFile] = []
    for raw_path in file_paths:
        rel = Path(raw_path).as_posix()
        name = Path(raw_path).name
        if Path(name).suffix.lower() != ".sql":
            continue
        rows.append(MigrationFile(version=_version_from_filename(name), filename=name, rel_path=rel))
    rows.sort(key=lambda row: row.filename)
    return tuple(rows)


def _extract_down_section(migration_text: str) -> tuple[bool, str]:
    down_match = _DOWN_MARKER_PATTERN.search(migration_text)
    if down_match is None:
        return False, ""
    section_start = down_match.end()
    next_section = _SECTION_MARKER_PATTERN.search(migration_text, section_start)
    section_end = next_section.start() if next_section is not None else len(migration_text)
    return True, migration_text[section_start:section_end]


def _contains_executable_sql(section_text: str) -> bool:
    without_block_comments = _BLOCK_COMMENT_PATTERN.sub("", section_text)
    without_line_comments = _LINE_COMMENT_ONLY_PATTERN.sub("", without_block_comments)
    stripped = re.sub(r"[;\s]+", "", without_line_comments)
    return bool(stripped)


def parse_down_section_state(sql_text: str) -> DownSectionState:
    marker_present, section = _extract_down_section(sql_text)
    return DownSectionState(marker_present=marker_present, has_executable_sql=_contains_executable_sql(section))
