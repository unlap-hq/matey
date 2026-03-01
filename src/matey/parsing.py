from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from matey.errors import ExternalCommandError
from matey.models import CmdResult

_STATUS_LINE_PATTERN = re.compile(r"^\[(?P<mark>[ X])\]\s+(?P<file>.+?)\s*$")
_SECTION_MARKER_PATTERN = re.compile(r"^\s*--\s*migrate:(up|down)\b.*$", re.IGNORECASE | re.MULTILINE)
_DOWN_MARKER_PATTERN = re.compile(r"^\s*--\s*migrate:down\b.*$", re.IGNORECASE | re.MULTILINE)
_LINE_COMMENT_ONLY_PATTERN = re.compile(r"^\s*--.*$", re.MULTILINE)
_BLOCK_COMMENT_PATTERN = re.compile(r"/\*.*?\*/", re.DOTALL)


@dataclass(frozen=True)
class DbStatusSnapshot:
    applied_files: tuple[str, ...]
    applied_count: int


@dataclass(frozen=True)
class DbmateOutput:
    exit_code: int
    stdout: str
    stderr: str


@dataclass(frozen=True)
class MigrationFile:
    version: str
    filename: str
    rel_path: str


@dataclass(frozen=True)
class DownSectionState:
    marker_present: bool
    has_executable_sql: bool


def cmd_result_to_output(result: CmdResult) -> DbmateOutput:
    return DbmateOutput(exit_code=result.exit_code, stdout=result.stdout, stderr=result.stderr)


def require_cmd_success(result: CmdResult, message: str) -> None:
    if result.exit_code == 0:
        return
    details = (result.stderr or result.stdout or "").strip()
    if details:
        raise ExternalCommandError(f"{message}. {details}")
    raise ExternalCommandError(message)


def parse_status_output(text: str) -> DbStatusSnapshot:
    applied: list[str] = []
    explicit_count: int | None = None

    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        status_match = _STATUS_LINE_PATTERN.match(line)
        if status_match is not None:
            if status_match.group("mark") == "X":
                applied.append(status_match.group("file"))
            continue

        if line.lower().startswith("applied:"):
            count_text = line.split(":", 1)[1].strip()
            try:
                explicit_count = int(count_text)
            except ValueError as error:
                raise ExternalCommandError(
                    f"Unable to parse dbmate status applied count: {count_text!r}"
                ) from error

    if explicit_count is not None and explicit_count != len(applied):
        raise ExternalCommandError(
            "Unable to parse dbmate status output: applied count does not match listed rows."
        )

    return DbStatusSnapshot(applied_files=tuple(applied), applied_count=len(applied))


def extract_status_text(output: DbmateOutput) -> str:
    payload = output.stdout.strip()
    if payload:
        return payload
    details = (output.stderr or "").strip()
    raise ExternalCommandError(f"dbmate status produced no parseable output. {details}".strip())


def extract_dump_sql(output: DbmateOutput) -> str:
    payload = output.stdout
    if payload.strip():
        return payload
    details = (output.stderr or "").strip()
    raise ExternalCommandError(f"dbmate dump produced no schema SQL output. {details}".strip())


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
