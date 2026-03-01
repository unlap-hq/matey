from __future__ import annotations

import re
from dataclasses import dataclass

from matey.domain.errors import ExternalCommandError

_STATUS_LINE_PATTERN = re.compile(r"^\[(?P<mark>[ X])\]\s+(?P<file>.+?)\s*$")


@dataclass(frozen=True)
class DbStatusSnapshot:
    applied_files: tuple[str, ...]
    applied_count: int


@dataclass(frozen=True)
class DbmateOutput:
    exit_code: int
    stdout: str
    stderr: str


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
