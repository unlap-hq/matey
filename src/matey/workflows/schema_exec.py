from __future__ import annotations

import subprocess
import time
from collections.abc import Callable
from pathlib import Path

from matey.workflows.schema_diff import _normalize_sql, _read_text


def _run_step_with_retry(
    *,
    retries: int,
    delay_seconds: float,
    run_step: Callable[[], int],
) -> int:
    attempts = 0
    while True:
        attempts += 1
        code = int(run_step())
        if code == 0:
            return 0
        if attempts >= retries:
            return code
        time.sleep(delay_seconds)


def _run_capture_with_retry(
    *,
    retries: int,
    delay_seconds: float,
    run_step: Callable[[], subprocess.CompletedProcess[str]],
) -> subprocess.CompletedProcess[str]:
    attempts = 0
    while True:
        attempts += 1
        result = run_step()
        if int(result.returncode) == 0:
            return result
        if attempts >= retries:
            return result
        time.sleep(delay_seconds)


def _extract_dump_schema(
    *,
    dump_result: subprocess.CompletedProcess[str],
    schema_file: Path,
) -> str:
    stdout_text = dump_result.stdout or ""
    stripped = stdout_text.lstrip().lower()
    if stdout_text and not stripped.startswith("writing:"):
        return _normalize_sql(stdout_text)
    return _normalize_sql(_read_text(schema_file))
