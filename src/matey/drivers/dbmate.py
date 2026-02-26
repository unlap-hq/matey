from __future__ import annotations

import platform
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from matey.env import load_runtime_env

_SYSTEM_TO_GOOS = {
    "Darwin": "darwin",
    "Linux": "linux",
    "Windows": "windows",
}

_MACHINE_TO_GOARCH = {
    "x86_64": "amd64",
    "amd64": "amd64",
    "AMD64": "amd64",
    "aarch64": "arm64",
    "arm64": "arm64",
}


@dataclass(frozen=True)
class DbmateLogContext:
    target: str | None = None
    phase: str | None = None
    step: str | None = None


@dataclass(frozen=True)
class DbmateExecutionResult:
    command: tuple[str, ...]
    display_command: tuple[str, ...]
    returncode: int
    duration_seconds: float
    stdout: str
    stderr: str
    verb: str
    context: DbmateLogContext | None = None
    captured: bool = False

    @property
    def ok(self) -> bool:
        return self.returncode == 0


DbmateResultCallback = Callable[[DbmateExecutionResult], None]


def _normalized_platform() -> tuple[str, str]:
    system = platform.system()
    machine = platform.machine()

    try:
        goos = _SYSTEM_TO_GOOS[system]
    except KeyError as error:
        raise RuntimeError(f"Unsupported platform system: {system}") from error

    try:
        goarch = _MACHINE_TO_GOARCH[machine]
    except KeyError as error:
        raise RuntimeError(f"Unsupported platform architecture: {machine}") from error

    return goos, goarch


def bundled_dbmate_path() -> Path:
    goos, goarch = _normalized_platform()
    binary_name = "dbmate.exe" if goos == "windows" else "dbmate"
    return (
        Path(__file__).resolve().parents[1]
        / "_vendor"
        / "dbmate"
        / f"{goos}-{goarch}"
        / binary_name
    )


def resolve_dbmate_binary(
    explicit_path: str | None = None,
    *,
    environ: Mapping[str, str] | None = None,
) -> Path:
    candidates: list[Path] = []
    if explicit_path:
        candidates.append(Path(explicit_path))

    runtime_env = load_runtime_env(environ=environ)
    env_path = runtime_env.dbmate_bin
    if env_path:
        candidates.append(Path(env_path))

    candidates.append(bundled_dbmate_path())

    for candidate in candidates:
        if candidate.exists():
            return candidate

    searched = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(
        "dbmate binary not found. Build a wheel with bundled dbmate, or set "
        f"MATEY_DBMATE_BIN. Searched: {searched}"
    )


def redact_url(url: str) -> str:
    parsed = urlsplit(url)
    if not parsed.scheme or parsed.password is None:
        return url

    netloc = ""
    if parsed.username:
        netloc = f"{parsed.username}:***@"
    if parsed.hostname:
        netloc += parsed.hostname
    if parsed.port:
        netloc += f":{parsed.port}"

    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def build_dbmate_argv(
    *,
    dbmate_binary: Path,
    url: str,
    migrations_dir: Path,
    schema_file: Path,
    verb: str,
    global_args: Sequence[str] = (),
    extra_args: Sequence[str] = (),
) -> list[str]:
    return [
        str(dbmate_binary),
        "--url",
        url,
        "--migrations-dir",
        str(migrations_dir),
        "--schema-file",
        str(schema_file),
        *global_args,
        verb,
        *extra_args,
    ]


def _to_execution_result(
    *,
    command: Sequence[str],
    display_command: Sequence[str],
    completed: subprocess.CompletedProcess[str],
    duration_seconds: float,
    verb: str,
    context: DbmateLogContext | None,
    captured: bool,
) -> DbmateExecutionResult:
    return DbmateExecutionResult(
        command=tuple(command),
        display_command=tuple(display_command),
        returncode=int(completed.returncode),
        duration_seconds=duration_seconds,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
        verb=verb,
        context=context,
        captured=captured,
    )


def run_dbmate(
    *,
    dbmate_binary: Path,
    url: str,
    migrations_dir: Path,
    schema_file: Path,
    verb: str,
    global_args: Sequence[str] = (),
    extra_args: Sequence[str] = (),
    log_context: DbmateLogContext | None = None,
    on_result: DbmateResultCallback | None = None,
) -> int:
    command = build_dbmate_argv(
        dbmate_binary=dbmate_binary,
        url=url,
        migrations_dir=migrations_dir,
        schema_file=schema_file,
        verb=verb,
        global_args=global_args,
        extra_args=extra_args,
    )
    display_command = command.copy()
    display_command[2] = redact_url(url)
    started = time.perf_counter()
    completed = subprocess.run(
        command,
        check=False,
        capture_output=True,
        text=True,
    )
    elapsed = time.perf_counter() - started
    result = _to_execution_result(
        command=command,
        display_command=display_command,
        completed=completed,
        duration_seconds=elapsed,
        verb=verb,
        context=log_context,
        captured=False,
    )
    if on_result is not None:
        on_result(result)
    return result.returncode


def run_dbmate_capture(
    *,
    dbmate_binary: Path,
    url: str,
    migrations_dir: Path,
    schema_file: Path,
    verb: str,
    global_args: Sequence[str] = (),
    extra_args: Sequence[str] = (),
    log_context: DbmateLogContext | None = None,
    on_result: DbmateResultCallback | None = None,
) -> subprocess.CompletedProcess[str]:
    command = build_dbmate_argv(
        dbmate_binary=dbmate_binary,
        url=url,
        migrations_dir=migrations_dir,
        schema_file=schema_file,
        verb=verb,
        global_args=global_args,
        extra_args=extra_args,
    )
    display_command = command.copy()
    display_command[2] = redact_url(url)
    started = time.perf_counter()
    completed = subprocess.run(command, check=False, capture_output=True, text=True)
    elapsed = time.perf_counter() - started
    result = _to_execution_result(
        command=command,
        display_command=display_command,
        completed=completed,
        duration_seconds=elapsed,
        verb=verb,
        context=log_context,
        captured=True,
    )
    if on_result is not None:
        on_result(result)
    return completed
