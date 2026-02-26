from __future__ import annotations

import secrets
import shutil
import socket
import subprocess
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

from matey.domain import ScratchProvisionError, ScratchTarget


@dataclass(frozen=True)
class ScratchPlan:
    target: ScratchTarget
    cleanup: Callable[[], None]


_BIGQUERY_MULTI_REGION_LOCATIONS = {"us", "eu"}


def detect_engine(url: str) -> str:
    scheme = urlsplit(url).scheme.lower()
    if scheme in {"postgres", "postgresql"}:
        return "postgres"
    if scheme == "mysql":
        return "mysql"
    if scheme in {"sqlite", "sqlite3"}:
        return "sqlite"
    if scheme.startswith("clickhouse"):
        return "clickhouse"
    if scheme == "bigquery":
        return "bigquery"
    if scheme in {"spanner", "spanner-postgres"}:
        raise ScratchProvisionError(
            "Spanner scratch is not supported for schema workflows: dbmate schema dump is unavailable."
        )
    raise ScratchProvisionError(f"Unsupported database URL scheme for scratch: {scheme}")


def _is_bigquery_location_token(value: str) -> bool:
    lowered = value.lower()
    return lowered in _BIGQUERY_MULTI_REGION_LOCATIONS or "-" in lowered


def _build_bigquery_scratch_url(base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    if not parsed.netloc:
        raise ScratchProvisionError(
            "BigQuery scratch base URL must include a project host "
            "(for example: bigquery://my-project/us)."
        )

    segments = [segment for segment in parsed.path.split("/") if segment]
    if len(segments) > 2:
        raise ScratchProvisionError(
            "BigQuery scratch base URL must be one of: "
            "bigquery://<project>, bigquery://<project>/<location>, "
            "bigquery://<project>/<location>/<dataset>."
        )

    if len(segments) == 0:
        scratch_segments = [scratch_name]
    elif len(segments) == 1:
        # For one path segment we accept either:
        # - location base: bigquery://project/us -> /us/<scratch>
        # - dataset placeholder: bigquery://project/my_dataset -> /<scratch>
        if _is_bigquery_location_token(segments[0]):
            scratch_segments = [segments[0], scratch_name]
        else:
            scratch_segments = [scratch_name]
    else:
        # location + dataset placeholder: keep location, replace dataset
        scratch_segments = [segments[0], scratch_name]

    return urlunsplit(
        SplitResult(
            scheme=parsed.scheme,
            netloc=parsed.netloc,
            path=f"/{'/'.join(scratch_segments)}",
            query=parsed.query,
            fragment=parsed.fragment,
        )
    )


def build_scratch_url(engine: str, base_url: str, scratch_name: str) -> str:
    parsed = urlsplit(base_url)
    if engine == "sqlite":
        # sqlite:path or sqlite:/abs/path
        raw_path = parsed.path or parsed.netloc
        if not raw_path:
            raise ScratchProvisionError("SQLite scratch base URL must include a path.")
        sqlite_path = Path(raw_path)
        parent = sqlite_path.parent if sqlite_path.suffix else sqlite_path
        scratch_file = parent / f"{scratch_name}.sqlite3"
        # dbmate currently registers sqlite3 driver names for URL parsing.
        prefix = "sqlite3:"
        return f"{prefix}{scratch_file.as_posix()}"

    if engine == "bigquery":
        return _build_bigquery_scratch_url(base_url, scratch_name)

    replaced = SplitResult(
        scheme=parsed.scheme,
        netloc=parsed.netloc,
        path=f"/{scratch_name}",
        query=parsed.query,
        fragment=parsed.fragment,
    )
    return urlunsplit(replaced)


def _find_free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        probe.listen(1)
        return int(probe.getsockname()[1])


def _run_docker(command: list[str]) -> str:
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise ScratchProvisionError(f"Docker command failed: {' '.join(command)} :: {stderr}")
    return (result.stdout or "").strip()


def _start_docker_server(engine: str) -> tuple[str, Callable[[], None]]:
    container_name = f"matey-{engine}-{secrets.token_hex(4)}"
    host_port = _find_free_tcp_port()

    if engine == "postgres":
        run_args = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "-e",
            "POSTGRES_USER=matey",
            "-e",
            "POSTGRES_PASSWORD=matey",
            "-e",
            "POSTGRES_DB=postgres",
            "-p",
            f"{host_port}:5432",
            "postgres:16-alpine",
        ]
        base_url = f"postgres://matey:matey@127.0.0.1:{host_port}/postgres?sslmode=disable"
    elif engine == "mysql":
        run_args = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "-e",
            "MYSQL_ROOT_PASSWORD=matey",
            "-p",
            f"{host_port}:3306",
            "mysql:8.4",
        ]
        base_url = f"mysql://root:matey@127.0.0.1:{host_port}/mysql"
    elif engine == "clickhouse":
        run_args = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "-e",
            "CLICKHOUSE_USER=matey",
            "-e",
            "CLICKHOUSE_PASSWORD=matey",
            "-p",
            f"{host_port}:9000",
            "clickhouse/clickhouse-server:24.8",
        ]
        base_url = f"clickhouse://matey:matey@127.0.0.1:{host_port}/default"
    else:
        raise ScratchProvisionError(f"Docker scratch not supported for engine: {engine}")

    _run_docker(run_args)

    def _cleanup() -> None:
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            check=False,
            capture_output=True,
            text=True,
        )

    return base_url, _cleanup


def plan_scratch_target(
    *,
    engine: str,
    scratch_name: str,
    test_url: str | None,
) -> ScratchPlan:
    if test_url:
        scratch_url = build_scratch_url(engine, test_url, scratch_name)
        target = ScratchTarget(
            engine=engine,
            scratch_name=scratch_name,
            scratch_url=scratch_url,
            cleanup_required=engine != "sqlite",
            auto_provisioned=False,
        )
        return ScratchPlan(target=target, cleanup=lambda: None)

    if engine == "sqlite":
        sqlite_dir = Path(tempfile.mkdtemp(prefix="matey-sqlite-"))
        base_url = f"sqlite3:{(sqlite_dir / 'base.sqlite3').as_posix()}"
        scratch_url = build_scratch_url("sqlite", base_url, scratch_name)

        def _cleanup_sqlite() -> None:
            shutil.rmtree(sqlite_dir, ignore_errors=True)

        target = ScratchTarget(
            engine="sqlite",
            scratch_name=scratch_name,
            scratch_url=scratch_url,
            cleanup_required=False,
            auto_provisioned=True,
        )
        return ScratchPlan(target=target, cleanup=_cleanup_sqlite)

    if engine == "bigquery":
        raise ScratchProvisionError(
            "BigQuery scratch requires --test-url (or configured test_url_env); "
            "there is no Docker fallback."
        )

    base_url, cleanup = _start_docker_server(engine)
    scratch_url = build_scratch_url(engine, base_url, scratch_name)
    target = ScratchTarget(
        engine=engine,
        scratch_name=scratch_name,
        scratch_url=scratch_url,
        cleanup_required=True,
        auto_provisioned=True,
    )
    return ScratchPlan(target=target, cleanup=cleanup)
