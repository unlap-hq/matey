from __future__ import annotations

import os
import subprocess
import tempfile
from pathlib import Path

import pytest

from matey.drivers.dbmate import bundled_dbmate_path


def _resolve_dbmate_binary() -> Path | None:
    env_path = os.getenv("MATEY_DBMATE_BIN")
    if env_path:
        candidate = Path(env_path)
        if candidate.exists():
            return candidate
        return None

    bundled = bundled_dbmate_path()
    if bundled.exists():
        return bundled
    return None


@pytest.fixture(scope="session")
def dbmate_binary() -> Path:
    resolved = _resolve_dbmate_binary()
    if resolved is None:
        pytest.skip(
            "No dbmate binary found for integration tests. "
            "Set MATEY_DBMATE_BIN or build bundled dbmate first."
        )
    return resolved


@pytest.fixture(scope="session")
def docker_available() -> bool:
    result = subprocess.run(
        ["docker", "info"],
        check=False,
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


@pytest.fixture(scope="session")
def engine_supported(dbmate_binary: Path) -> callable:
    cache: dict[str, bool] = {}

    def _check(engine: str) -> bool:
        cached = cache.get(engine)
        if cached is not None:
            return cached

        with tempfile.TemporaryDirectory(prefix="matey-engine-probe-") as probe_dir:
            root = Path(probe_dir)
            migrations = root / "migrations"
            migrations.mkdir(parents=True, exist_ok=True)
            schema = root / "schema.sql"
            schema.write_text("-- probe\n", encoding="utf-8")

            url_by_engine = {
                "sqlite": f"sqlite3:{(root / 'probe.sqlite3').as_posix()}",
                "postgres": "postgres://127.0.0.1:1/postgres?sslmode=disable",
                "mysql": "mysql://127.0.0.1:1/mysql",
                "clickhouse": "clickhouse://127.0.0.1:1/default",
            }
            probe_url = url_by_engine.get(engine)
            if probe_url is None:
                cache[engine] = False
                return False

            result = subprocess.run(
                [
                    str(dbmate_binary),
                    "--url",
                    probe_url,
                    "--migrations-dir",
                    str(migrations),
                    "--schema-file",
                    str(schema),
                    "status",
                ],
                check=False,
                capture_output=True,
                text=True,
            )
            stderr_text = (result.stderr or "").lower()
            supported = "unsupported driver" not in stderr_text
            cache[engine] = supported
            return supported

    return _check
