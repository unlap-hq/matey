from __future__ import annotations

import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

import pytest

from matey.dbmate import Dbmate, default_dbmate_binary
from matey.project import TargetConfig
from matey.scratch import Engine, Scratch, ScratchError


def _integration_engines() -> list[Engine]:
    engines = [Engine.SQLITE, Engine.POSTGRES, Engine.MYSQL, Engine.CLICKHOUSE, Engine.BIGQUERY_EMULATOR]
    if os.getenv("MATEY_BIGQUERY_TEST_URL", "").strip():
        engines.append(Engine.BIGQUERY)
    return engines


def _normalize_runtime_base_url(*, engine: Engine, url: str) -> str:
    parsed = urlsplit(url)

    if engine is Engine.POSTGRES and "+" in parsed.scheme:
        base_scheme = parsed.scheme.split("+", 1)[0]
        query = parsed.query
        if "sslmode=" not in query:
            query = f"{query}&sslmode=disable" if query else "sslmode=disable"
        return urlunsplit(
            SplitResult(
                scheme=base_scheme,
                netloc=parsed.netloc,
                path=parsed.path,
                query=query,
                fragment=parsed.fragment,
            )
        )

    if engine is Engine.POSTGRES and "sslmode=" not in parsed.query:
        query = f"{parsed.query}&sslmode=disable" if parsed.query else "sslmode=disable"
        return urlunsplit(
            SplitResult(
                scheme=parsed.scheme,
                netloc=parsed.netloc,
                path=parsed.path,
                query=query,
                fragment=parsed.fragment,
            )
        )

    if engine is Engine.MYSQL and parsed.hostname == "localhost":
        host = "127.0.0.1"
        netloc = parsed.netloc.replace("localhost", host, 1)
        return urlunsplit(
            SplitResult(
                scheme=parsed.scheme,
                netloc=netloc,
                path=parsed.path,
                query=parsed.query,
                fragment=parsed.fragment,
            )
        )

    return url


@dataclass(frozen=True, slots=True)
class IntegrationRuntime:
    engine: Engine
    test_base_url: str
    dbmate_bin: Path


@pytest.fixture(scope="session")
def dbmate_bin() -> Path:
    path = default_dbmate_binary()
    if not path.exists() or not path.is_file():
        pytest.skip(f"Bundled dbmate binary not available: {path}")
    if not os.access(path, os.X_OK):
        pytest.skip(f"Bundled dbmate binary is not executable: {path}")
    return path


@pytest.fixture(scope="session", params=_integration_engines(), ids=lambda engine: engine.value)
def runtime(request: pytest.FixtureRequest, dbmate_bin: Path, tmp_path_factory: pytest.TempPathFactory):
    engine: Engine = request.param
    if engine is Engine.BIGQUERY:
        base = os.getenv("MATEY_BIGQUERY_TEST_URL", "").strip()
        if not base:
            pytest.skip("Set MATEY_BIGQUERY_TEST_URL to run BigQuery integration tests.")
        yield IntegrationRuntime(engine=engine, test_base_url=base, dbmate_bin=dbmate_bin)
        return

    scratch = Scratch(temp_root=tmp_path_factory.mktemp(f"scratch-{engine.value}"))
    base_name = f"matey_it_base_{engine.value}_{uuid.uuid4().hex[:8]}"
    try:
        with scratch.lease(
            engine=engine,
            scratch_name=base_name,
            test_base_url=None,
            keep=False,
        ) as lease:
            yield IntegrationRuntime(
                engine=engine,
                test_base_url=_normalize_runtime_base_url(
                    engine=engine,
                    url=lease.url,
                ),
                dbmate_bin=dbmate_bin,
            )
    except ScratchError as error:
        pytest.skip(f"Scratch runtime unavailable for {engine.value}: {error}")


@pytest.fixture
def target(tmp_path: Path) -> TargetConfig:
    return TargetConfig(
        name="core",
        root=(tmp_path / "db" / "core").resolve(),
        url_env="MATEY_INTEGRATION_DATABASE_URL",
        test_url_env="MATEY_INTEGRATION_TEST_DATABASE_URL",
    )


@pytest.fixture
def live_url(runtime: IntegrationRuntime, tmp_path: Path) -> str:
    scratch = Scratch(temp_root=tmp_path)
    scratch_name = f"matey_it_live_{runtime.engine.value}_{uuid.uuid4().hex[:10]}"
    with scratch.lease(
        engine=runtime.engine,
        scratch_name=scratch_name,
        test_base_url=runtime.test_base_url,
        keep=False,
    ) as lease:
        url = lease.url
    yield url

    cleanup_migrations = tmp_path / "_cleanup" / "migrations"
    cleanup_migrations.mkdir(parents=True, exist_ok=True)
    try:
        dbmate = Dbmate(migrations_dir=cleanup_migrations, dbmate_bin=runtime.dbmate_bin)
        _ = dbmate.database(url).drop()
    except Exception:
        # Best-effort cleanup for derived scratch URLs. Runtime teardown still
        # cleans auto-provisioned resources.
        pass
