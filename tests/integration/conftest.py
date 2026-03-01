from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import SplitResult, urlsplit, urlunsplit

import pytest

from matey.artifacts import SqliteArtifactStore
from matey.config import ConfigDefaults, TargetRuntime, build_target_runtime
from matey.dbmate import DbmateGateway
from matey.engine import EnginePolicyRegistry, ScratchManager
from matey.models import ResolvedTargetConfig, WorktreeChange
from matey.platform import (
    LocalFileSystem,
    ReentrantLockManager,
    SubprocessRunner,
    TypedSettingsEnvProvider,
)
from matey.runtime import AppContext, CommandScope
from matey.sql import SqlPipeline


class _GitStub:
    def __init__(self, repo_root: Path) -> None:
        self._repo_root = repo_root.resolve()

    def repo_root(self) -> Path:
        return self._repo_root

    def head_commit(self) -> str:
        return "head"

    def resolve_ref(self, ref: str) -> str:
        return ref

    def merge_base(self, left_ref: str, right_ref: str) -> str:
        del left_ref, right_ref
        return "base"

    def read_blob_bytes(self, commit: str, rel_path: Path) -> bytes | None:
        del commit, rel_path
        return None

    def list_tree_paths(self, commit: str, rel_dir: Path) -> tuple[Path, ...]:
        del commit, rel_dir
        return ()

    def has_local_changes(self, *, rel_paths: tuple[Path, ...]) -> bool:
        del rel_paths
        return False

    def list_local_changes(self, *, rel_paths: tuple[Path, ...]) -> tuple[WorktreeChange, ...]:
        del rel_paths
        return ()


@dataclass(frozen=True)
class LiveUrl:
    engine: str
    url: str
    test_url: str | None


@pytest.fixture(scope="session")
def defaults() -> ConfigDefaults:
    return ConfigDefaults()


@pytest.fixture
def integration_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    return repo


@pytest.fixture
def runtime(integration_repo: Path) -> TargetRuntime:
    db_dir = integration_repo / "db" / "core"
    db_dir.mkdir(parents=True, exist_ok=True)
    (db_dir / "migrations").mkdir(parents=True, exist_ok=True)
    (db_dir / "checkpoints").mkdir(parents=True, exist_ok=True)
    return build_target_runtime(
        resolved=ResolvedTargetConfig(
            name="core",
            db_dir=db_dir,
            url_env="MATEY_URL",
            test_url_env="MATEY_TEST_URL",
        )
    )


@pytest.fixture
def app_context(integration_repo: Path) -> AppContext:
    env = TypedSettingsEnvProvider()
    git = _GitStub(integration_repo)
    dbmate = DbmateGateway(runner=SubprocessRunner(), env=env)
    artifact_store = SqliteArtifactStore(repo_root=integration_repo)
    lock_manager = ReentrantLockManager(lock_root=integration_repo / ".matey" / "locks")
    scope = CommandScope(
        repo_root=integration_repo,
        lock_manager=lock_manager,
        artifact_store=artifact_store,
    )
    policies = EnginePolicyRegistry()
    return AppContext(
        fs=LocalFileSystem(),
        proc=SubprocessRunner(),
        env=env,
        git=git,
        dbmate=dbmate,
        sql_pipeline=SqlPipeline(),
        engine_policies=policies,
        scratch=ScratchManager(engine_policies=policies),
        artifact_store=artifact_store,
        scope=scope,
    )


def write_migration(
    *,
    runtime: TargetRuntime,
    version: str,
    name: str,
    up_sql: str,
    down_sql: str | None = None,
) -> Path:
    filename = f"{version}_{name}.sql"
    path = runtime.paths.migrations_dir / filename
    lines = ["-- migrate:up", up_sql]
    if down_sql is not None:
        lines.extend(["-- migrate:down", down_sql])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def docker_available() -> bool:
    try:
        completed = subprocess.run(
            ("docker", "info"),
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except FileNotFoundError:
        return False
    return completed.returncode == 0


@pytest.fixture(scope="session")
def has_docker() -> bool:
    return docker_available()


@contextmanager
def live_container(engine: str, db_name: str) -> Iterator[LiveUrl]:
    if engine == "postgres":
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer(
            image="postgres:16-alpine",
            username="postgres",
            password="postgres",
            dbname=db_name,
        )
        container.start()
        try:
            host = container.get_container_host_ip()
            if host in {"localhost", "0.0.0.0"}:
                host = "127.0.0.1"
            port = container.get_exposed_port(5432)
            url = f"postgres://postgres:postgres@{host}:{port}/{db_name}?sslmode=disable"
            yield LiveUrl(engine=engine, url=url, test_url=None)
        finally:
            container.stop()
        return

    if engine == "mysql":
        from testcontainers.mysql import MySqlContainer

        container = MySqlContainer(
            image="mysql:8.4",
            username="root",
            password="root",
            dbname=db_name,
        )
        container.start()
        try:
            host = container.get_container_host_ip()
            if host in {"localhost", "0.0.0.0"}:
                host = "127.0.0.1"
            port = container.get_exposed_port(3306)
            url = f"mysql://root:root@{host}:{port}/{db_name}"
            yield LiveUrl(engine=engine, url=url, test_url=None)
        finally:
            container.stop()
        return

    if engine == "clickhouse":
        from testcontainers.clickhouse import ClickHouseContainer

        container = ClickHouseContainer(image="clickhouse/clickhouse-server:24.8")
        container.start()
        try:
            base = str(container.get_connection_url())
            parsed = urlsplit(base)
            host = parsed.hostname or container.get_container_host_ip()
            if host in {"localhost", "0.0.0.0"}:
                host = "127.0.0.1"
            port = parsed.port or int(container.get_exposed_port(9000))
            user = parsed.username or ""
            password = parsed.password or ""
            auth = user
            if password or user:
                auth = f"{user}:{password}"
            netloc = f"{auth}@{host}:{port}" if auth else f"{host}:{port}"
            rebuilt = SplitResult(
                scheme=parsed.scheme,
                netloc=netloc,
                path=f"/{db_name}",
                query=parsed.query,
                fragment=parsed.fragment,
            )
            url = urlunsplit(rebuilt)
            yield LiveUrl(engine=engine, url=url, test_url=None)
        finally:
            container.stop()
        return

    raise ValueError(f"Unsupported engine for live container: {engine}")


@pytest.fixture
def bigquery_urls() -> LiveUrl:
    live = os.environ.get("MATEY_BIGQUERY_URL", "").strip()
    test = os.environ.get("MATEY_BIGQUERY_TEST_URL", "").strip()
    if not live or not test:
        pytest.skip("BigQuery integration requires MATEY_BIGQUERY_URL and MATEY_BIGQUERY_TEST_URL")
    return LiveUrl(engine="bigquery", url=live, test_url=test)
