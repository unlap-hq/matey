from __future__ import annotations

import sys
from pathlib import Path
from types import ModuleType

import pytest

from matey.scratch import Engine, Scratch, ScratchConfigError


class _FakeContainer:
    def __init__(self, url: str) -> None:
        self._url = url
        self.started = False
        self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def get_connection_url(self) -> str:
        return self._url


def _install_fake_container_module(
    monkeypatch: pytest.MonkeyPatch,
    *,
    module_name: str,
    class_name: str,
    url: str,
) -> _FakeContainer:
    module = ModuleType(module_name)
    container = _FakeContainer(url=url)

    def factory(*args: object, **kwargs: object) -> _FakeContainer:
        del args, kwargs
        return container

    setattr(module, class_name, factory)
    monkeypatch.setitem(sys.modules, module_name, module)
    return container


def test_sqlite_auto_provision_creates_and_cleans_file(tmp_path: Path) -> None:
    scratch = Scratch(temp_root=tmp_path)
    with scratch.lease(engine=Engine.SQLITE, scratch_name="scratch1", test_base_url=None) as lease:
        assert lease.auto_provisioned is True
        assert lease.url.startswith("sqlite3:")
        sqlite_path = Path(lease.url.removeprefix("sqlite3:"))
        assert sqlite_path.exists()

    assert sqlite_path.exists() is False


def test_sqlite_keep_preserves_auto_provisioned_file(tmp_path: Path) -> None:
    scratch = Scratch(temp_root=tmp_path)
    with scratch.lease(
        engine=Engine.SQLITE,
        scratch_name="scratch_keep",
        test_base_url=None,
        keep=True,
    ) as lease:
        sqlite_path = Path(lease.url.removeprefix("sqlite3:"))
        assert sqlite_path.exists()

    assert sqlite_path.exists()


def test_postgres_base_url_rewrite_preserves_query_and_fragment() -> None:
    scratch = Scratch()
    base = "postgres://u:p@db.internal:5432/app?sslmode=disable#frag"
    with scratch.lease(
        engine=Engine.POSTGRES,
        scratch_name="scratch_db",
        test_base_url=base,
    ) as lease:
        assert lease.auto_provisioned is False
        assert lease.url == "postgres://u:p@db.internal:5432/scratch_db?sslmode=disable#frag"


@pytest.mark.parametrize(
    ("base_url", "expected"),
    [
        ("bigquery://project", "bigquery://project/scratch_ds"),
        ("bigquery://project/dataset", "bigquery://project/scratch_ds"),
        ("bigquery://project/us/dataset", "bigquery://project/us/scratch_ds"),
        ("bigquery://project/us-central1/dataset", "bigquery://project/us-central1/scratch_ds"),
    ],
)
def test_bigquery_base_url_rewrite(base_url: str, expected: str) -> None:
    scratch = Scratch()
    with scratch.lease(
        engine=Engine.BIGQUERY,
        scratch_name="scratch_ds",
        test_base_url=base_url,
    ) as lease:
        assert lease.auto_provisioned is False
        assert lease.url == expected


def test_bigquery_without_base_url_fails() -> None:
    scratch = Scratch()
    with (
        pytest.raises(ScratchConfigError),
        scratch.lease(
            engine=Engine.BIGQUERY,
            scratch_name="scratch_ds",
            test_base_url=None,
        ),
    ):
        pass


def test_bigquery_location_like_single_segment_is_ambiguous() -> None:
    scratch = Scratch()
    with pytest.raises(
        ScratchConfigError, match="Ambiguous BigQuery scratch base URL"
    ), scratch.lease(
        engine=Engine.BIGQUERY,
        scratch_name="scratch_ds",
        test_base_url="bigquery://project/us",
    ):
        pass


def test_postgres_auto_provision_starts_and_stops_container(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = _install_fake_container_module(
        monkeypatch,
        module_name="testcontainers.postgres",
        class_name="PostgresContainer",
        url="postgres://u:p@127.0.0.1:5432/testdb",
    )
    scratch = Scratch()

    with scratch.lease(
        engine=Engine.POSTGRES, scratch_name="scratch_pg", test_base_url=None
    ) as lease:
        assert lease.auto_provisioned is True
        assert lease.url == "postgres://u:p@127.0.0.1:5432/testdb"
        assert container.started is True
        assert container.stopped is False

    assert container.stopped is True


def test_postgres_keep_skips_container_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    container = _install_fake_container_module(
        monkeypatch,
        module_name="testcontainers.postgres",
        class_name="PostgresContainer",
        url="postgres://u:p@127.0.0.1:5432/testdb",
    )
    scratch = Scratch()

    with scratch.lease(
        engine=Engine.POSTGRES,
        scratch_name="scratch_pg",
        test_base_url=None,
        keep=True,
    ) as lease:
        assert lease.auto_provisioned is True
        assert container.started is True

    assert container.stopped is False


def test_clickhouse_auto_provision_rewrites_url_structurally(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    container = _install_fake_container_module(
        monkeypatch,
        module_name="testcontainers.clickhouse",
        class_name="ClickHouseContainer",
        url="clickhouse://default:@127.0.0.1:8123/default?secure=1#frag",
    )
    scratch = Scratch()

    with scratch.lease(
        engine=Engine.CLICKHOUSE,
        scratch_name="scratch_clickhouse",
        test_base_url=None,
    ) as lease:
        assert lease.url == "clickhouse://default:@127.0.0.1:8123/scratch_clickhouse?secure=1#frag"
        assert container.started is True

    assert container.stopped is True


def test_sqlite_base_url_rewrite_preserves_query_and_fragment() -> None:
    scratch = Scratch()
    with scratch.lease(
        engine=Engine.SQLITE,
        scratch_name="scratch_sqlite",
        test_base_url="sqlite3:/tmp/base.sqlite3?mode=rw#shared",
    ) as lease:
        assert lease.url == "sqlite3:/tmp/scratch_sqlite.sqlite3?mode=rw#shared"


def test_mysql_auto_provision_uses_detected_image(monkeypatch: pytest.MonkeyPatch) -> None:
    module = ModuleType("testcontainers.mysql")
    container = _FakeContainer(url="mysql://root:root@127.0.0.1:3306/testdb")
    captured: dict[str, object] = {}

    def factory(*args: object, **kwargs: object) -> _FakeContainer:
        del args
        captured.update(kwargs)
        return container

    module.MySqlContainer = factory
    monkeypatch.setitem(sys.modules, "testcontainers.mysql", module)
    monkeypatch.setattr("matey.scratch._mysql_image_for_local_dump_client", lambda: "mysql:9")

    scratch = Scratch()
    with scratch.lease(engine=Engine.MYSQL, scratch_name="scratch_mysql", test_base_url=None) as lease:
        assert lease.auto_provisioned is True
        assert container.started is True
        assert lease.url == "mysql://root:root@127.0.0.1:3306/testdb"

    assert captured["image"] == "mysql:9"


def test_detect_local_mysqldump_major_parses_mysql_client(monkeypatch: pytest.MonkeyPatch) -> None:
    class _Completed:
        returncode = 0
        stdout = "mysqldump  Ver 8.0.43 for Linux on x86_64 (MySQL Community Server - GPL)"
        stderr = ""

    monkeypatch.setattr("matey.scratch.shutil.which", lambda binary: "/usr/bin/mysqldump")
    monkeypatch.setattr("matey.scratch.subprocess.run", lambda *args, **kwargs: _Completed())

    from matey import scratch as scratch_mod

    assert (
        scratch_mod._detect_client_major(
            binary_name="mysqldump",
            version_flag="--version",
            pattern=r"(?:Ver|Distrib)\s+(\d+)(?:\.\d+)?",
            reject_substring="mariadb",
            valid_range=range(5, 11),
        )
        == 8
    )


def test_detect_local_mysqldump_major_ignores_mariadb_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Completed:
        returncode = 0
        stdout = "mysqldump  Ver 10.11.7-MariaDB for Linux on x86_64 (MariaDB Server)"
        stderr = ""

    monkeypatch.setattr("matey.scratch.shutil.which", lambda binary: "/usr/bin/mysqldump")
    monkeypatch.setattr("matey.scratch.subprocess.run", lambda *args, **kwargs: _Completed())

    from matey import scratch as scratch_mod

    assert (
        scratch_mod._detect_client_major(
            binary_name="mysqldump",
            version_flag="--version",
            pattern=r"(?:Ver|Distrib)\s+(\d+)(?:\.\d+)?",
            reject_substring="mariadb",
            valid_range=range(5, 11),
        )
        is None
    )
