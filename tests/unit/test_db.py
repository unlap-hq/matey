from __future__ import annotations

import importlib
from contextlib import contextmanager
from pathlib import Path

import pytest

import matey.db as db_mod
from matey.config import TargetConfig
from matey.dbmate import CmdResult, DbmateError
from matey.lockfile import LockState, WorktreeStep
from matey.repo import Snapshot

db_runtime_mod = importlib.import_module("matey.db.runtime")


def _target(tmp_path: Path, name: str = "core") -> TargetConfig:
    return TargetConfig(
        name=name,
        dir=(tmp_path / "db" / name).resolve(),
        url_env=f"{name.upper()}_DATABASE_URL",
        test_url_env=f"{name.upper()}_TEST_DATABASE_URL",
    )


def _step(index: int, filename: str) -> WorktreeStep:
    return WorktreeStep(
        index=index,
        version=f"{index:03d}",
        migration_file=f"migrations/{filename}",
        migration_digest=f"m{index}",
        checkpoint_file=f"checkpoints/{filename}",
        checkpoint_digest=f"c{index}",
        chain_hash=f"h{index}",
    )


def _cmd(*argv: str, exit_code: int = 0, stdout: str = "", stderr: str = "") -> CmdResult:
    return CmdResult(argv=tuple(argv), exit_code=exit_code, stdout=stdout, stderr=stderr)


class _FakeConn:
    def __init__(self, *, url: str = "sqlite3:/tmp/test.sqlite3") -> None:
        self.url = url
        self.create_calls = 0
        self.up_calls = 0
        self.migrate_calls = 0
        self.rollback_calls: list[int] = []
        self.loaded_sql: list[str] = []

    def create(self) -> CmdResult:
        self.create_calls += 1
        return _cmd("dbmate", "create")

    def up(self) -> CmdResult:
        self.up_calls += 1
        return _cmd("dbmate", "up")

    def migrate(self) -> CmdResult:
        self.migrate_calls += 1
        return _cmd("dbmate", "migrate")

    def rollback(self, steps: int) -> CmdResult:
        self.rollback_calls.append(steps)
        return _cmd("dbmate", "rollback", str(steps))

    def load(self, schema_sql: str) -> CmdResult:
        self.loaded_sql.append(schema_sql)
        return _cmd("dbmate", "load")

    def status(self) -> CmdResult:  # pragma: no cover - patched in tests
        return _cmd("dbmate", "status")

    def dump(self) -> CmdResult:  # pragma: no cover - patched in tests
        return _cmd("dbmate", "dump")


def _ctx(
    tmp_path: Path, *, conn: _FakeConn, steps: tuple[WorktreeStep, ...]
) -> db_runtime_mod.RuntimeContext:
    target = _target(tmp_path)
    snapshot = Snapshot(
        target_name=target.name,
        schema_sql=b"CREATE TABLE head(id INTEGER);\n",
        lock_toml=None,
        migrations={step.migration_file: b"-- migrate:up\nSELECT 1;\n" for step in steps},
        checkpoints={
            step.checkpoint_file: f"CREATE TABLE c{step.index}(id INTEGER);\n".encode()
            for step in steps
        },
    )
    state = LockState(
        target_name=target.name,
        lock=None,
        worktree_steps=steps,
        schema_digest=None,
        orphan_checkpoints=(),
        diagnostics=(),
    )
    return db_runtime_mod.RuntimeContext(target=target, snapshot=snapshot, state=state, conn=conn)


def _qualified_write(engine: str) -> str:
    match engine:
        case "bigquery":
            return "CREATE TABLE analytics.events (id INT64);"
        case "mysql":
            return "CREATE TABLE other_db.events (id BIGINT);"
        case "clickhouse":
            return "CREATE TABLE other_db.events (id Int64) ENGINE = MergeTree ORDER BY tuple();"
        case _:
            return "CREATE TABLE events (id BIGINT);"


def _qualified_down(engine: str) -> str:
    match engine:
        case "bigquery":
            return "DROP TABLE analytics.events;"
        case "mysql":
            return "DROP TABLE other_db.events;"
        case "clickhouse":
            return "DROP TABLE other_db.events;"
        case _:
            return "DROP TABLE events;"


def test_up_uses_create_if_pre_status_reports_missing_db(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    ctx = _ctx(tmp_path, conn=conn, steps=(_step(1, "001_init.sql"),))

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    missing = _cmd("dbmate", "status", exit_code=1, stderr="database does not exist")
    statuses = iter(
        [
            db_runtime_mod.StatusError(result=missing, missing_db=True),
            (
                _cmd("dbmate", "status", stdout="[ ] 001_init.sql\nApplied: 0\n"),
                db_runtime_mod.LiveStatus(applied_files=(), applied_count=0),
            ),
            (
                _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
                db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
            ),
        ]
    )

    def _fake_status(_conn: _FakeConn):
        value = next(statuses)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(db_runtime_mod, "read_status", _fake_status)
    monkeypatch.setattr(
        db_runtime_mod,
        "verify_expected_schema",
        lambda **kwargs: True,
    )

    result = db_mod.up(ctx.target)

    assert result.before_index == 0
    assert result.after_index == 1
    assert conn.create_calls == 1
    assert conn.up_calls == 1


def test_up_rejects_zero_migration_baseline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    ctx = _ctx(tmp_path, conn=conn, steps=())

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)

    with pytest.raises(
        db_mod.DbError,
        match="db up is unavailable before the first worktree migration checkpoint",
    ):
        db_mod.up(ctx.target)
    assert conn.create_calls == 0
    assert conn.up_calls == 0


def test_migrate_does_not_fallback_to_create_on_missing_db(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    ctx = _ctx(tmp_path, conn=conn, steps=(_step(1, "001_init.sql"),))

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    missing = _cmd("dbmate", "status", exit_code=1, stderr="database does not exist")

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (_ for _ in ()).throw(
            db_runtime_mod.StatusError(result=missing, missing_db=True)
        ),
    )

    with pytest.raises(db_mod.DbError, match="db migrate pre-status failed"):
        db_mod.migrate(ctx.target)
    assert conn.create_calls == 0


def test_migrate_rejects_zero_migration_baseline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    ctx = _ctx(tmp_path, conn=conn, steps=())

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)

    with pytest.raises(
        db_mod.DbError,
        match="db migrate is unavailable before the first worktree migration checkpoint",
    ):
        db_mod.migrate(ctx.target)
    assert conn.migrate_calls == 0


def test_bootstrap_loads_schema_and_verifies_head(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"), _step(2, "002_next.sql"))
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    statuses = iter(
        [
            (
                _cmd("dbmate", "status", stdout="[ ] 001_init.sql\n[ ] 002_next.sql\nApplied: 0\n"),
                db_runtime_mod.LiveStatus(applied_files=(), applied_count=0),
            ),
            (
                _cmd(
                    "dbmate",
                    "status",
                    stdout="[X] 001_init.sql\n[X] 002_next.sql\nApplied: 2\n",
                ),
                db_runtime_mod.LiveStatus(
                    applied_files=("001_init.sql", "002_next.sql"),
                    applied_count=2,
                ),
            ),
        ]
    )

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(db_runtime_mod, "read_status", lambda _conn: next(statuses))
    monkeypatch.setattr(
        db_runtime_mod,
        "verify_expected_schema",
        lambda **kwargs: True,
    )

    result = db_mod.bootstrap(ctx.target)

    assert result.before_index == 0
    assert result.after_index == 2
    assert conn.loaded_sql == ["CREATE TABLE head(id INTEGER);\n"]


def test_bootstrap_creates_missing_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn(url="postgres://u:p@host:5432/app")
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    missing = _cmd("dbmate", "status", exit_code=1, stderr='database "app" does not exist')
    statuses = iter(
        [
            db_runtime_mod.StatusError(result=missing, missing_db=True),
            (
                _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
                db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
            ),
        ]
    )

    def _fake_status(_conn):
        value = next(statuses)
        if isinstance(value, Exception):
            raise value
        return value

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(db_runtime_mod, "read_status", _fake_status)
    monkeypatch.setattr(db_runtime_mod, "verify_expected_schema", lambda **kwargs: True)

    result = db_mod.bootstrap(ctx.target)

    assert result.before_index == 0
    assert result.after_index == 1
    assert conn.create_calls == 1


def test_bootstrap_rejects_nonempty_database(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
            db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
        ),
    )

    with pytest.raises(db_mod.DbError, match="db bootstrap requires an empty or unapplied database"):
        db_mod.bootstrap(ctx.target)


def test_missing_db_status_classifier_is_engine_specific() -> None:
    assert (
        db_runtime_mod.is_missing_db_status_error(
            "postgresql://u:p@host:5432/app_db",
            'database "app_db" does not exist',
        )
        is True
    )
    assert (
        db_runtime_mod.is_missing_db_status_error(
            "mysql://u:p@host:3306/app_db",
            "Unknown database 'app_db'",
        )
        is True
    )
    assert (
        db_runtime_mod.is_missing_db_status_error(
            "sqlite3:/tmp/app.sqlite3",
            "unable to open database file",
        )
        is True
    )
    assert (
        db_runtime_mod.is_missing_db_status_error(
            "bigquery://example-project/us/app_ds",
            "Not found: Dataset example-project:app_ds was not found in location US",
        )
        is True
    )
    assert (
        db_runtime_mod.is_missing_db_status_error(
            "mysql://u:p@host:3306/app_db",
            "table app_db.events does not exist",
        )
        is False
    )
    assert (
        db_runtime_mod.is_missing_db_status_error(
            "postgresql://u:p@host:5432/app_db",
            "connection refused",
        )
        is False
    )


def test_ensure_prefix_allows_duplicate_basenames_only_in_unapplied_tail(tmp_path: Path) -> None:
    state = LockState(
        target_name="core",
        lock=None,
        worktree_steps=(
            _step(1, "001_init.sql"),
            WorktreeStep(
                index=2,
                version="002",
                migration_file="migrations/a/002_duplicate.sql",
                migration_digest="m2",
                checkpoint_file="checkpoints/a/002_duplicate.sql",
                checkpoint_digest="c2",
                chain_hash="h2",
            ),
            WorktreeStep(
                index=3,
                version="003",
                migration_file="migrations/b/002_duplicate.sql",
                migration_digest="m3",
                checkpoint_file="checkpoints/b/002_duplicate.sql",
                checkpoint_digest="c3",
                chain_hash="h3",
            ),
        ),
        schema_digest=None,
        orphan_checkpoints=(),
        diagnostics=(),
    )
    live = db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1)

    db_runtime_mod.ensure_prefix(state=state, live=live)


def test_ensure_prefix_rejects_duplicate_basenames_in_applied_prefix(tmp_path: Path) -> None:
    del tmp_path
    state = LockState(
        target_name="core",
        lock=None,
        worktree_steps=(
            WorktreeStep(
                index=1,
                version="001",
                migration_file="migrations/a/001_duplicate.sql",
                migration_digest="m1",
                checkpoint_file="checkpoints/a/001_duplicate.sql",
                checkpoint_digest="c1",
                chain_hash="h1",
            ),
            WorktreeStep(
                index=2,
                version="002",
                migration_file="migrations/b/001_duplicate.sql",
                migration_digest="m2",
                checkpoint_file="checkpoints/b/001_duplicate.sql",
                checkpoint_digest="c2",
                chain_hash="h2",
            ),
        ),
        schema_digest=None,
        orphan_checkpoints=(),
        diagnostics=(),
    )
    live = db_runtime_mod.LiveStatus(
        applied_files=("001_duplicate.sql", "001_duplicate.sql"),
        applied_count=2,
    )

    with pytest.raises(db_mod.DbError, match="applied worktree prefix has duplicate migration basenames"):
        db_runtime_mod.ensure_prefix(state=state, live=live)


def test_pending_up_allowed_wraps_invalid_utf8_as_db_error(tmp_path: Path) -> None:
    conn = _FakeConn(url="mysql://root:root@127.0.0.1:3306/testdb")
    step = _step(1, "001_init.sql")
    ctx = _ctx(tmp_path, conn=conn, steps=(step,))
    ctx.snapshot.migrations[step.migration_file] = b"\xff\xfe\x00"

    with pytest.raises(
        db_mod.DbError,
        match=r"Unable to decode migration migrations/001_init\.sql as UTF-8",
    ):
        db_runtime_mod.ensure_pending_up_allowed(
            runtime=ctx,
            applied_count=0,
            context="db up precheck",
        )


def test_expected_sql_for_index_wraps_invalid_utf8_schema_sql(tmp_path: Path) -> None:
    conn = _FakeConn()
    step = _step(1, "001_init.sql")
    target = _target(tmp_path)
    ctx = db_runtime_mod.RuntimeContext(
        target=target,
        snapshot=Snapshot(
            target_name=target.name,
            schema_sql=b"\xff\xfe\x00",
            lock_toml=None,
            migrations={step.migration_file: b"-- migrate:up\nSELECT 1;\n"},
            checkpoints={step.checkpoint_file: b"CREATE TABLE c1(id INTEGER);\n"},
        ),
        state=LockState(
            target_name=target.name,
            lock=None,
            worktree_steps=(step,),
            schema_digest=None,
            orphan_checkpoints=(),
            diagnostics=(),
        ),
        conn=conn,
    )

    with pytest.raises(db_mod.DbError, match=r"Unable to decode worktree schema\.sql as UTF-8"):
        db_runtime_mod.expected_sql_for_index(runtime=ctx, index=1)


def test_ensure_prefix_accepts_backslash_path_status_entries(tmp_path: Path) -> None:
    steps = (_step(1, "001_init.sql"),)
    state = LockState(
        target_name="core",
        lock=None,
        worktree_steps=steps,
        schema_digest=None,
        orphan_checkpoints=(),
        diagnostics=(),
    )
    live = db_runtime_mod.LiveStatus(
        applied_files=("migrations\\001_init.sql",),
        applied_count=1,
    )

    db_runtime_mod.ensure_prefix(state=state, live=live)


def test_compare_expected_schema_wraps_dump_dbmate_error(tmp_path: Path) -> None:
    conn = _FakeConn()
    step = _step(1, "001_init.sql")
    ctx = _ctx(tmp_path, conn=conn, steps=(step,))

    def _boom() -> CmdResult:
        raise DbmateError("dbmate dump completed without producing a schema file.")

    conn.dump = _boom  # type: ignore[method-assign]

    with pytest.raises(
        db_mod.DbError,
        match=r"db drift dump failed: dbmate dump completed without producing a schema file",
    ):
        db_runtime_mod.compare_expected_schema(
            runtime=ctx,
            expected_index=1,
            context="db drift",
        )


def test_down_expected_index_comes_from_post_status(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"), _step(2, "002_next.sql"), _step(3, "003_end.sql"))
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    statuses = iter(
        [
            (
                _cmd(
                    "dbmate",
                    "status",
                    stdout="[X] 001_init.sql\n[X] 002_next.sql\n[X] 003_end.sql\nApplied: 3\n",
                ),
                db_runtime_mod.LiveStatus(
                    applied_files=("001_init.sql", "002_next.sql", "003_end.sql"),
                    applied_count=3,
                ),
            ),
            (
                _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
                db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
            ),
        ]
    )
    captured: dict[str, int] = {}

    def _fake_status(_conn: _FakeConn):
        return next(statuses)

    def _fake_verify(*, runtime: db_runtime_mod.RuntimeContext, expected_index: int, context: str):
        del runtime
        del context
        captured["expected_index"] = expected_index
        return True

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(db_runtime_mod, "read_status", _fake_status)
    monkeypatch.setattr(db_runtime_mod, "verify_expected_schema", _fake_verify)

    result = db_mod.down(ctx.target, steps=2)

    assert result.before_index == 3
    assert result.after_index == 1
    assert captured["expected_index"] == 1
    assert conn.rollback_calls == [2]


def test_down_rejects_zero_index_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
            db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
        ),
    )

    with pytest.raises(
        db_mod.DbError,
        match="db down to migration index 0 is not supported",
    ):
        db_mod.down(ctx.target, steps=1)
    assert conn.rollback_calls == []


def test_plan_uses_worktree_target_index_for_expected_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"), _step(2, "002_next.sql"))
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    captured: dict[str, int] = {}

    def _fake_compare(*, runtime: db_runtime_mod.RuntimeContext, expected_index: int, context: str):
        del runtime
        del context
        captured["expected_index"] = expected_index
        return True, "EXPECTED", "LIVE"

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
            db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
        ),
    )
    monkeypatch.setattr(db_runtime_mod, "compare_expected_schema", _fake_compare)

    result = db_mod.plan(ctx.target)

    assert captured["expected_index"] == 2
    assert result.applied_index == 1
    assert result.target_index == 2
    assert result.matches is True


def test_plan_sql_returns_worktree_target_schema(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"), _step(2, "002_next.sql"))
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
            db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
        ),
    )

    sql = db_mod.plan_sql(ctx.target)

    assert sql == "CREATE TABLE head(id INTEGER);\n"


def test_plan_diff_compares_live_vs_worktree_target(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
            db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
        ),
    )
    monkeypatch.setattr(
        db_runtime_mod,
        "expected_sql_for_index",
        lambda **kwargs: "CREATE TABLE expected(id INTEGER);\n",
    )
    monkeypatch.setattr(
        db_runtime_mod,
        "dump_live_schema",
        lambda *args, **kwargs: "CREATE TABLE live(id INTEGER);\n",
    )

    diff = db_mod.plan_diff(ctx.target)

    assert "--- live/schema.sql" in diff
    assert "+++ expected/worktree.sql" in diff
    assert "-CREATE TABLE live (id INTEGER)" in diff
    assert "+CREATE TABLE expected (id INTEGER)" in diff


def test_new_calls_dbmate_new(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = _target(tmp_path)
    captured: dict[str, object] = {}

    class _FakeDbmate:
        def __init__(self, *, migrations_dir: Path, dbmate_bin: Path | None = None) -> None:
            captured["migrations_dir"] = migrations_dir
            captured["migrations_dir_exists"] = migrations_dir.exists()
            captured["migrations_dir_is_dir"] = migrations_dir.is_dir()
            captured["dbmate_bin"] = dbmate_bin

        def new(self, name: str) -> CmdResult:
            captured["name"] = name
            return _cmd("dbmate", "new", name, stdout=f"{name}.sql\n")

    monkeypatch.setattr(db_mod, "Dbmate", _FakeDbmate)

    result = db_mod.new(target, name="add_users")

    assert result.exit_code == 0
    assert captured["migrations_dir"] == target.migrations
    assert captured["migrations_dir_exists"] is True
    assert captured["migrations_dir_is_dir"] is True
    assert captured["name"] == "add_users"


def test_new_requires_non_empty_name(tmp_path: Path) -> None:
    target = _target(tmp_path)
    with pytest.raises(db_mod.DbError, match="Migration name is required"):
        db_mod.new(target, name="   ")


def test_new_rejects_symlinked_migrations_dir(tmp_path: Path) -> None:
    target = _target(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    target.dir.mkdir(parents=True, exist_ok=True)
    target.migrations.symlink_to(outside, target_is_directory=True)

    with pytest.raises(db_mod.DbError, match="symlinked file or directory"):
        db_mod.new(target, name="init")


def test_expected_sql_for_index_uses_checkpoint_and_head(tmp_path: Path) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"), _step(2, "002_next.sql"))
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    assert db_runtime_mod.expected_sql_for_index(runtime=ctx, index=0) is None
    assert db_runtime_mod.expected_sql_for_index(runtime=ctx, index=1) == "CREATE TABLE c1(id INTEGER);\n"
    assert (
        db_runtime_mod.expected_sql_for_index(runtime=ctx, index=2) == "CREATE TABLE head(id INTEGER);\n"
    )


def test_ensure_prefix_rejects_non_prefix(tmp_path: Path) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"), _step(2, "002_next.sql"))
    ctx = _ctx(tmp_path, conn=conn, steps=steps)
    parsed = db_runtime_mod.LiveStatus(applied_files=("009_bad.sql",), applied_count=1)

    with pytest.raises(db_mod.DbError, match="does not match worktree migration prefix"):
        db_runtime_mod.ensure_prefix(state=ctx.state, live=parsed)


def test_ensure_prefix_rejects_ambiguous_basename_only_status(tmp_path: Path) -> None:
    conn = _FakeConn()
    duplicate_steps = (
        WorktreeStep(
            index=1,
            version="001",
            migration_file="migrations/a/001_init.sql",
            migration_digest="m1",
            checkpoint_file="checkpoints/a/001_init.sql",
            checkpoint_digest="c1",
            chain_hash="h1",
        ),
        WorktreeStep(
            index=2,
            version="002",
            migration_file="migrations/b/001_init.sql",
            migration_digest="m2",
            checkpoint_file="checkpoints/b/001_init.sql",
            checkpoint_digest="c2",
            chain_hash="h2",
        ),
    )
    ctx = _ctx(tmp_path, conn=conn, steps=duplicate_steps)
    parsed = db_runtime_mod.LiveStatus(
        applied_files=("001_init.sql", "001_init.sql"),
        applied_count=2,
    )

    with pytest.raises(db_mod.DbError, match="duplicate migration basenames"):
        db_runtime_mod.ensure_prefix(state=ctx.state, live=parsed)


def test_ensure_prefix_rejects_mixed_path_styles(tmp_path: Path) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"), _step(2, "002_next.sql"))
    ctx = _ctx(tmp_path, conn=conn, steps=steps)
    parsed = db_runtime_mod.LiveStatus(
        applied_files=("001_init.sql", "migrations/002_next.sql"),
        applied_count=2,
    )

    with pytest.raises(
        db_mod.DbError,
        match="mixed path styles; cannot validate live migration prefix safely",
    ):
        db_runtime_mod.ensure_prefix(state=ctx.state, live=parsed)


def test_drift_reports_live_ahead_as_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd(
                "dbmate",
                "status",
                stdout="[X] 001_init.sql\n[X] 002_extra.sql\nApplied: 2\n",
            ),
            db_runtime_mod.LiveStatus(
                applied_files=("001_init.sql", "002_extra.sql"),
                applied_count=2,
            ),
        ),
    )

    result = db_mod.drift(ctx.target)

    assert result.drifted is True
    assert result.applied_index == 2


def test_plan_reports_live_ahead_as_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd(
                "dbmate",
                "status",
                stdout="[X] 001_init.sql\n[X] 002_extra.sql\nApplied: 2\n",
            ),
            db_runtime_mod.LiveStatus(
                applied_files=("001_init.sql", "002_extra.sql"),
                applied_count=2,
            ),
        ),
    )

    result = db_mod.plan(ctx.target)

    assert result.matches is False
    assert result.applied_index == 2


def test_drift_rejects_missing_expected_baseline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    ctx = _ctx(tmp_path, conn=conn, steps=())

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="Applied: 0\n"),
            db_runtime_mod.LiveStatus(applied_files=(), applied_count=0),
        ),
    )

    with pytest.raises(db_mod.DbError, match="db drift is unavailable before the first applied migration checkpoint"):
        db_mod.drift(ctx.target)


def test_plan_rejects_missing_worktree_baseline(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    ctx = _ctx(tmp_path, conn=conn, steps=())

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="Applied: 0\n"),
            db_runtime_mod.LiveStatus(applied_files=(), applied_count=0),
        ),
    )

    with pytest.raises(db_mod.DbError, match="db plan is unavailable before the first worktree migration checkpoint"):
        db_mod.plan(ctx.target)


def test_up_rejects_live_ahead(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    conn = _FakeConn()
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd(
                "dbmate",
                "status",
                stdout="[X] 001_init.sql\n[X] 002_extra.sql\nApplied: 2\n",
            ),
            db_runtime_mod.LiveStatus(
                applied_files=("001_init.sql", "002_extra.sql"),
                applied_count=2,
            ),
        ),
    )

    with pytest.raises(db_mod.DbError, match="ahead of worktree"):
        db_mod.up(ctx.target)


@pytest.mark.parametrize(
    ("engine", "url"),
    [
        ("bigquery", "bigquery://example-project/us/target_ds"),
        ("mysql", "mysql://user:pass@127.0.0.1:3306/target_db"),
        ("clickhouse", "clickhouse://default:@127.0.0.1:8123/target_db"),
    ],
)
def test_up_rejects_qualified_pending_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    engine: str,
    url: str,
) -> None:
    conn = _FakeConn(url=url)
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)
    ctx.snapshot.migrations[steps[0].migration_file] = (
        f"-- migrate:up\n{_qualified_write(engine)}\n\n-- migrate:down\nDROP TABLE events;\n".encode()
    )

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[ ] 001_init.sql\nApplied: 0\n"),
            db_runtime_mod.LiveStatus(applied_files=(), applied_count=0),
        ),
    )

    with pytest.raises(db_mod.DbError, match=r"qualified .* write target"):
        db_mod.up(ctx.target)
    assert conn.up_calls == 0


@pytest.mark.parametrize(
    ("engine", "url"),
    [
        ("bigquery", "bigquery://example-project/us/target_ds"),
        ("mysql", "mysql://user:pass@127.0.0.1:3306/target_db"),
        ("clickhouse", "clickhouse://default:@127.0.0.1:8123/target_db"),
    ],
)
def test_migrate_rejects_qualified_pending_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    engine: str,
    url: str,
) -> None:
    conn = _FakeConn(url=url)
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)
    ctx.snapshot.migrations[steps[0].migration_file] = (
        f"-- migrate:up\n{_qualified_write(engine)}\n\n-- migrate:down\nDROP TABLE events;\n".encode()
    )

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[ ] 001_init.sql\nApplied: 0\n"),
            db_runtime_mod.LiveStatus(applied_files=(), applied_count=0),
        ),
    )

    with pytest.raises(db_mod.DbError, match=r"qualified .* write target"):
        db_mod.migrate(ctx.target)
    assert conn.migrate_calls == 0


@pytest.mark.parametrize(
    ("engine", "url"),
    [
        ("bigquery", "bigquery://example-project/us/target_ds"),
        ("mysql", "mysql://user:pass@127.0.0.1:3306/target_db"),
        ("clickhouse", "clickhouse://default:@127.0.0.1:8123/target_db"),
    ],
)
def test_down_rejects_qualified_executable_down_writes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    engine: str,
    url: str,
) -> None:
    conn = _FakeConn(url=url)
    steps = (_step(1, "001_init.sql"),)
    ctx = _ctx(tmp_path, conn=conn, steps=steps)
    ctx.snapshot.migrations[steps[0].migration_file] = (
        f"-- migrate:up\nCREATE TABLE events (id BIGINT);\n\n-- migrate:down\n{_qualified_down(engine)}\n".encode()
    )

    @contextmanager
    def _fake_open_ctx(**kwargs):
        del kwargs
        yield ctx

    monkeypatch.setattr(db_runtime_mod, "open_runtime", _fake_open_ctx)
    monkeypatch.setattr(
        db_runtime_mod,
        "read_status",
        lambda _conn: (
            _cmd("dbmate", "status", stdout="[X] 001_init.sql\nApplied: 1\n"),
            db_runtime_mod.LiveStatus(applied_files=("001_init.sql",), applied_count=1),
        ),
    )

    with pytest.raises(db_mod.DbError, match=r"qualified .* write target"):
        db_mod.down(ctx.target)
    assert conn.rollback_calls == []
