from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from matey.db import DbEngine
from matey.errors import (
    BigQueryPreflightError,
    LiveDriftError,
    LiveHistoryMismatchError,
    SchemaMismatchError,
)
from matey.models import ConfigDefaults, Engine, SchemaStatusResult
from tests.unit.app.helpers import (
    FakeScratch,
    ScriptedDbmate,
    build_context,
    build_runtime,
    cmd_result,
    write_lock_for_runtime,
)


class _FreshSchemaEngine:
    def schema_status(self, **kwargs) -> SchemaStatusResult:
        del kwargs
        return SchemaStatusResult(up_to_date=True, stale=False, rows=(), summary=("state=up-to-date",))


def _write_migration(path: Path, *, up_sql: str, down_sql: str | None = None) -> None:
    lines = ["-- migrate:up", up_sql]
    if down_sql is not None:
        lines.extend(["-- migrate:down", down_sql])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_db_status_returns_raw_passthrough_text(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    dbmate = ScriptedDbmate()
    weird = "?? not standard status output @@\nline2\n"
    dbmate.queue("status", cmd_result(stdout=weird))
    engine = DbEngine(context=build_context(repo_root=repo, dbmate=dbmate), schema_engine=_FreshSchemaEngine())
    assert engine.db_status(runtime=runtime, url_override="sqlite3:/tmp/live.db") == weird


def test_db_up_fails_precheck_on_live_drift_before_mutation(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")
    (runtime.paths.checkpoints_dir / "001_init.sql").write_text(
        "CREATE TABLE a(id INTEGER);\n", encoding="utf-8"
    )
    write_lock_for_runtime(
        runtime=runtime,
        repo_root=repo,
        engine=Engine.SQLITE,
        schema_sql="CREATE TABLE a(id INTEGER);\n",
    )

    dbmate = ScriptedDbmate()
    dbmate.queue("status", cmd_result(stdout="[X] migrations/001_init.sql\napplied: 1\n"))
    dbmate.queue("dump", cmd_result(stdout="CREATE TABLE wrong(id INTEGER);\n"))

    engine = DbEngine(context=build_context(repo_root=repo, dbmate=dbmate), schema_engine=_FreshSchemaEngine())
    with pytest.raises(LiveDriftError):
        engine.db_up(
            runtime=runtime,
            defaults=ConfigDefaults(),
            url_override="sqlite3:/tmp/live.db",
            test_url_override=None,
            keep_scratch=False,
        )
    assert "up" not in dbmate.calls


def test_db_drift_rejects_lock_prefix_mismatch(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")
    (runtime.paths.checkpoints_dir / "001_init.sql").write_text(
        "CREATE TABLE a(id INTEGER);\n", encoding="utf-8"
    )
    write_lock_for_runtime(
        runtime=runtime,
        repo_root=repo,
        engine=Engine.SQLITE,
        schema_sql="CREATE TABLE a(id INTEGER);\n",
    )

    dbmate = ScriptedDbmate()
    dbmate.queue("status", cmd_result(stdout="[X] migrations/999_other.sql\napplied: 1\n"))

    engine = DbEngine(context=build_context(repo_root=repo, dbmate=dbmate), schema_engine=_FreshSchemaEngine())
    with pytest.raises(LiveHistoryMismatchError):
        engine.db_drift(
            runtime=runtime,
            defaults=ConfigDefaults(),
            url_override="sqlite3:/tmp/live.db",
            test_url_override=None,
            keep_scratch=False,
        )


def test_db_drift_bigquery_index0_requires_test_url(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    runtime.paths.lock_file.write_text(
        """
lock_version = 0
hash_algorithm = "blake2b-256"
canonicalizer = "matey-sql-v0"
engine = "bigquery"
target = "core"
schema_file = "schema.sql"
migrations_dir = "migrations"
checkpoints_dir = "checkpoints"
head_index = 0
head_chain_hash = "x"
head_schema_digest = "x"
steps = []
""".strip()
        + "\n",
        encoding="utf-8",
    )
    runtime.paths.schema_file.write_text("", encoding="utf-8")

    dbmate = ScriptedDbmate()
    dbmate.queue("status", cmd_result(stdout="applied: 0\n"))
    engine = DbEngine(context=build_context(repo_root=repo, dbmate=dbmate), schema_engine=_FreshSchemaEngine())

    with pytest.raises(SchemaMismatchError, match="Index-0 baseline"):
        engine.db_drift(
            runtime=runtime,
            defaults=ConfigDefaults(),
            url_override="bigquery://project/us/dataset",
            test_url_override=None,
            keep_scratch=False,
        )


def test_bigquery_preflight_create_classifier_branches(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    runtime.paths.lock_file.write_text(
        """
lock_version = 0
hash_algorithm = "blake2b-256"
canonicalizer = "matey-sql-v0"
engine = "bigquery"
target = "core"
schema_file = "schema.sql"
migrations_dir = "migrations"
checkpoints_dir = "checkpoints"
head_index = 0
head_chain_hash = "x"
head_schema_digest = "x"
steps = []
""".strip()
        + "\n",
        encoding="utf-8",
    )
    runtime.paths.schema_file.write_text("", encoding="utf-8")

    # Fatal branch
    fatal_dbmate = ScriptedDbmate()
    fatal_dbmate.queue("create", cmd_result(exit_code=1, stderr="permission denied"))
    fatal_engine = DbEngine(
        context=build_context(repo_root=repo, dbmate=fatal_dbmate),
        schema_engine=_FreshSchemaEngine(),
    )
    op, lock = fatal_engine._build_db_context(
        runtime=runtime,
        url_override="bigquery://project/us/dataset",
        test_url_override="bigquery://project/us/test_base",
        keep_scratch=False,
    )
    with pytest.raises(BigQueryPreflightError):
        fatal_engine._preflight_status(op=op, lock=lock, verb="up")

    # Non-fatal "already exists" branch
    ok_dbmate = ScriptedDbmate()
    ok_dbmate.queue("create", cmd_result(exit_code=1, stderr="already exists"))
    ok_dbmate.queue("status", cmd_result(stdout="applied: 0\n"))
    ok_engine = DbEngine(
        context=build_context(repo_root=repo, dbmate=ok_dbmate),
        schema_engine=_FreshSchemaEngine(),
    )
    op_ok, lock_ok = ok_engine._build_db_context(
        runtime=runtime,
        url_override="bigquery://project/us/dataset",
        test_url_override="bigquery://project/us/test_base",
        keep_scratch=False,
    )
    snapshot = ok_engine._preflight_status(op=op_ok, lock=lock_ok, verb="up")
    assert snapshot.applied_count == 0


def test_db_down_runs_guarded_pre_and_post_checks(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(
        runtime.paths.migrations_dir / "001_init.sql",
        up_sql="CREATE TABLE a(id INTEGER);",
        down_sql="DROP TABLE a;",
    )
    (runtime.paths.checkpoints_dir / "001_init.sql").write_text(
        "CREATE TABLE a(id INTEGER);\n", encoding="utf-8"
    )
    write_lock_for_runtime(
        runtime=runtime,
        repo_root=repo,
        engine=Engine.SQLITE,
        schema_sql="CREATE TABLE a(id INTEGER);\n",
    )

    dbmate = ScriptedDbmate()
    dbmate.queue("status", cmd_result(stdout="[X] migrations/001_init.sql\napplied: 1\n"))
    dbmate.queue("dump", cmd_result(stdout="CREATE TABLE a(id INTEGER);\n"))
    dbmate.queue("rollback", cmd_result())
    dbmate.queue("status", cmd_result(stdout="applied: 0\n"))
    dbmate.queue("create", cmd_result())
    dbmate.queue("dump", cmd_result(stdout="-- baseline\n"))
    dbmate.queue("dump", cmd_result(stdout="-- baseline\n"))

    engine = DbEngine(context=build_context(repo_root=repo, dbmate=dbmate), schema_engine=_FreshSchemaEngine())
    engine.db_down(
        runtime=runtime,
        defaults=ConfigDefaults(),
        steps=1,
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
        keep_scratch=False,
    )

    assert "rollback" in dbmate.calls


@pytest.mark.parametrize(
    ("method_name", "expected_mutation_call"),
    (
        ("db_up", "up"),
        ("db_migrate", "migrate"),
    ),
)
def test_db_up_and_migrate_accept_multiple_pending_migrations(
    tmp_path: Path,
    method_name: str,
    expected_mutation_call: str,
) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")
    _write_migration(runtime.paths.migrations_dir / "002_next.sql", up_sql="CREATE TABLE b(id INTEGER);")
    (runtime.paths.checkpoints_dir / "001_init.sql").write_text("CREATE TABLE a(id INTEGER);\n", encoding="utf-8")
    (runtime.paths.checkpoints_dir / "002_next.sql").write_text(
        "CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
        encoding="utf-8",
    )
    write_lock_for_runtime(
        runtime=runtime,
        repo_root=repo,
        engine=Engine.SQLITE,
        schema_sql="CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
    )

    dbmate = ScriptedDbmate()
    dbmate.queue("status", cmd_result(stdout="applied: 0\n"))
    dbmate.queue("create", cmd_result())
    dbmate.queue("dump", cmd_result(stdout="-- baseline\n"))
    dbmate.queue("dump", cmd_result(stdout="-- baseline\n"))
    dbmate.queue(expected_mutation_call, cmd_result())
    dbmate.queue("status", cmd_result(stdout="[X] migrations/001_init.sql\n[X] migrations/002_next.sql\napplied: 2\n"))
    dbmate.queue("dump", cmd_result(stdout="CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n"))

    engine = DbEngine(context=build_context(repo_root=repo, dbmate=dbmate), schema_engine=_FreshSchemaEngine())
    getattr(engine, method_name)(
        runtime=runtime,
        defaults=ConfigDefaults(),
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
        keep_scratch=False,
    )

    assert expected_mutation_call in dbmate.calls


def test_db_plan_returns_diff_payload_instead_of_raising(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")
    (runtime.paths.checkpoints_dir / "001_init.sql").write_text("CREATE TABLE a(id INTEGER);\n", encoding="utf-8")
    write_lock_for_runtime(
        runtime=runtime,
        repo_root=repo,
        engine=Engine.SQLITE,
        schema_sql="CREATE TABLE a(id INTEGER);\n",
    )

    dbmate = ScriptedDbmate()
    dbmate.queue("status", cmd_result(stdout="[X] migrations/001_init.sql\napplied: 1\n"))
    dbmate.queue("dump", cmd_result(stdout="CREATE TABLE a(id INTEGER, changed TEXT);\n"))

    engine = DbEngine(context=build_context(repo_root=repo, dbmate=dbmate), schema_engine=_FreshSchemaEngine())
    plan = engine.db_plan(
        runtime=runtime,
        defaults=ConfigDefaults(),
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
        keep_scratch=False,
    )

    assert plan.result.comparison.equal is False
    assert plan.result.comparison.diff


def test_db_drift_honors_keep_scratch(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    runtime.paths.lock_file.write_text(
        """
lock_version = 0
hash_algorithm = "blake2b-256"
canonicalizer = "matey-sql-v0"
engine = "sqlite"
target = "core"
schema_file = "schema.sql"
migrations_dir = "migrations"
checkpoints_dir = "checkpoints"
head_index = 0
head_chain_hash = "x"
head_schema_digest = "x"
steps = []
""".strip()
        + "\n",
        encoding="utf-8",
    )
    runtime.paths.schema_file.write_text("", encoding="utf-8")

    dbmate = ScriptedDbmate()
    dbmate.queue("status", cmd_result(stdout="applied: 0\n"))
    dbmate.queue("create", cmd_result())
    dbmate.queue("dump", cmd_result(stdout="-- schema\n"))
    dbmate.queue("dump", cmd_result(stdout="-- schema\n"))

    scratch = FakeScratch()
    ctx = build_context(repo_root=repo, dbmate=dbmate, scratch=scratch)
    engine = DbEngine(context=ctx, schema_engine=_FreshSchemaEngine())
    engine.db_drift(
        runtime=runtime,
        defaults=ConfigDefaults(),
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
        keep_scratch=True,
    )

    assert scratch.cleaned == []


def test_target_mutating_commands_run_under_scope_lock(tmp_path: Path) -> None:
    class RecordingScope:
        def __init__(self) -> None:
            self.opens: list[Path] = []

        @contextmanager
        def open(self, *, target_key, target_root: Path):
            del target_key
            self.opens.append(target_root)
            yield

    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    dbmate = ScriptedDbmate()
    scope = RecordingScope()
    engine = DbEngine(
        context=build_context(repo_root=repo, dbmate=dbmate, scope=scope),
        schema_engine=_FreshSchemaEngine(),
    )

    schema_file = runtime.paths.db_dir / "seed.sql"
    schema_file.write_text("CREATE TABLE x(id INTEGER);\n", encoding="utf-8")
    engine.db_create(runtime=runtime, url_override="sqlite3:/tmp/live.db")
    engine.db_load(runtime=runtime, url_override="sqlite3:/tmp/live.db", schema_path=schema_file)
    engine.db_drop(runtime=runtime, url_override="sqlite3:/tmp/live.db")
    engine.db_raw(runtime=runtime, url_override="sqlite3:/tmp/live.db", argv_suffix=("status",))

    assert len(scope.opens) == 4
    assert all(path == runtime.paths.db_dir for path in scope.opens)
