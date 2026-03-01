from __future__ import annotations

from pathlib import Path

import pytest

from matey.errors import ReplayError
from matey.lock import (
    LockStep,
    SchemaLock,
    digest_bytes_blake2b256,
    lock_chain_seed,
    lock_chain_step,
)
from matey.models import Engine, SqlSource, derive_target_key
from matey.schema import ReplayPlan as _ReplayPlan
from matey.schema import SchemaEngine, build_replay_plan, workspace_migrations
from matey.sql import SqlPipeline
from tests.unit.app.helpers import (
    FakeEnv,
    FakeGit,
    ScriptedDbmate,
    build_context,
    build_runtime,
    cmd_result,
    default_defaults,
    write_lock_for_runtime,
)


def _write_migration(path: Path, *, up_sql: str, down_sql: str | None = None) -> None:
    lines = ["-- migrate:up", up_sql]
    if down_sql is not None:
        lines.extend(["-- migrate:down", down_sql])
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_build_replay_plan_selects_base_checkpoint_anchor_after_divergence(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)

    mig_001 = runtime.paths.migrations_dir / "001_init.sql"
    mig_002 = runtime.paths.migrations_dir / "002_add_widget.sql"
    _write_migration(mig_001, up_sql="CREATE TABLE a(id INTEGER);")
    _write_migration(mig_002, up_sql="ALTER TABLE a ADD COLUMN name TEXT;")
    (runtime.paths.checkpoints_dir / "001_init.sql").write_text(
        "CREATE TABLE a(id INTEGER);\n", encoding="utf-8"
    )
    (runtime.paths.checkpoints_dir / "002_add_widget.sql").write_text(
        "CREATE TABLE a(id INTEGER, name TEXT);\n", encoding="utf-8"
    )
    write_lock_for_runtime(
        runtime=runtime,
        repo_root=repo,
        engine=Engine.SQLITE,
        schema_sql="CREATE TABLE a(id INTEGER, name TEXT);\n",
    )

    git = FakeGit(repo_root=repo)
    git.head = "head"
    git.merge_base_value = "base"
    git.tree_paths[("base", "db/core/migrations")] = (Path("db/core/migrations/001_init.sql"),)
    git.tree_paths[("base", "db/core/checkpoints")] = (Path("db/core/checkpoints/001_init.sql"),)
    git.blobs[("base", "db/core/migrations/001_init.sql")] = mig_001.read_bytes()
    git.blobs[("base", "db/core/checkpoints/001_init.sql")] = b"CREATE TABLE a(id INTEGER);\n"
    base_target_key = derive_target_key(repo_root=repo, db_dir=runtime.paths.db_dir)
    base_seed = lock_chain_seed(Engine.SQLITE, base_target_key)
    base_migration_digest = digest_bytes_blake2b256(mig_001.read_bytes())
    base_chain = lock_chain_step(
        base_seed,
        "001",
        "migrations/001_init.sql",
        base_migration_digest,
    )
    base_checkpoint_text = "CREATE TABLE a(id INTEGER);\n"
    base_schema_digest = SqlPipeline().prepare(
        engine=Engine.SQLITE,
        source=SqlSource(text=base_checkpoint_text, origin="artifact"),
    ).digest
    base_lock = SchemaLock(
        lock_version=0,
        hash_algorithm="blake2b-256",
        canonicalizer="matey-sql-v0",
        engine="sqlite",
        target="core",
        schema_file="schema.sql",
        migrations_dir="migrations",
        checkpoints_dir="checkpoints",
        head_index=1,
        head_chain_hash=base_chain,
        head_schema_digest=base_schema_digest,
        steps=(
            LockStep(
                index=1,
                version="001",
                migration_file="migrations/001_init.sql",
                migration_digest=base_migration_digest,
                checkpoint_file="checkpoints/001_init.sql",
                checkpoint_digest=digest_bytes_blake2b256(base_checkpoint_text.encode("utf-8")),
                schema_digest=base_schema_digest,
                chain_hash=base_chain,
            ),
        ),
    )
    git.blobs[("base", "db/core/schema.lock.toml")] = base_lock.to_toml().encode("utf-8")

    engine = SchemaEngine(context=build_context(repo_root=repo, git=git))
    op = engine._build_schema_op_context(
        runtime=runtime,
        defaults=default_defaults(),
        base_ref="origin/main",
        clean=False,
        keep_scratch=False,
        url_override="sqlite3:/tmp/live.db",
        test_url_override="sqlite3:/tmp/test.db",
    )
    replay = build_replay_plan(ctx=engine._ctx, op=op)

    assert replay.divergence == 2
    assert replay.anchor_sql == "CREATE TABLE a(id INTEGER);\n"
    assert [row.migration.rel_path for row in replay.tail_states] == [
        "migrations/002_add_widget.sql"
    ]


def test_build_replay_plan_clean_uses_full_tail_without_git(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")
    _write_migration(runtime.paths.migrations_dir / "002_next.sql", up_sql="CREATE TABLE b(id INTEGER);")

    git = FakeGit(repo_root=repo)
    engine = SchemaEngine(context=build_context(repo_root=repo, git=git))
    op = engine._build_schema_op_context(
        runtime=runtime,
        defaults=default_defaults(),
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
    )
    replay = build_replay_plan(ctx=engine._ctx, op=op)

    assert replay.divergence == 1
    assert replay.anchor_sql is None
    assert len(replay.tail_states) == 2


def test_build_replay_plan_local_mode_skips_git_base_resolution(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")

    git = FakeGit(repo_root=repo)

    def _boom(left_ref: str, right_ref: str) -> str:
        raise AssertionError(f"merge_base should not be called in local mode: {left_ref}, {right_ref}")

    git.merge_base = _boom  # type: ignore[method-assign]
    engine = SchemaEngine(context=build_context(repo_root=repo, git=git))
    op = engine._build_schema_op_context(
        runtime=runtime,
        defaults=default_defaults(),
        base_ref=None,
        clean=False,
        keep_scratch=False,
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
    )
    replay = build_replay_plan(ctx=engine._ctx, op=op)
    assert replay.divergence == 1
    assert [row.migration.rel_path for row in replay.tail_states] == ["migrations/001_init.sql"]


def test_build_replay_plan_uses_ci_base_env_when_base_not_passed(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")

    git = FakeGit(repo_root=repo)
    git.merge_base_value = "base"
    env = FakeEnv({"GITHUB_BASE_REF": "origin/main"})
    engine = SchemaEngine(context=build_context(repo_root=repo, git=git, env=env))
    op = engine._build_schema_op_context(
        runtime=runtime,
        defaults=default_defaults(),
        base_ref=None,
        clean=False,
        keep_scratch=False,
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
    )
    replay = build_replay_plan(ctx=engine._ctx, op=op)
    assert replay.divergence == 1


def test_build_replay_plan_rejects_base_schema_without_migrations(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")

    git = FakeGit(repo_root=repo)
    git.head = "head"
    git.merge_base_value = "base"
    git.blobs[("base", "db/core/schema.sql")] = b"CREATE TABLE legacy(id INTEGER);\n"

    engine = SchemaEngine(context=build_context(repo_root=repo, git=git))
    op = engine._build_schema_op_context(
        runtime=runtime,
        defaults=default_defaults(),
        base_ref="origin/main",
        clean=False,
        keep_scratch=False,
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
    )
    with pytest.raises(ReplayError, match="schema file exists without migrations"):
        build_replay_plan(ctx=engine._ctx, op=op)


def test_build_replay_plan_rejects_base_checkpoints_without_migrations(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")

    git = FakeGit(repo_root=repo)
    git.head = "head"
    git.merge_base_value = "base"
    git.tree_paths[("base", "db/core/checkpoints")] = (Path("db/core/checkpoints/001_init.sql"),)
    git.blobs[("base", "db/core/checkpoints/001_init.sql")] = b"CREATE TABLE orphan(id INTEGER);\n"

    engine = SchemaEngine(context=build_context(repo_root=repo, git=git))
    op = engine._build_schema_op_context(
        runtime=runtime,
        defaults=default_defaults(),
        base_ref="origin/main",
        clean=False,
        keep_scratch=False,
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
    )
    with pytest.raises(ReplayError, match="checkpoints exist without migrations"):
        build_replay_plan(ctx=engine._ctx, op=op)


def test_schema_status_reports_missing_and_orphan_checkpoints(tmp_path: Path) -> None:
    from tests.unit.app.helpers import write_lock_for_runtime

    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)
    _write_migration(runtime.paths.migrations_dir / "001_init.sql", up_sql="CREATE TABLE a(id INTEGER);")
    checkpoint = runtime.paths.checkpoints_dir / "001_init.sql"
    checkpoint.write_text("CREATE TABLE a(id INTEGER);\n", encoding="utf-8")
    write_lock_for_runtime(
        runtime=runtime,
        repo_root=repo,
        engine=Engine.SQLITE,
        schema_sql="CREATE TABLE a(id INTEGER);\n",
    )

    checkpoint.unlink()
    (runtime.paths.checkpoints_dir / "999_orphan.sql").write_text("-- orphan\n", encoding="utf-8")

    engine = SchemaEngine(context=build_context(repo_root=repo))
    result = engine.schema_status(runtime=runtime, defaults=default_defaults(), base_ref=None)

    assert result.stale is True
    statuses = {row.status for row in result.rows}
    assert "checkpoint-missing" in statuses
    assert "orphan-checkpoint" in statuses


def test_schema_status_reports_missing_schema_sql(tmp_path: Path) -> None:
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
    runtime.paths.schema_file.unlink()

    engine = SchemaEngine(context=build_context(repo_root=repo))
    result = engine.schema_status(runtime=runtime, defaults=default_defaults(), base_ref=None)

    assert result.stale is True
    assert any(row.status == "schema-missing" for row in result.rows)


def test_schema_status_reports_lock_target_mismatch(tmp_path: Path) -> None:
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
    runtime.paths.lock_file.write_text(
        runtime.paths.lock_file.read_text(encoding="utf-8").replace('target = "core"', 'target = "other"', 1),
        encoding="utf-8",
    )

    engine = SchemaEngine(context=build_context(repo_root=repo))
    result = engine.schema_status(runtime=runtime, defaults=default_defaults(), base_ref=None)

    assert result.stale is True
    assert any(row.status == "target-mismatch" for row in result.rows)


def test_execute_down_roundtrip_only_rolls_back_reversible_steps(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    runtime = build_runtime(repo_root=repo)

    _write_migration(
        runtime.paths.migrations_dir / "001_irreversible.sql",
        up_sql="CREATE TABLE a(id INTEGER);",
        down_sql="-- no-op",
    )
    _write_migration(
        runtime.paths.migrations_dir / "002_reversible.sql",
        up_sql="CREATE TABLE b(id INTEGER);",
        down_sql="DROP TABLE b;",
    )

    dbmate = ScriptedDbmate()
    dbmate.queue("create", cmd_result())
    dbmate.queue("up", cmd_result(), cmd_result(), cmd_result())
    dbmate.queue("rollback", cmd_result())
    dbmate.queue("dump", cmd_result(stdout="CREATE TABLE a(id INTEGER);\n"), cmd_result(stdout="CREATE TABLE a(id INTEGER);\n"))

    engine = SchemaEngine(context=build_context(repo_root=repo, dbmate=dbmate))
    op = engine._build_schema_op_context(
        runtime=runtime,
        defaults=default_defaults(),
        base_ref=None,
        clean=True,
        keep_scratch=False,
        url_override="sqlite3:/tmp/live.db",
        test_url_override=None,
    )
    replay_plan = _ReplayPlan(
        divergence=1,
        anchor_sql=None,
        tail_states=workspace_migrations(runtime.paths),
        base_states=(),
        orphan_checkpoints=(),
    )

    engine._execute_down_roundtrip(op=op, replay_plan=replay_plan)

    assert dbmate.calls == [
        "create",
        "up",
        "dump",
        "up",
        "rollback",
        "dump",
        "up",
    ]
