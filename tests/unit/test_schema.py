from __future__ import annotations

import importlib
from contextlib import contextmanager
from pathlib import Path

import pygit2
import pytest

import matey.schema as schema_mod
import matey.scratch as scratch_mod
import matey.sql as sql_mod
from matey.config import TargetConfig
from matey.dbmate import CmdResult, DbmateError
from matey.lockfile import LockFile, LockPolicy, LockState
from matey.repo import Snapshot
from matey.schema import SchemaError, apply, plan, plan_diff, plan_sql, status
from matey.scratch import Engine

schema_plan_mod = importlib.import_module("matey.schema.plan")
schema_replay_mod = importlib.import_module("matey.schema.replay")


def _cmd(*argv: str, exit_code: int = 0, stdout: str = "", stderr: str = "") -> CmdResult:
    return CmdResult(argv=tuple(argv), exit_code=exit_code, stdout=stdout, stderr=stderr)


def _target(tmp_path: Path, name: str = "core") -> TargetConfig:
    return TargetConfig(
        name=name,
        dir=(tmp_path / "db" / name).resolve(),
        url_env=f"{name.upper()}_DATABASE_URL",
        test_url_env=f"{name.upper()}_TEST_DATABASE_URL",
    )


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _commit_all(repo: pygit2.Repository, message: str) -> pygit2.Oid:
    signature = pygit2.Signature("Matey Test", "matey-test@example.com")
    index = repo.index
    index.add_all()
    index.write()
    tree_id = index.write_tree()
    parent = repo.head.target if not repo.head_is_unborn else None
    parents = [parent] if parent is not None else []
    return repo.create_commit("HEAD", signature, signature, message, tree_id, parents)


def _single_step_lock(
    *,
    target: str,
    engine: str,
    migration_file: str,
    migration_digest: str,
    checkpoint_file: str,
    checkpoint_digest: str,
    schema_digest: str,
    policy: LockPolicy,
    step_version: str = "001",
) -> str:
    chain_seed = policy.chain_seed(engine=engine, target=target)
    chain = policy.chain_step(
        previous=chain_seed,
        version=step_version,
        migration_file=migration_file,
        migration_digest=migration_digest,
    )
    return (
        f"""
lock_version = {policy.lock_version}
hash_algorithm = "{policy.hash_algorithm}"
canonicalizer = "{policy.canonicalizer}"
engine = "{engine}"
target = "{target}"
schema_file = "{policy.schema_file}"
migrations_dir = "{policy.migrations_dir}"
checkpoints_dir = "{policy.checkpoints_dir}"
head_index = 1
head_chain_hash = "{chain}"
head_schema_digest = "{schema_digest}"

[[steps]]
index = 1
version = "{step_version}"
migration_file = "{migration_file}"
migration_digest = "{migration_digest}"
checkpoint_file = "{checkpoint_file}"
checkpoint_digest = "{checkpoint_digest}"
schema_digest = "{schema_digest}"
chain_hash = "{chain}"
""".strip()
        + "\n"
    )


def _check_outcome(*, schema_sql: str, checkpoint_map: dict[str, str]) -> schema_replay_mod.ReplayOutcome:
    return schema_replay_mod.ReplayOutcome(
        replay_scratch_url="sqlite3:/tmp/matey-schema-test-replay.sqlite3",
        down_scratch_url="sqlite3:/tmp/matey-schema-test-down.sqlite3",
        replay_schema_sql=schema_sql,
        checkpoint_sql_by_file=checkpoint_map,
        down_checked=(),
        down_skipped=(),
    )


def _scratch_base_url(engine: str) -> str:
    match engine:
        case "bigquery":
            return "bigquery://example-project/us/target_ds"
        case "mysql":
            return "mysql://user:pass@127.0.0.1:3306/target_db"
        case "clickhouse":
            return "clickhouse://default:@127.0.0.1:8123/target_db"
        case _:
            return "sqlite3:/tmp/schema-test.sqlite3"


def test_status_returns_lock_state_for_target(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")

    state = status(target)

    assert state.target_name == target.name
    assert len(state.worktree_steps) == 1


def test_plan_local_without_lock_uses_full_tail(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    _write(target.migrations / "002_next.sql", "-- migrate:up\nCREATE TABLE b(id INTEGER);\n")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
            checkpoint_map={},
        ),
    )

    computed = plan(target, test_base_url="sqlite3:/tmp/schema-local.sqlite3")

    assert computed.divergence_index == 1
    assert computed.anchor_index == 0
    assert computed.tail_count == 2
    assert computed.matches is False
    assert computed.replay_scratch_url.startswith("sqlite3:")
    assert computed.down_scratch_url.startswith("sqlite3:")


def test_plan_local_with_coherent_lock_is_noop(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    policy = LockPolicy()
    migration_sql = "-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = "CREATE TABLE a(id INTEGER);\n"
    migration_file = "migrations/001_init.sql"
    checkpoint_file = "checkpoints/001_init.sql"

    _write(target.migrations / "001_init.sql", migration_sql)
    _write(target.checkpoints / "001_init.sql", checkpoint_sql)
    _write(target.schema, checkpoint_sql)
    _write(
        target.lockfile,
        _single_step_lock(
            target=target.name,
            engine="sqlite",
            migration_file=migration_file,
            migration_digest=policy.digest(migration_sql.encode("utf-8")),
            checkpoint_file=checkpoint_file,
            checkpoint_digest=policy.digest(checkpoint_sql.encode("utf-8")),
            schema_digest=policy.digest(checkpoint_sql.encode("utf-8")),
            policy=policy,
        ),
    )
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql=checkpoint_sql,
            checkpoint_map={},
        ),
    )

    computed = plan(target)

    assert computed.divergence_index is None
    assert computed.anchor_index == 1
    assert computed.tail_count == 0
    assert computed.matches is True


def test_plan_clean_forces_full_replay(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={},
        ),
    )

    computed = plan(target, clean=True, test_base_url="sqlite3:/tmp/schema-clean.sqlite3")

    assert computed.anchor_index == 0
    assert computed.divergence_index == 1
    assert computed.tail_count == 1
    assert computed.matches is False


@pytest.mark.parametrize(
    ("engine", "qualified_write"),
    [
        ("bigquery", "CREATE TABLE analytics.events (id INT64);"),
        ("mysql", "CREATE TABLE other_db.events (id BIGINT);"),
        ("clickhouse", "CREATE TABLE other_db.events (id Int64) ENGINE = MergeTree ORDER BY tuple();"),
    ],
)
def test_plan_rejects_qualified_writes_in_up_section(
    tmp_path: Path,
    engine: str,
    qualified_write: str,
) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        f"-- migrate:up\n{qualified_write}\n\n-- migrate:down\nDROP TABLE events;\n",
    )

    with pytest.raises(SchemaError, match=r"qualified .* write target"):
        plan(target, test_base_url=_scratch_base_url(engine))


@pytest.mark.parametrize(
    ("engine", "qualified_write"),
    [
        ("bigquery", "DROP TABLE analytics.events;"),
        ("mysql", "DROP TABLE other_db.events;"),
        ("clickhouse", "DROP TABLE other_db.events;"),
    ],
)
def test_plan_rejects_qualified_writes_in_executable_down_section(
    tmp_path: Path,
    engine: str,
    qualified_write: str,
) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        f"-- migrate:up\nCREATE TABLE events (id BIGINT);\n\n-- migrate:down\n{qualified_write}\n",
    )

    with pytest.raises(SchemaError, match=r"qualified .* write target"):
        plan(target, test_base_url=_scratch_base_url(engine))


def test_plan_wraps_invalid_utf8_migration_as_schema_error(tmp_path: Path) -> None:
    target = _target(tmp_path)
    target.dir.mkdir(parents=True, exist_ok=True)
    bad_path = target.migrations / "001_init.sql"
    bad_path.parent.mkdir(parents=True, exist_ok=True)
    bad_path.write_bytes(b"\xff\xfe\x00")

    with pytest.raises(
        SchemaError,
        match=r"Unable to decode migration migrations/001_init\.sql as UTF-8",
    ):
        plan(target, test_base_url="mysql://user:pass@127.0.0.1:3306/target_db")


def test_plan_wraps_invalid_utf8_worktree_schema_as_schema_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    target.schema.write_bytes(b"\xff\xfe\x00")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={},
        ),
    )

    with pytest.raises(SchemaError, match=r"Unable to decode schema\.sql as UTF-8"):
        plan(target, test_base_url="sqlite3:/tmp/schema-invalid-schema.sqlite3")


def test_dump_schema_wraps_dbmate_error(tmp_path: Path) -> None:
    class _BoomConn:
        def dump(self) -> CmdResult:
            raise DbmateError("dbmate dump completed without producing a schema file.")

    with pytest.raises(
        SchemaError,
        match=r"replay dump failed: dbmate dump completed without producing a schema file",
    ):
        schema_replay_mod.dump_schema(_BoomConn(), context="replay")


def test_plan_base_uses_merge_base_vs_worktree(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    repo = pygit2.init_repository(str(tmp_path), initial_head="main")
    target = _target(tmp_path)
    policy = LockPolicy()
    migration_sql = "-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = "CREATE TABLE a(id INTEGER);\n"
    _write(target.migrations / "001_init.sql", migration_sql)
    _write(target.checkpoints / "001_init.sql", checkpoint_sql)
    _write(target.schema, checkpoint_sql)
    _write(
        target.lockfile,
        _single_step_lock(
            target=target.name,
            engine="sqlite",
            migration_file="migrations/001_init.sql",
            migration_digest=policy.digest(migration_sql.encode("utf-8")),
            checkpoint_file="checkpoints/001_init.sql",
            checkpoint_digest=policy.digest(checkpoint_sql.encode("utf-8")),
            schema_digest=policy.digest(checkpoint_sql.encode("utf-8")),
            policy=policy,
        ),
    )
    _commit_all(repo, "base")
    _write(target.migrations / "002_next.sql", "-- migrate:up\nCREATE TABLE b(id INTEGER);\n")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
            checkpoint_map={},
        ),
    )

    computed = plan(
        target,
        base_ref="refs/heads/main",
        test_base_url="sqlite3:/tmp/schema-base.sqlite3",
    )

    assert computed.divergence_index == 2
    assert computed.anchor_index == 1
    assert computed.tail_count == 1
    assert computed.matches is False


def test_plan_sql_returns_replay_b(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    expected = "CREATE TABLE a(id INTEGER);\n"
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql=expected,
            checkpoint_map={},
        ),
    )

    sql = plan_sql(target, test_base_url="sqlite3:/tmp/schema-plan-sql.sqlite3")

    assert sql == expected


def test_plan_sql_does_not_require_compare_path(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={},
        ),
    )
    monkeypatch.setattr(
        schema_mod,
        "compare_replay_sql",
        lambda **kwargs: (_ for _ in ()).throw(AssertionError("compare should not run")),
    )

    sql = plan_sql(target, test_base_url="sqlite3:/tmp/schema-plan-sql.sqlite3")

    assert sql == "CREATE TABLE a(id INTEGER);\n"


def test_plan_diff_compares_worktree_vs_replay(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    _write(target.schema, "CREATE TABLE old_table(id INTEGER);\n")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE new_table(id INTEGER);\n",
            checkpoint_map={},
        ),
    )

    diff = plan_diff(target, test_base_url="sqlite3:/tmp/schema-plan-diff.sqlite3")

    assert "--- worktree/schema.sql" in diff
    assert "+++ replay/schema.sql" in diff
    assert "-CREATE TABLE old_table (id INTEGER)" in diff
    assert "+CREATE TABLE new_table (id INTEGER)" in diff


def test_plan_rejects_clean_plus_base_ref(tmp_path: Path) -> None:
    target = _target(tmp_path)
    with pytest.raises(SchemaError):
        plan(target, clean=True, base_ref="refs/heads/main")


def test_apply_writes_schema_lock_and_tail_checkpoints(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={"checkpoints/001_init.sql": "CREATE TABLE a(id INTEGER);\n"},
        ),
    )

    result = apply(target, test_base_url="sqlite3:/tmp/schema-apply.sqlite3")

    assert result.wrote is True
    assert "schema.sql" in result.changed_files
    assert "schema.lock.toml" in result.changed_files
    assert "checkpoints/001_init.sql" in result.changed_files
    assert target.schema.exists()
    assert target.lockfile.exists()
    assert (target.checkpoints / "001_init.sql").exists()

    lock_state = status(target)
    assert lock_state.is_clean is True


def test_apply_is_noop_when_tail_is_empty(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    migration_sql = "-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = "CREATE TABLE a(id INTEGER);\n"

    _write(target.migrations / "001_init.sql", migration_sql)
    _write(target.checkpoints / "001_init.sql", checkpoint_sql)
    _write(target.schema, checkpoint_sql)

    def _fake_replay(
        structural: schema_plan_mod.StructuralPlan, *, keep_scratch: bool, dbmate_bin: Path | None
    ) -> schema_replay_mod.ReplayOutcome:
        del keep_scratch, dbmate_bin
        checkpoint_map = (
            {"checkpoints/001_init.sql": checkpoint_sql} if structural.tail_steps else {}
        )
        return _check_outcome(schema_sql=checkpoint_sql, checkpoint_map=checkpoint_map)

    monkeypatch.setattr(schema_replay_mod, "run_replay_checks", _fake_replay)

    first = apply(target, test_base_url="sqlite3:/tmp/schema-idempotent-first.sqlite3")
    second = apply(target, test_base_url="sqlite3:/tmp/schema-idempotent-second.sqlite3")

    assert first.wrote is True
    assert second.wrote is False
    assert second.changed_files == ()
    assert Snapshot.from_worktree(target).schema_sql == checkpoint_sql.encode("utf-8")


def test_apply_tail_empty_divergence_rewrites_and_prunes(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)

    # First, materialize a 2-step artifact state.
    migration1 = "-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    migration2 = "-- migrate:up\nCREATE TABLE b(id INTEGER);\n"
    _write(target.migrations / "001_init.sql", migration1)
    _write(target.migrations / "002_next.sql", migration2)
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
            checkpoint_map={
                "checkpoints/001_init.sql": "CREATE TABLE a(id INTEGER);\n",
                "checkpoints/002_next.sql": "CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
            },
        ),
    )
    first = apply(target, test_base_url="sqlite3:/tmp/schema-tail-empty-initial.sqlite3")
    assert first.wrote is True
    assert (target.checkpoints / "002_next.sql").exists()

    # Now remove the second migration from worktree to force lock/worktree divergence
    # with an empty tail slice (lock has one trailing step not present in worktree).
    (target.migrations / "002_next.sql").unlink()
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={},
        ),
    )
    second = apply(target, test_base_url="sqlite3:/tmp/schema-tail-empty-prune.sqlite3")

    assert second.wrote is True
    assert "schema.sql" in second.changed_files
    assert "schema.lock.toml" in second.changed_files
    assert "checkpoints/002_next.sql" in second.changed_files
    assert not (target.checkpoints / "002_next.sql").exists()
    assert status(target).is_clean is True


def test_status_recovers_pending_tx(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    target = _target(tmp_path)
    calls: list[Path] = []
    lock_calls: list[Path] = []

    def _fake_recover(path: Path) -> None:
        calls.append(path)

    @contextmanager
    def _fake_serialized(path: Path):
        lock_calls.append(path)
        yield

    monkeypatch.setattr(schema_mod, "recover_artifacts", _fake_recover)
    monkeypatch.setattr(schema_mod, "serialized_target", _fake_serialized)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")

    _ = status(target)

    assert lock_calls == [target.dir]
    assert calls == [target.dir]


def test_plan_recovers_pending_tx(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    calls: list[Path] = []
    lock_calls: list[Path] = []

    def _fake_recover(path: Path) -> None:
        calls.append(path)

    @contextmanager
    def _fake_serialized(path: Path):
        lock_calls.append(path)
        yield

    monkeypatch.setattr(schema_mod, "recover_artifacts", _fake_recover)
    monkeypatch.setattr(schema_mod, "serialized_target", _fake_serialized)
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={},
        ),
    )
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")

    _ = plan(target, test_base_url="sqlite3:/tmp/schema-recover-plan.sqlite3")

    assert lock_calls == [target.dir]
    assert calls == [target.dir]


def test_apply_recovers_pending_tx(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    calls: list[Path] = []
    lock_calls: list[Path] = []

    def _fake_recover(path: Path) -> None:
        calls.append(path)

    @contextmanager
    def _fake_serialized(path: Path):
        lock_calls.append(path)
        yield

    monkeypatch.setattr(schema_mod, "recover_artifacts", _fake_recover)
    monkeypatch.setattr(schema_mod, "serialized_target", _fake_serialized)
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={"checkpoints/001_init.sql": "CREATE TABLE a(id INTEGER);\n"},
        ),
    )
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")

    _ = apply(target, test_base_url="sqlite3:/tmp/schema-recover-apply.sqlite3")

    assert lock_calls == [target.dir]
    assert calls == [target.dir]


def test_apply_commits_inside_target_serialization(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE a(id INTEGER);\n")
    monkeypatch.setattr(
        schema_replay_mod,
        "run_replay_checks",
        lambda *args, **kwargs: _check_outcome(
            schema_sql="CREATE TABLE a(id INTEGER);\n",
            checkpoint_map={"checkpoints/001_init.sql": "CREATE TABLE a(id INTEGER);\n"},
        ),
    )

    commit_calls: list[tuple[Path, int, int]] = []

    def _fake_commit(
        target_dir: Path,
        writes: dict[Path, bytes],
        deletes: tuple[Path, ...],
    ) -> tuple[Path, ...]:
        commit_calls.append((target_dir, len(writes), len(deletes)))
        return (target_dir / "schema.sql",)

    monkeypatch.setattr(schema_mod.artifacts, "commit_artifacts", _fake_commit)

    result = apply(target, test_base_url="sqlite3:/tmp/schema-commit-assume-locked.sqlite3")

    assert result.wrote is True
    assert commit_calls == [(target.dir, 3, 0)]


def test_resolve_replay_context_skips_invalid_url_candidate(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    monkeypatch.setenv(target.test_url_env, "sqlite3:/tmp/schema-env.sqlite3")
    monkeypatch.delenv(target.url_env, raising=False)

    engine, base_url = schema_plan_mod.resolve_replay_context(
        target=target,
        lock=None,
        explicit_test_base_url="not-a-valid-url",
    )

    assert engine is Engine.SQLITE
    assert base_url == "sqlite3:/tmp/schema-env.sqlite3"


def test_resolve_replay_context_uses_url_env_only_for_engine_inference(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    monkeypatch.delenv(target.test_url_env, raising=False)
    monkeypatch.setenv(target.url_env, "mysql://user:pass@127.0.0.1:3306/test_base")

    engine, base_url = schema_plan_mod.resolve_replay_context(
        target=target,
        lock=None,
        explicit_test_base_url=None,
    )

    assert engine is Engine.MYSQL
    assert base_url is None


def test_resolve_replay_context_rejects_bigquery_without_scratch_base(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    monkeypatch.delenv(target.test_url_env, raising=False)
    monkeypatch.setenv(target.url_env, "bigquery://example-project/us/live_ds")

    with pytest.raises(SchemaError, match="BigQuery scratch requires test_base_url"):
        schema_plan_mod.resolve_replay_context(
            target=target,
            lock=None,
            explicit_test_base_url=None,
        )


def test_resolve_replay_context_falls_back_to_lock_engine_on_invalid_urls(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    monkeypatch.setenv(target.test_url_env, "not-a-valid-url")
    monkeypatch.setenv(target.url_env, "also-invalid")
    policy = LockPolicy()
    lock = LockFile(
        lock_version=policy.lock_version,
        hash_algorithm=policy.hash_algorithm,
        canonicalizer=policy.canonicalizer,
        engine="sqlite",
        target=target.name,
        schema_file=policy.schema_file,
        migrations_dir=policy.migrations_dir,
        checkpoints_dir=policy.checkpoints_dir,
        head_index=0,
        head_chain_hash=policy.chain_seed(engine="sqlite", target=target.name),
        head_schema_digest="",
        steps=(),
    )

    engine, base_url = schema_plan_mod.resolve_replay_context(
        target=target,
        lock=lock,
        explicit_test_base_url=None,
    )

    assert engine is Engine.SQLITE
    assert base_url is None


def test_resolve_replay_context_rejects_invalid_lock_engine(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    monkeypatch.delenv(target.test_url_env, raising=False)
    monkeypatch.delenv(target.url_env, raising=False)
    policy = LockPolicy()
    lock = LockFile(
        lock_version=policy.lock_version,
        hash_algorithm=policy.hash_algorithm,
        canonicalizer=policy.canonicalizer,
        engine="bogus",
        target=target.name,
        schema_file=policy.schema_file,
        migrations_dir=policy.migrations_dir,
        checkpoints_dir=policy.checkpoints_dir,
        head_index=0,
        head_chain_hash=policy.chain_seed(engine="", target=target.name),
        head_schema_digest="",
        steps=(),
    )

    with pytest.raises(SchemaError, match="Invalid lockfile engine 'bogus'"):
        schema_plan_mod.resolve_replay_context(
            target=target,
            lock=lock,
            explicit_test_base_url=None,
        )


def test_run_down_roundtrip_uses_incremental_single_connection(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    step1 = schema_plan_mod.WorktreeStep(
        index=1,
        version="001",
        migration_file="migrations/001_first.sql",
        migration_digest="m1",
        checkpoint_file="checkpoints/001_first.sql",
        checkpoint_digest="c1",
        chain_hash="h1",
    )
    step2 = schema_plan_mod.WorktreeStep(
        index=2,
        version="002",
        migration_file="migrations/002_second.sql",
        migration_digest="m2",
        checkpoint_file="checkpoints/002_second.sql",
        checkpoint_digest="c2",
        chain_hash="h2",
    )
    structural = schema_plan_mod.StructuralPlan(
        target=target,
        policy=LockPolicy(),
        head_snapshot=Snapshot(
            target_name=target.name,
            schema_sql=None,
            lock_toml=None,
            migrations={
                step1.migration_file: (
                    b"-- migrate:up\nCREATE TABLE first(id INTEGER);\n"
                    b"-- migrate:down\nDROP TABLE first;\n"
                ),
                step2.migration_file: (
                    b"-- migrate:up\nCREATE TABLE second(id INTEGER);\n"
                    b"-- migrate:down\n-- irreversible\n"
                ),
            },
            checkpoints={},
        ),
        head_state=LockState(
            target_name=target.name,
            lock=None,
            worktree_steps=(step1, step2),
            schema_digest=None,
            orphan_checkpoints=(),
            diagnostics=(),
        ),
        divergence_index=1,
        anchor_index=0,
        tail_steps=(step1, step2),
        anchor_sql=None,
        engine=Engine.SQLITE,
        test_base_url="sqlite3:/tmp/schema-down.sqlite3",
    )

    class _FakeConn:
        def __init__(self) -> None:
            self.migrate_calls = 0
            self.rollback_calls: list[int] = []

        def migrate(self):
            self.migrate_calls += 1
            return _cmd("dbmate", "migrate")

        def rollback(self, steps: int):
            self.rollback_calls.append(steps)
            return _cmd("dbmate", "rollback", str(steps))

    conn = _FakeConn()

    @contextmanager
    def _fake_lease(**kwargs):
        del kwargs
        yield conn, "sqlite3:/tmp/schema-down.sqlite3"

    dumps = iter(["BASE0\n", "STEP1\n", "BASE0\n", "STEP2\n"])

    monkeypatch.setattr(schema_replay_mod, "lease_bootstrapped_connection", _fake_lease)
    monkeypatch.setattr(schema_replay_mod, "dump_schema", lambda *_args, **_kwargs: next(dumps))

    checkpoint_map, down_checked, down_skipped, scratch_url = schema_replay_mod.run_down_roundtrip(
        structural,
        keep_scratch=False,
        dbmate_bin=None,
    )

    assert conn.migrate_calls == 3
    assert conn.rollback_calls == [1]
    assert checkpoint_map == {
        "checkpoints/001_first.sql": "STEP1\n",
        "checkpoints/002_second.sql": "STEP2\n",
    }
    assert down_checked == ("migrations/001_first.sql",)
    assert down_skipped == ("migrations/002_second.sql",)
    assert scratch_url == "sqlite3:/tmp/schema-down.sqlite3"


def test_sanitize_mysql_anchor_sql_filters_replication_set_noise_only() -> None:
    anchor_sql = (
        "SET @@GLOBAL.GTID_PURGED = 'uuid:1-10';\n"
        "set @@session.sql_log_bin = 0;\n"
        "SET SQL_LOG_BIN=0;\n"
        "SET NAMES utf8mb4;\n"
        "CREATE TABLE t (id BIGINT);\n"
    )

    statements = sql_mod.SqlProgram(anchor_sql, engine="mysql").anchor_statements(
        target_url="mysql://user:pass@127.0.0.1:3306/target_db"
    )
    upper = ";\n".join(statements).upper()

    assert "GTID_PURGED" not in upper
    assert "SQL_LOG_BIN" not in upper
    assert "CREATE TABLE T (ID BIGINT)" in upper


def test_retarget_bigquery_statement_rewrites_dataset_qualifiers() -> None:
    statement = (
        "INSERT INTO `example-project:old_ds.schema_migrations` (version) "
        "VALUES ('001')"
    )
    rewritten = sql_mod.SqlProgram(statement, engine="bigquery").anchor_statements(
        target_url="bigquery://example-project/us/new_ds"
    )
    assert len(rewritten) == 1
    rewritten_sql = rewritten[0]
    assert "example-project.new_ds.schema_migrations" in rewritten_sql


def test_retarget_bigquery_statement_rewrites_only_write_target() -> None:
    statement = (
        "CREATE VIEW `example-project.old_ds.events_view` AS "
        "SELECT * FROM `example-project.analytics.users`"
    )
    rewritten = sql_mod.SqlProgram(statement, engine="bigquery").anchor_statements(
        target_url="bigquery://example-project/us/new_ds"
    )
    assert len(rewritten) == 1
    assert "example-project.new_ds.events_view" in rewritten[0]
    assert "example-project.analytics.users" in rewritten[0]


def test_retarget_bigquery_statement_skips_create_schema() -> None:
    statement = "CREATE SCHEMA IF NOT EXISTS `example-project.old_ds`"
    rewritten = sql_mod.SqlProgram(statement, engine="bigquery").anchor_statements(
        target_url="bigquery://example-project/us/new_ds"
    )
    assert rewritten == ()


def test_retarget_bigquery_statement_rejects_inconsistent_foreign_write_targets() -> None:
    program = sql_mod.SqlProgram(
        "CREATE TABLE `example-project.old_ds.events` (id INT64);\n"
        "INSERT INTO `example-project.foreign_ds.schema_migrations` (version) VALUES ('001');\n",
        engine="bigquery",
    )

    with pytest.raises(sql_mod.SqlError, match="multiple targets"):
        program.anchor_statements(target_url="bigquery://example-project/us/new_ds")


def test_lease_bootstrapped_connection_drops_explicit_server_scratch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    structural = schema_plan_mod.StructuralPlan(
        target=target,
        policy=LockPolicy(),
        head_snapshot=Snapshot(target_name=target.name, schema_sql=None, lock_toml=None, migrations={}, checkpoints={}),
        head_state=LockState(
            target_name=target.name,
            lock=None,
            worktree_steps=(),
            schema_digest=None,
            orphan_checkpoints=(),
            diagnostics=(),
        ),
        divergence_index=None,
        anchor_index=0,
        tail_steps=(),
        anchor_sql=None,
        engine=Engine.POSTGRES,
        test_base_url="postgres://user:pass@127.0.0.1:5432/base_db",
    )

    class _FakeConn:
        def __init__(self, url: str) -> None:
            self.url = url
            self.drop_calls = 0

        def drop(self) -> CmdResult:
            self.drop_calls += 1
            return _cmd("dbmate", "drop")

    fake_conn: _FakeConn | None = None

    class _FakeDbmate:
        def __init__(self, *, migrations_dir: Path, dbmate_bin: Path | None) -> None:
            del migrations_dir, dbmate_bin

        def database(self, url: str) -> _FakeConn:
            nonlocal fake_conn
            fake_conn = _FakeConn(url)
            return fake_conn

    @contextmanager
    def _fake_lease(self, **kwargs):
        del self, kwargs
        yield scratch_mod.ScratchLease(
            engine=Engine.POSTGRES,
            scratch_name="matey_core_schema",
            url="postgres://user:pass@127.0.0.1:5432/matey_core_schema",
            auto_provisioned=False,
        )

    monkeypatch.setattr(schema_replay_mod, "Dbmate", _FakeDbmate)
    monkeypatch.setattr(schema_replay_mod, "bootstrap_scratch", lambda **kwargs: None)
    monkeypatch.setattr(scratch_mod.Scratch, "lease", _fake_lease)

    with schema_replay_mod.lease_bootstrapped_connection(
        structural=structural,
        keep_scratch=False,
        dbmate_bin=None,
        migrations_dir=tmp_path / "migrations",
        scratch_label="schema_replay",
        context="replay",
    ) as (_conn, _url):
        pass

    assert fake_conn is not None
    assert fake_conn.drop_calls == 1


def test_lease_bootstrapped_connection_unlinks_explicit_sqlite_scratch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    target = _target(tmp_path)
    scratch_file = (tmp_path / "scratch.sqlite3").resolve()
    scratch_file.write_text("", encoding="utf-8")
    structural = schema_plan_mod.StructuralPlan(
        target=target,
        policy=LockPolicy(),
        head_snapshot=Snapshot(target_name=target.name, schema_sql=None, lock_toml=None, migrations={}, checkpoints={}),
        head_state=LockState(
            target_name=target.name,
            lock=None,
            worktree_steps=(),
            schema_digest=None,
            orphan_checkpoints=(),
            diagnostics=(),
        ),
        divergence_index=None,
        anchor_index=0,
        tail_steps=(),
        anchor_sql=None,
        engine=Engine.SQLITE,
        test_base_url=f"sqlite3:{tmp_path / 'base.sqlite3'}",
    )

    class _FakeConn:
        def __init__(self, url: str) -> None:
            self.url = url

    class _FakeDbmate:
        def __init__(self, *, migrations_dir: Path, dbmate_bin: Path | None) -> None:
            del migrations_dir, dbmate_bin

        def database(self, url: str) -> _FakeConn:
            return _FakeConn(url)

    @contextmanager
    def _fake_lease(self, **kwargs):
        del self, kwargs
        yield scratch_mod.ScratchLease(
            engine=Engine.SQLITE,
            scratch_name="matey_core_schema",
            url=f"sqlite3:{scratch_file}",
            auto_provisioned=False,
        )

    monkeypatch.setattr(schema_replay_mod, "Dbmate", _FakeDbmate)
    monkeypatch.setattr(schema_replay_mod, "bootstrap_scratch", lambda **kwargs: None)
    monkeypatch.setattr(scratch_mod.Scratch, "lease", _fake_lease)

    with schema_replay_mod.lease_bootstrapped_connection(
        structural=structural,
        keep_scratch=False,
        dbmate_bin=None,
        migrations_dir=tmp_path / "migrations",
        scratch_label="schema_replay",
        context="replay",
    ) as (_conn, _url):
        pass

    assert not scratch_file.exists()
