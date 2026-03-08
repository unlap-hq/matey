from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pygit2
import pytest

from matey.dbmate import default_dbmate_binary
from matey.lockfile import LockPolicy
from matey.project import TargetConfig
from matey.schema import SchemaError, apply, plan, status

pytestmark = pytest.mark.integration


@pytest.fixture(autouse=True)
def _require_dbmate_binary() -> None:
    dbmate_bin = default_dbmate_binary()
    if not dbmate_bin.exists() or not dbmate_bin.is_file():
        pytest.skip(f"Bundled dbmate binary not available: {dbmate_bin}")
    if not os.access(dbmate_bin, os.X_OK):
        pytest.skip(f"Bundled dbmate binary is not executable: {dbmate_bin}")


def _target(tmp_path: Path, name: str = "core") -> TargetConfig:
    return TargetConfig(
        name=name,
        root=(tmp_path / "db" / name).resolve(),
        url_env=f"{name.upper()}_DATABASE_URL",
        test_url_env=f"{name.upper()}_TEST_DATABASE_URL",
    )


def _sqlite_test_base_url(tmp_path: Path) -> str:
    return f"sqlite3:{(tmp_path / 'scratch-base.sqlite3').as_posix()}"


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


def _spawn_apply_worker(
    target: TargetConfig, test_base_url: str, output_path: Path
) -> subprocess.Popen[str]:
    code = (
        "import json, sys\n"
        "from pathlib import Path\n"
        "from matey.project import TargetConfig\n"
        "from matey.schema import apply\n"
        "target = TargetConfig(\n"
        "    name=sys.argv[1],\n"
        "    root=Path(sys.argv[2]),\n"
        "    url_env=sys.argv[3],\n"
        "    test_url_env=sys.argv[4],\n"
        ")\n"
        "result = apply(target, test_base_url=sys.argv[5])\n"
        "Path(sys.argv[6]).write_text(json.dumps({'wrote': result.wrote}), encoding='utf-8')\n"
    )
    project_root = Path(__file__).resolve().parents[2]
    src_path = str(project_root / "src")
    env = dict(os.environ)
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = src_path if not existing else f"{src_path}:{existing}"
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            code,
            target.name,
            str(target.root),
            target.url_env,
            target.test_url_env,
            test_base_url,
            str(output_path),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
    )


def test_live_plan_reports_down_checked_for_executable_down(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        """
-- migrate:up
CREATE TABLE widget(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE widget;
""".strip()
        + "\n",
    )

    result = plan(target, test_base_url=_sqlite_test_base_url(tmp_path))

    assert result.tail_count == 1
    assert result.down_checked == ("migrations/001_init.sql",)
    assert result.down_skipped == ()
    assert result.replay_scratch_url.startswith("sqlite3:")
    assert result.down_scratch_url is not None
    assert result.down_scratch_url.startswith("sqlite3:")


def test_live_plan_reports_down_skipped_for_non_executable_down(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        """
-- migrate:up
CREATE TABLE widget(id INTEGER PRIMARY KEY);
-- migrate:down
-- intentionally irreversible
-- no executable SQL
""".strip()
        + "\n",
    )

    result = plan(target, test_base_url=_sqlite_test_base_url(tmp_path))

    assert result.tail_count == 1
    assert result.down_checked == ()
    assert result.down_skipped == ("migrations/001_init.sql",)
    assert result.replay_scratch_url.startswith("sqlite3:")
    assert result.down_scratch_url is not None
    assert result.down_scratch_url.startswith("sqlite3:")


def test_live_apply_prunes_trailing_checkpoint_when_migration_removed(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        """
-- migrate:up
CREATE TABLE one(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE one;
""".strip()
        + "\n",
    )
    _write(
        target.migrations / "002_next.sql",
        """
-- migrate:up
CREATE TABLE two(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE two;
""".strip()
        + "\n",
    )

    first = apply(target, test_base_url=_sqlite_test_base_url(tmp_path))
    assert first.wrote is True
    assert first.replay_scratch_url.startswith("sqlite3:")
    assert first.down_scratch_url is not None
    assert first.down_scratch_url.startswith("sqlite3:")
    assert (target.checkpoints / "002_next.sql").exists()

    (target.migrations / "002_next.sql").unlink()

    second = apply(target, test_base_url=_sqlite_test_base_url(tmp_path))
    assert second.wrote is True
    assert "checkpoints/002_next.sql" in second.changed_files
    assert not (target.checkpoints / "002_next.sql").exists()
    assert status(target).is_clean is True


def test_live_plan_base_uses_merge_base_vs_worktree(tmp_path: Path) -> None:
    repo = pygit2.init_repository(str(tmp_path), initial_head="main")
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        """
-- migrate:up
CREATE TABLE one(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE one;
""".strip()
        + "\n",
    )
    apply(target, test_base_url=_sqlite_test_base_url(tmp_path))
    _commit_all(repo, "base")

    _write(
        target.migrations / "002_next.sql",
        """
-- migrate:up
CREATE TABLE two(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE two;
""".strip()
        + "\n",
    )

    result = plan(
        target,
        base_ref="refs/heads/main",
        test_base_url=_sqlite_test_base_url(tmp_path),
    )

    assert result.divergence_index == 2
    assert result.anchor_index == 1
    assert result.tail_count == 1
    assert result.down_checked == ("migrations/002_next.sql",)
    assert result.replay_scratch_url.startswith("sqlite3:")
    assert result.down_scratch_url is not None
    assert result.down_scratch_url.startswith("sqlite3:")


def test_live_down_roundtrip_mismatch_raises_schema_error(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_broken.sql",
        """
-- migrate:up
CREATE TABLE alpha(id INTEGER PRIMARY KEY);
-- migrate:down
CREATE TABLE beta(id INTEGER PRIMARY KEY);
""".strip()
        + "\n",
    )

    with pytest.raises(SchemaError, match="Down roundtrip mismatch"):
        plan(target, test_base_url=_sqlite_test_base_url(tmp_path))


def test_live_missing_anchor_checkpoint_raises_clear_error(tmp_path: Path) -> None:
    target = _target(tmp_path)
    policy = LockPolicy()

    migration1 = "-- migrate:up\nCREATE TABLE one(id INTEGER PRIMARY KEY);\n"
    migration2 = "-- migrate:up\nCREATE TABLE two(id INTEGER PRIMARY KEY);\n"
    checkpoint1 = "CREATE TABLE one(id INTEGER PRIMARY KEY);\n"
    _write(target.migrations / "001_init.sql", migration1)
    _write(target.migrations / "002_next.sql", migration2)
    _write(target.schema, checkpoint1)
    _write(
        target.lockfile,
        _single_step_lock(
            target=target.name,
            engine="sqlite",
            migration_file="migrations/001_init.sql",
            migration_digest=policy.digest(migration1.encode("utf-8")),
            checkpoint_file="checkpoints/001_init.sql",
            checkpoint_digest=policy.digest(checkpoint1.encode("utf-8")),
            schema_digest=policy.digest(checkpoint1.encode("utf-8")),
            policy=policy,
        ),
    )

    with pytest.raises(SchemaError, match="Missing anchor checkpoint"):
        plan(target, test_base_url=_sqlite_test_base_url(tmp_path))


def test_live_status_recovers_pending_tx_before_read(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(target.migrations / "001_init.sql", "-- migrate:up\nCREATE TABLE one(id INTEGER);\n")
    _write(target.schema, "CREATE TABLE stale(id INTEGER);\n")

    # Simulate interrupted apply journal.
    tx_root = target.root / ".matey" / "tx" / "manual-applying"
    tx_root.mkdir(parents=True, exist_ok=True)
    (tx_root / "state").write_text("applying\n", encoding="utf-8")
    (tx_root / "manifest.json").write_text(
        '{"version":1,"created_ns":1,"writes":["schema.sql"],"deletes":[]}',
        encoding="utf-8",
    )
    (tx_root / "backup").mkdir(parents=True, exist_ok=True)
    (tx_root / "backup" / "schema.sql").write_text(
        "CREATE TABLE recovered(id INTEGER);\n",
        encoding="utf-8",
    )

    state = status(target)

    assert state.target_name == target.name
    assert target.schema.read_text(encoding="utf-8") == "CREATE TABLE recovered(id INTEGER);\n"


def test_live_plan_recovers_pending_tx_before_lock_parse(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        """
-- migrate:up
CREATE TABLE one(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE one;
""".strip()
        + "\n",
    )
    _ = apply(target, test_base_url=_sqlite_test_base_url(tmp_path))
    good_lock = target.lockfile.read_text(encoding="utf-8")
    _write(target.lockfile, "not a lockfile\n")

    tx_root = target.root / ".matey" / "tx" / "manual-applying"
    tx_root.mkdir(parents=True, exist_ok=True)
    (tx_root / "state").write_text("applying\n", encoding="utf-8")
    (tx_root / "manifest.json").write_text(
        '{"version":1,"created_ns":1,"writes":["schema.lock.toml"],"deletes":[]}',
        encoding="utf-8",
    )
    (tx_root / "backup").mkdir(parents=True, exist_ok=True)
    (tx_root / "backup" / "schema.lock.toml").write_text(good_lock, encoding="utf-8")

    result = plan(target, test_base_url=_sqlite_test_base_url(tmp_path))

    assert result.tail_count == 0
    assert target.lockfile.read_text(encoding="utf-8") == good_lock
    assert not (target.root / ".matey" / "tx").exists()


def test_live_apply_recovers_pending_tx_before_lock_parse(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        """
-- migrate:up
CREATE TABLE one(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE one;
""".strip()
        + "\n",
    )
    _ = apply(target, test_base_url=_sqlite_test_base_url(tmp_path))
    good_lock = target.lockfile.read_text(encoding="utf-8")
    _write(target.lockfile, "not a lockfile\n")

    tx_root = target.root / ".matey" / "tx" / "manual-applying"
    tx_root.mkdir(parents=True, exist_ok=True)
    (tx_root / "state").write_text("applying\n", encoding="utf-8")
    (tx_root / "manifest.json").write_text(
        '{"version":1,"created_ns":1,"writes":["schema.lock.toml"],"deletes":[]}',
        encoding="utf-8",
    )
    (tx_root / "backup").mkdir(parents=True, exist_ok=True)
    (tx_root / "backup" / "schema.lock.toml").write_text(good_lock, encoding="utf-8")

    result = apply(target, test_base_url=_sqlite_test_base_url(tmp_path))

    assert result.wrote is False
    assert target.lockfile.read_text(encoding="utf-8") == good_lock
    assert not (target.root / ".matey" / "tx").exists()


def test_live_concurrent_apply_is_serialized(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.migrations / "001_init.sql",
        """
-- migrate:up
CREATE TABLE one(id INTEGER PRIMARY KEY);
-- migrate:down
DROP TABLE one;
""".strip()
        + "\n",
    )
    test_base_url = _sqlite_test_base_url(tmp_path)
    first_out = tmp_path / "apply-first.json"
    second_out = tmp_path / "apply-second.json"
    first = _spawn_apply_worker(target, test_base_url, first_out)
    second = _spawn_apply_worker(target, test_base_url, second_out)

    first_stdout, first_stderr = first.communicate(timeout=60)
    second_stdout, second_stderr = second.communicate(timeout=60)
    assert first.returncode == 0, f"first worker failed: {first_stderr or first_stdout}"
    assert second.returncode == 0, f"second worker failed: {second_stderr or second_stdout}"

    outcomes = []
    for output in (first_out, second_out):
        payload = json.loads(output.read_text(encoding="utf-8"))
        outcomes.append(bool(payload["wrote"]))
    assert outcomes.count(True) == 1
    assert outcomes.count(False) == 1
    assert status(target).is_clean is True
    assert not (target.root / ".matey" / "tx").exists()
