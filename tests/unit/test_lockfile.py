from __future__ import annotations

from matey.lockfile import (
    DiagnosticCode,
    LockPolicy,
    build_lock_state,
    divergence_between_states,
    first_lock_divergence,
    generated_sql_digest,
)
from matey.snapshot import Snapshot


def _lock_toml(
    *,
    policy: LockPolicy,
    target: str,
    engine: str,
    head_schema_digest: str,
    steps: str,
    head_index: int,
    head_chain_hash: str,
) -> bytes:
    text = f"""
lock_version = {policy.lock_version}
hash_algorithm = "{policy.hash_algorithm}"
canonicalizer = "{policy.canonicalizer}"
engine = "{engine}"
target = "{target}"
schema_file = "{policy.schema_file}"
migrations_dir = "{policy.migrations_dir}"
checkpoints_dir = "{policy.checkpoints_dir}"
head_index = {head_index}
head_chain_hash = "{head_chain_hash}"
head_schema_digest = "{head_schema_digest}"

{steps}
"""
    return text.strip().encode("utf-8") + b"\n"


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
) -> bytes:
    chain_seed = policy.chain_seed(engine=engine, target=target)
    chain = policy.chain_step(
        previous=chain_seed,
        version=step_version,
        migration_file=migration_file,
        migration_digest=migration_digest,
    )
    steps = f"""
[[steps]]
index = 1
version = "{step_version}"
migration_file = "{migration_file}"
migration_digest = "{migration_digest}"
checkpoint_file = "{checkpoint_file}"
checkpoint_digest = "{checkpoint_digest}"
schema_digest = "{schema_digest}"
chain_hash = "{chain}"
"""
    return _lock_toml(
        policy=policy,
        target=target,
        engine=engine,
        head_schema_digest=schema_digest,
        steps=steps,
        head_index=1,
        head_chain_hash=chain,
    )


def test_build_lock_state_clean_for_coherent_input() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = b"CREATE TABLE a(id INTEGER);\n"
    schema_sql = checkpoint_sql
    migration_file = "migrations/001_init.sql"
    checkpoint_file = "checkpoints/001_init.sql"

    lock_toml = _single_step_lock(
        target="core",
        engine="sqlite",
        migration_file=migration_file,
        migration_digest=policy.digest(migration_sql),
        checkpoint_file=checkpoint_file,
        checkpoint_digest=policy.digest(checkpoint_sql),
        schema_digest=policy.digest(schema_sql),
        policy=policy,
    )

    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=schema_sql,
            lock_toml=lock_toml,
            migrations={migration_file: migration_sql},
            checkpoints={checkpoint_file: checkpoint_sql},
        ),
        policy=policy,
    )

    assert state.is_clean is True
    assert state.diagnostics == ()
    assert len(state.worktree_steps) == 1


def test_build_lock_state_emits_structural_diagnostics() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = b"CREATE TABLE a(id INTEGER);\n"

    lock_toml = _single_step_lock(
        target="wrong-target",
        engine="sqlite",
        migration_file="migrations/001_init.sql",
        migration_digest=policy.digest(migration_sql),
        checkpoint_file="checkpoints/001_init.sql",
        checkpoint_digest=policy.digest(checkpoint_sql),
        schema_digest=policy.digest(checkpoint_sql),
        policy=policy,
    )

    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=None,
            lock_toml=lock_toml,
            migrations={"migrations/001_init.sql": migration_sql},
            checkpoints={
                "checkpoints/001_init.sql": checkpoint_sql,
                "checkpoints/999_orphan.sql": b"-- orphan\n",
            },
        ),
        policy=policy,
    )

    codes = {row.code for row in state.diagnostics}
    assert state.is_clean is False
    assert DiagnosticCode.COHERENCE_TARGET_MISMATCH in codes
    assert DiagnosticCode.INPUT_SCHEMA_MISSING in codes
    assert DiagnosticCode.INPUT_ORPHAN_CHECKPOINT in codes


def test_build_lock_state_reports_lock_parse_error() -> None:
    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"",
            lock_toml=b"not = valid = toml",
            migrations={},
            checkpoints={},
        )
    )

    assert state.is_clean is False
    assert state.diagnostics[0].code is DiagnosticCode.LOCKFILE_PARSE_ERROR


def test_build_lock_state_handles_invalid_snapshot_paths_as_diagnostics() -> None:
    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"",
            lock_toml=None,
            migrations={"../migrations/001_init.sql": b"-- migrate:up\nSELECT 1;\n"},
            checkpoints={},
        )
    )

    codes = {row.code for row in state.diagnostics}
    assert state.is_clean is False
    assert DiagnosticCode.INPUT_PATH_INVALID in codes


def test_build_lock_state_reports_lock_step_mismatches() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = b"CREATE TABLE a(id INTEGER);\n"
    checkpoint_digest = policy.digest(checkpoint_sql)
    wrong_step_schema_digest = policy.digest(b"CREATE TABLE not_a(id INTEGER);\n")
    chain_seed = policy.chain_seed(engine="sqlite", target="core")
    chain = policy.chain_step(
        previous=chain_seed,
        version="999",
        migration_file="migrations/001_init.sql",
        migration_digest=policy.digest(migration_sql),
    )
    lock_toml = _lock_toml(
        policy=policy,
        target="core",
        engine="sqlite",
        head_schema_digest=checkpoint_digest,
        head_index=1,
        head_chain_hash=chain,
        steps=f"""
[[steps]]
index = 1
version = "999"
migration_file = "migrations/001_init.sql"
migration_digest = "{policy.digest(migration_sql)}"
checkpoint_file = "checkpoints/001_init.sql"
checkpoint_digest = "{checkpoint_digest}"
schema_digest = "{wrong_step_schema_digest}"
chain_hash = "{chain}"
""".strip(),
    )

    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=checkpoint_sql,
            lock_toml=lock_toml,
            migrations={"migrations/001_init.sql": migration_sql},
            checkpoints={"checkpoints/001_init.sql": checkpoint_sql},
        ),
        policy=policy,
    )

    codes = {row.code for row in state.diagnostics}
    assert DiagnosticCode.COHERENCE_STEP_VERSION_MISMATCH in codes
    assert DiagnosticCode.COHERENCE_STEP_SCHEMA_MISMATCH in codes


def test_first_lock_divergence_detects_digest_and_count() -> None:
    base = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"",
            lock_toml=None,
            migrations={"migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"},
            checkpoints={},
        )
    )
    changed = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"",
            lock_toml=None,
            migrations={"migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id TEXT);\n"},
            checkpoints={},
        )
    )
    extended = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"",
            lock_toml=None,
            migrations={
                "migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n",
                "migrations/002_next.sql": b"-- migrate:up\nCREATE TABLE b(id INTEGER);\n",
            },
            checkpoints={},
        )
    )

    digest_divergence = first_lock_divergence(base, changed)
    count_divergence = first_lock_divergence(base, extended)

    assert digest_divergence is not None
    assert digest_divergence.index == 1
    assert digest_divergence.field == "migration_digest"
    assert count_divergence is not None
    assert count_divergence.index == 2
    assert count_divergence.field == "step_count"


def test_first_lock_divergence_ignores_lock_only_chain_seed_difference() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = b"CREATE TABLE a(id INTEGER);\n"
    migration_file = "migrations/001_init.sql"
    checkpoint_file = "checkpoints/001_init.sql"

    unlocked = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=checkpoint_sql,
            lock_toml=None,
            migrations={migration_file: migration_sql},
            checkpoints={checkpoint_file: checkpoint_sql},
        ),
        policy=policy,
    )
    locked = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=checkpoint_sql,
            lock_toml=_single_step_lock(
                target="core",
                engine="sqlite",
                migration_file=migration_file,
                migration_digest=policy.digest(migration_sql),
                checkpoint_file=checkpoint_file,
                checkpoint_digest=generated_sql_digest(checkpoint_sql, policy=policy) or "",
                schema_digest=generated_sql_digest(checkpoint_sql, policy=policy) or "",
                policy=policy,
            ),
            migrations={migration_file: migration_sql},
            checkpoints={checkpoint_file: checkpoint_sql},
        ),
        policy=policy,
    )

    assert first_lock_divergence(unlocked, locked) is None


def test_divergence_between_states_allows_non_clean_inputs() -> None:
    base = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=None,
            lock_toml=None,
            migrations={"migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"},
            checkpoints={},
        )
    )
    head = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=None,
            lock_toml=None,
            migrations={
                "migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n",
                "migrations/002_next.sql": b"-- migrate:up\nCREATE TABLE b(id INTEGER);\n",
            },
            checkpoints={},
        )
    )

    divergence = divergence_between_states(base, head)

    assert divergence is not None
    assert divergence.index == 2
    assert divergence.field == "step_count"


def test_build_lock_state_is_deterministic_for_mapping_order() -> None:
    migrations_a = {
        "migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n",
        "migrations/002_next.sql": b"-- migrate:up\nCREATE TABLE b(id INTEGER);\n",
    }
    migrations_b = {
        "migrations/002_next.sql": b"-- migrate:up\nCREATE TABLE b(id INTEGER);\n",
        "migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n",
    }
    checkpoints_a = {
        "checkpoints/002_next.sql": b"CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
        "checkpoints/001_init.sql": b"CREATE TABLE a(id INTEGER);\n",
    }
    checkpoints_b = {
        "checkpoints/001_init.sql": b"CREATE TABLE a(id INTEGER);\n",
        "checkpoints/002_next.sql": b"CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
    }

    state_a = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
            lock_toml=None,
            migrations=migrations_a,
            checkpoints=checkpoints_a,
        )
    )
    state_b = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"CREATE TABLE a(id INTEGER);\nCREATE TABLE b(id INTEGER);\n",
            lock_toml=None,
            migrations=migrations_b,
            checkpoints=checkpoints_b,
        )
    )

    assert state_a.worktree_steps == state_b.worktree_steps
    assert state_a.diagnostics == state_b.diagnostics


def test_build_lock_state_preserves_nested_checkpoint_mapping() -> None:
    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"",
            lock_toml=None,
            migrations={
                "migrations/a/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n",
                "migrations/b/001_init.sql": b"-- migrate:up\nCREATE TABLE b(id INTEGER);\n",
            },
            checkpoints={
                "checkpoints/a/001_init.sql": b"CREATE TABLE a(id INTEGER);\n",
                "checkpoints/b/001_init.sql": b"CREATE TABLE b(id INTEGER);\n",
            },
        )
    )

    checkpoint_files = {step.checkpoint_file for step in state.worktree_steps}
    assert checkpoint_files == {"checkpoints/a/001_init.sql", "checkpoints/b/001_init.sql"}
    assert DiagnosticCode.INPUT_ORPHAN_CHECKPOINT not in {diag.code for diag in state.diagnostics}


def test_build_lock_state_orphan_is_blocking_and_not_clean() -> None:
    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"CREATE TABLE a(id INTEGER);\n",
            lock_toml=None,
            migrations={"migrations/001_init.sql": b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"},
            checkpoints={
                "checkpoints/001_init.sql": b"CREATE TABLE a(id INTEGER);\n",
                "checkpoints/999_orphan.sql": b"-- orphan\n",
            },
        )
    )

    assert state.is_clean is False
    assert len(state.diagnostics) == 1
    assert state.diagnostics[0].code is DiagnosticCode.INPUT_ORPHAN_CHECKPOINT


def test_build_lock_state_invalid_lock_step_still_runs_alignment_diagnostics() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = b"CREATE TABLE a(id INTEGER);\n"
    chain_seed = policy.chain_seed(engine="sqlite", target="core")
    chain_hash = policy.chain_step(
        previous=chain_seed,
        version="001",
        migration_file="../001_init.sql",
        migration_digest=policy.digest(migration_sql),
    )
    bad_lock = f"""
lock_version = {policy.lock_version}
hash_algorithm = "{policy.hash_algorithm}"
canonicalizer = "{policy.canonicalizer}"
engine = "sqlite"
target = "core"
schema_file = "{policy.schema_file}"
migrations_dir = "{policy.migrations_dir}"
checkpoints_dir = "{policy.checkpoints_dir}"
head_index = 1
head_chain_hash = "{chain_hash}"
head_schema_digest = "{policy.digest(checkpoint_sql)}"

[[steps]]
index = 1
version = "001"
migration_file = "../001_init.sql"
migration_digest = "{policy.digest(migration_sql)}"
checkpoint_file = "../001_init.sql"
checkpoint_digest = "{policy.digest(checkpoint_sql)}"
schema_digest = "{policy.digest(checkpoint_sql)}"
chain_hash = "{chain_hash}"
""".strip().encode("utf-8")

    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=checkpoint_sql,
            lock_toml=bad_lock,
            migrations={"migrations/001_init.sql": migration_sql},
            checkpoints={"checkpoints/001_init.sql": checkpoint_sql},
        ),
        policy=policy,
    )
    codes = [diag.code for diag in state.diagnostics]

    assert DiagnosticCode.LOCKFILE_STEP_PATH_INVALID in codes
    assert DiagnosticCode.LOCKFILE_STEP_PATH_MISMATCH not in codes
    assert DiagnosticCode.COHERENCE_NEW_IN_INPUT in codes
    assert DiagnosticCode.COHERENCE_HEAD_INDEX_MISMATCH in codes


def test_build_lock_state_reports_alignment_even_with_orphan_checkpoint() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = b"CREATE TABLE a(id INTEGER);\n"
    checkpoint_digest = policy.digest(checkpoint_sql)
    chain_seed = policy.chain_seed(engine="sqlite", target="core")
    chain = policy.chain_step(
        previous=chain_seed,
        version="999",
        migration_file="migrations/001_init.sql",
        migration_digest=policy.digest(migration_sql),
    )
    lock_toml = _lock_toml(
        policy=policy,
        target="core",
        engine="sqlite",
        head_schema_digest=checkpoint_digest,
        head_index=1,
        head_chain_hash=chain,
        steps=f"""
[[steps]]
index = 1
version = "999"
migration_file = "migrations/001_init.sql"
migration_digest = "{policy.digest(migration_sql)}"
checkpoint_file = "checkpoints/001_init.sql"
checkpoint_digest = "{checkpoint_digest}"
schema_digest = "{checkpoint_digest}"
chain_hash = "{chain}"
""".strip(),
    )

    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=checkpoint_sql,
            lock_toml=lock_toml,
            migrations={"migrations/001_init.sql": migration_sql},
            checkpoints={
                "checkpoints/001_init.sql": checkpoint_sql,
                "checkpoints/999_orphan.sql": b"-- orphan\n",
            },
        ),
        policy=policy,
    )

    codes = {diag.code for diag in state.diagnostics}
    assert DiagnosticCode.INPUT_ORPHAN_CHECKPOINT in codes
    assert DiagnosticCode.COHERENCE_STEP_VERSION_MISMATCH in codes


def test_build_lock_state_emits_single_schema_mismatch_signal() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_sql = b"CREATE TABLE a(id INTEGER);\n"
    lock_toml = _single_step_lock(
        target="core",
        engine="sqlite",
        migration_file="migrations/001_init.sql",
        migration_digest=policy.digest(migration_sql),
        checkpoint_file="checkpoints/001_init.sql",
        checkpoint_digest=policy.digest(checkpoint_sql),
        schema_digest=policy.digest(checkpoint_sql),
        policy=policy,
    )
    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"CREATE TABLE a(id TEXT);\n",
            lock_toml=lock_toml,
            migrations={"migrations/001_init.sql": migration_sql},
            checkpoints={"checkpoints/001_init.sql": checkpoint_sql},
        ),
        policy=policy,
    )

    codes = [diag.code for diag in state.diagnostics]
    assert codes.count(DiagnosticCode.COHERENCE_SCHEMA_DIGEST_MISMATCH) == 1


def test_build_lock_state_ignores_trailing_newline_only_generated_sql_difference() -> None:
    policy = LockPolicy()
    migration_sql = b"-- migrate:up\nCREATE TABLE a(id INTEGER);\n"
    checkpoint_with_newline = b"CREATE TABLE a(id INTEGER);\n"
    checkpoint_without_newline = b"CREATE TABLE a(id INTEGER);"
    lock_toml = _single_step_lock(
        target="core",
        engine="sqlite",
        migration_file="migrations/001_init.sql",
        migration_digest=policy.digest(migration_sql),
        checkpoint_file="checkpoints/001_init.sql",
        checkpoint_digest=generated_sql_digest(checkpoint_with_newline, policy=policy) or "",
        schema_digest=generated_sql_digest(checkpoint_with_newline, policy=policy) or "",
        policy=policy,
    )

    state = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=checkpoint_without_newline,
            lock_toml=lock_toml,
            migrations={"migrations/001_init.sql": migration_sql},
            checkpoints={"checkpoints/001_init.sql": checkpoint_without_newline},
        ),
        policy=policy,
    )

    assert state.is_clean is True


def test_first_lock_divergence_requires_clean_states() -> None:
    clean = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=b"",
            lock_toml=None,
            migrations={},
            checkpoints={},
        )
    )
    invalid = build_lock_state(
        Snapshot(
            target_name="core",
            schema_sql=None,
            lock_toml=b"not = valid = toml",
            migrations={},
            checkpoints={},
        )
    )

    try:
        first_lock_divergence(clean, invalid)
        raise AssertionError("Expected ValueError for non-clean lock states.")
    except ValueError:
        pass
