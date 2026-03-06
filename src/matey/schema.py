from __future__ import annotations

import os
import tempfile
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from matey.config import TargetConfig
from matey.dbmate import CmdResult, DbConnection, Dbmate
from matey.git_repo import GitRepo
from matey.lockfile import (
    DiagnosticCode,
    Divergence,
    LockFile,
    LockPolicy,
    LockState,
    LockStep,
    WorktreeStep,
    build_lock_state,
    divergence_between_states,
    generated_sql_digest,
    lock_worktree_divergence,
)
from matey.scratch import Engine, Scratch
from matey.snapshot import Snapshot
from matey.sql import (
    SqlProgram,
    WriteViolation,
    compare_schema_sql,
    engine_from_url,
    ensure_newline,
    schema_sql_diff,
)
from matey.tx import TxError, commit_artifacts, recover_artifacts, serialized_target

_HEAD_FATAL_DIAGNOSTICS = frozenset(
    {
        DiagnosticCode.LOCKFILE_PARSE_ERROR,
        DiagnosticCode.LOCKFILE_VERSION_MISMATCH,
        DiagnosticCode.LOCKFILE_HASH_ALGORITHM_MISMATCH,
        DiagnosticCode.LOCKFILE_CANONICALIZER_MISMATCH,
        DiagnosticCode.LOCKFILE_SCHEMA_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_MIGRATIONS_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_CHECKPOINTS_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_STEP_PATH_INVALID,
        DiagnosticCode.LOCKFILE_STEP_PATH_MISMATCH,
        DiagnosticCode.LOCKFILE_DUPLICATE_MIGRATION,
        DiagnosticCode.LOCKFILE_DUPLICATE_STEP_INDEX,
        DiagnosticCode.LOCKFILE_STEP_INDEX_INVALID,
        DiagnosticCode.INPUT_PATH_INVALID,
        DiagnosticCode.INPUT_PATH_DUPLICATE,
        DiagnosticCode.COHERENCE_TARGET_MISMATCH,
    }
)


class SchemaError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class PlanResult:
    target_name: str
    divergence_index: int | None
    anchor_index: int
    tail_count: int
    matches: bool
    replay_scratch_url: str
    down_scratch_url: str | None
    down_checked: tuple[str, ...]
    down_skipped: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ApplyResult:
    target_name: str
    wrote: bool
    changed_files: tuple[str, ...]
    replay_scratch_url: str
    down_scratch_url: str | None


@dataclass(frozen=True, slots=True)
class _StructuralPlan:
    target: TargetConfig
    policy: LockPolicy
    head_snapshot: Snapshot
    head_state: LockState
    divergence_index: int | None
    anchor_index: int
    tail_steps: tuple[WorktreeStep, ...]
    anchor_sql: str | None
    engine: Engine
    test_base_url: str | None


@dataclass(frozen=True, slots=True)
class _CheckOutcome:
    replay_scratch_url: str
    down_scratch_url: str | None
    b_schema_sql: str
    checkpoint_sql_by_file: Mapping[str, str]
    down_checked: tuple[str, ...]
    down_skipped: tuple[str, ...]


def status(target: TargetConfig, *, policy: LockPolicy | None = None) -> LockState:
    with serialized_target(target.dir):
        _recover_target_artifacts(target, context="status")
        snapshot = Snapshot.from_worktree(target)
        return build_lock_state(snapshot, policy=policy)


def plan(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> PlanResult:
    with serialized_target(target.dir):
        structural, checks = _prepare_plan(
            target=target,
            context="plan",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        matches = _schemas_match(
            _worktree_schema_sql(structural.head_snapshot),
            checks.b_schema_sql,
            engine=structural.engine,
            context_url=checks.replay_scratch_url,
        )
        return PlanResult(
            target_name=target.name,
            divergence_index=structural.divergence_index,
            anchor_index=structural.anchor_index,
            tail_count=len(structural.tail_steps),
            matches=matches,
            replay_scratch_url=checks.replay_scratch_url,
            down_scratch_url=checks.down_scratch_url,
            down_checked=checks.down_checked,
            down_skipped=checks.down_skipped,
        )


def plan_sql(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> str:
    with serialized_target(target.dir):
        _structural, checks = _prepare_plan(
            target=target,
            context="plan sql",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        return checks.b_schema_sql


def plan_diff(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> str:
    with serialized_target(target.dir):
        structural, checks = _prepare_plan(
            target=target,
            context="plan diff",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        return schema_sql_diff(
            _worktree_schema_sql(structural.head_snapshot),
            checks.b_schema_sql,
            engine=structural.engine.value,
            left_label="worktree/schema.sql",
            right_label="replay/schema.sql",
            left_context_url=checks.replay_scratch_url,
            right_context_url=checks.replay_scratch_url,
        )


def apply(
    target: TargetConfig,
    *,
    base_ref: str | None = None,
    clean: bool = False,
    test_base_url: str | None = None,
    keep_scratch: bool = False,
    dbmate_bin: Path | None = None,
    policy: LockPolicy | None = None,
) -> ApplyResult:
    with serialized_target(target.dir):
        structural, checks = _prepare_plan(
            target=target,
            context="apply",
            base_ref=base_ref,
            clean=clean,
            test_base_url=test_base_url,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            policy=policy,
        )
        desired_artifacts = _build_desired_artifacts(
            structural=structural,
            checks=checks,
        )
        writes, deletes = _compute_artifact_delta(
            target=structural.target,
            desired_artifacts=desired_artifacts,
        )
        if not writes and not deletes:
            return ApplyResult(
                target_name=target.name,
                wrote=False,
                changed_files=(),
                replay_scratch_url=checks.replay_scratch_url,
                down_scratch_url=checks.down_scratch_url,
            )

        changed_files = _apply_artifact_delta(
            target=structural.target,
            writes=writes,
            deletes=deletes,
        )
        return ApplyResult(
            target_name=target.name,
            wrote=True,
            changed_files=changed_files,
            replay_scratch_url=checks.replay_scratch_url,
            down_scratch_url=checks.down_scratch_url,
        )


def _prepare_plan(
    *,
    target: TargetConfig,
    context: str,
    base_ref: str | None,
    clean: bool,
    test_base_url: str | None,
    keep_scratch: bool,
    dbmate_bin: Path | None,
    policy: LockPolicy | None,
) -> tuple[_StructuralPlan, _CheckOutcome]:
    _recover_target_artifacts(target, context=context)
    structural = _build_structural_plan(
        target=target,
        base_ref=base_ref,
        clean=clean,
        test_base_url=test_base_url,
        policy=policy,
    )
    checks = _run_replay_checks(
        structural,
        keep_scratch=keep_scratch,
        dbmate_bin=dbmate_bin,
    )
    return structural, checks


def _recover_target_artifacts(target: TargetConfig, *, context: str) -> None:
    try:
        recover_artifacts(target.dir)
    except TxError as error:
        raise SchemaError(f"{context}: artifact recovery failed: {error}") from error


def _build_structural_plan(
    *,
    target: TargetConfig,
    base_ref: str | None,
    clean: bool,
    test_base_url: str | None,
    policy: LockPolicy | None,
) -> _StructuralPlan:
    if clean and base_ref is not None:
        raise SchemaError("Cannot combine clean=True with base_ref.")

    effective_policy = policy or LockPolicy()
    head_snapshot = Snapshot.from_worktree(target)
    head_state = build_lock_state(head_snapshot, policy=effective_policy)
    _require_head_state_usable(head_state)

    base_snapshot: Snapshot | None = None
    base_state: LockState | None = None
    divergence: Divergence | None

    if clean:
        divergence = (
            Divergence(index=1, field="clean", base_value="clean=false", head_value="clean=true")
            if head_state.worktree_steps
            else None
        )
    elif base_ref is None:
        divergence = lock_worktree_divergence(head_state)
    else:
        git_repo = GitRepo.open(target.dir)
        merge_base = git_repo.resolve_merge_base(base_ref)
        target_rel_dir = target.dir.resolve().relative_to(git_repo.repo_root).as_posix()
        base_snapshot = Snapshot.from_tree(
            target_name=target.name,
            target_rel_dir=target_rel_dir,
            root_tree=git_repo.tree_for(merge_base.merge_base_oid),
        )
        base_state = build_lock_state(base_snapshot, policy=effective_policy)
        _require_clean_state(base_state, label=f"base snapshot ({base_ref})")
        divergence = divergence_between_states(base_state, head_state)

    if divergence is None:
        anchor_index = len(head_state.worktree_steps)
        divergence_index = None
        tail_steps: tuple[WorktreeStep, ...] = ()
    else:
        anchor_index = divergence.index - 1
        divergence_index = divergence.index
        tail_steps = head_state.worktree_steps[anchor_index:]

    anchor_sql: str | None = None
    if anchor_index > 0:
        anchor_sql = _select_anchor_sql(
            anchor_index=anchor_index,
            head_snapshot=head_snapshot,
            head_steps=head_state.worktree_steps,
            base_snapshot=base_snapshot,
            base_steps=base_state.worktree_steps if base_state is not None else (),
            use_base=base_ref is not None and not clean and base_state is not None,
        )

    engine, resolved_test_base_url = _resolve_replay_context(
        target=target,
        lock=head_state.lock,
        explicit_test_base_url=test_base_url,
    )
    return _StructuralPlan(
        target=target,
        policy=effective_policy,
        head_snapshot=head_snapshot,
        head_state=head_state,
        divergence_index=divergence_index,
        anchor_index=anchor_index,
        tail_steps=tail_steps,
        anchor_sql=anchor_sql,
        engine=engine,
        test_base_url=resolved_test_base_url,
    )


def _select_anchor_sql(
    *,
    anchor_index: int,
    head_snapshot: Snapshot,
    head_steps: tuple[WorktreeStep, ...],
    base_snapshot: Snapshot | None,
    base_steps: tuple[WorktreeStep, ...],
    use_base: bool,
) -> str:
    if use_base:
        if base_snapshot is None:
            raise SchemaError("Base snapshot is required for base-aware anchoring.")
        if len(base_steps) < anchor_index:
            raise SchemaError(
                f"Base snapshot has {len(base_steps)} steps; cannot anchor at index {anchor_index}."
            )
        anchor_step = base_steps[anchor_index - 1]
        anchor_bytes = base_snapshot.checkpoints.get(anchor_step.checkpoint_file)
        source_label = "base"
    else:
        anchor_step = head_steps[anchor_index - 1]
        anchor_bytes = head_snapshot.checkpoints.get(anchor_step.checkpoint_file)
        source_label = "head"

    if anchor_bytes is None:
        raise SchemaError(
            f"Missing anchor checkpoint {anchor_step.checkpoint_file!r} in {source_label} snapshot."
        )
    return anchor_bytes.decode("utf-8")


def _resolve_replay_context(
    *,
    target: TargetConfig,
    lock: LockFile | None,
    explicit_test_base_url: str | None,
) -> tuple[Engine, str | None]:
    test_base_from_arg = _normalized_optional(explicit_test_base_url)
    test_base_from_env = _normalized_optional(os.getenv(target.test_url_env))
    url_from_env = _normalized_optional(os.getenv(target.url_env))
    lock_engine = Engine(lock.engine) if lock is not None else None

    inferred_engine: Engine | None = None
    resolved_test_base_url: str | None = None
    invalid_candidates: list[str] = []
    for source, candidate in (
        ("test_base_url", test_base_from_arg),
        (target.test_url_env, test_base_from_env),
        (target.url_env, url_from_env),
    ):
        if candidate is None:
            continue
        try:
            inferred_engine = _engine_from_url(candidate)
        except SchemaError as error:
            invalid_candidates.append(f"{source}: {error}")
            continue
        resolved_test_base_url = candidate
        break

    if inferred_engine is None:
        inferred_engine = lock_engine

    if inferred_engine is None:
        details = (
            f" Invalid URL values: {'; '.join(invalid_candidates)}." if invalid_candidates else ""
        )
        raise SchemaError(
            "Unable to infer replay engine. Provide test_base_url, set test_url_env/url_env, or add schema.lock.toml."
            + details
        )

    if lock_engine is not None and inferred_engine is not lock_engine:
        raise SchemaError(
            f"Replay engine mismatch: inferred {inferred_engine.value!r} from URL, "
            f"but lockfile engine is {lock_engine.value!r}."
        )

    return inferred_engine, resolved_test_base_url


def _engine_from_url(url: str) -> Engine:
    match engine_from_url(url):
        case "postgres" | "postgresql":
            return Engine.POSTGRES
        case "mysql":
            return Engine.MYSQL
        case "sqlite":
            return Engine.SQLITE
        case "clickhouse":
            return Engine.CLICKHOUSE
        case "bigquery":
            return Engine.BIGQUERY
        case _:
            raise SchemaError(f"Unsupported URL scheme for engine inference: {url!r}.")


def _normalized_optional(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip()
    return normalized or None


def _require_clean_state(state: LockState, *, label: str) -> None:
    if state.is_clean:
        return
    details = "; ".join(
        f"{diag.code.value}@{diag.path}: {diag.detail}" for diag in state.diagnostics
    )
    raise SchemaError(f"{label} lock state is not clean: {details}")


def _require_head_state_usable(state: LockState) -> None:
    fatal = _head_state_errors(state)
    if not fatal:
        return
    details = "; ".join(f"{code}@{path}: {detail}" for code, path, detail in fatal)
    raise SchemaError(f"head worktree lock state is invalid: {details}")


def _head_state_errors(state: LockState) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (diag.code.value, diag.path, diag.detail)
        for diag in state.diagnostics
        if diag.code in _HEAD_FATAL_DIAGNOSTICS
    )


def _run_replay_checks(
    structural: _StructuralPlan,
    *,
    keep_scratch: bool,
    dbmate_bin: Path | None,
) -> _CheckOutcome:
    _validate_tail_migration_targets(structural)
    with tempfile.TemporaryDirectory(prefix="matey-schema-tail-") as temp_root:
        tail_dir = _write_tail_slice(structural, Path(temp_root))
        with _lease_bootstrapped_connection(
            structural=structural,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            migrations_dir=tail_dir,
            scratch_label="schema_replay",
            context="replay",
        ) as (replay_conn, replay_scratch_url):
            if structural.tail_steps:
                _require_ok(
                    replay_conn.migrate(),
                    context="replay tail migrations",
                )
            b_schema_sql = _dump_schema(replay_conn, context="replay")

        checkpoint_sql_by_file, down_checked, down_skipped, down_scratch_url = _run_down_roundtrip(
            structural,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
        )
        return _CheckOutcome(
            replay_scratch_url=replay_scratch_url,
            down_scratch_url=down_scratch_url,
            b_schema_sql=b_schema_sql,
            checkpoint_sql_by_file=checkpoint_sql_by_file,
            down_checked=down_checked,
            down_skipped=down_skipped,
        )


def _run_down_roundtrip(
    structural: _StructuralPlan,
    *,
    keep_scratch: bool,
    dbmate_bin: Path | None,
) -> tuple[dict[str, str], tuple[str, ...], tuple[str, ...], str | None]:
    if not structural.tail_steps:
        return {}, (), (), None

    down_checked: list[str] = []
    down_skipped: list[str] = []
    checkpoint_sql_by_file: dict[str, str] = {}
    with (
        tempfile.TemporaryDirectory(prefix="matey-schema-down-bootstrap-") as bootstrap_dir,
        _lease_bootstrapped_connection(
            structural=structural,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            migrations_dir=Path(bootstrap_dir),
            scratch_label="schema_down",
            context="down-roundtrip",
        ) as (_, down_scratch_url),
    ):
        for index, step in enumerate(structural.tail_steps, start=1):
            with tempfile.TemporaryDirectory(prefix="matey-schema-down-step-") as temp_root:
                step_dir = _write_tail_slice(
                    structural,
                    Path(temp_root),
                    step_count=index,
                )
                step_dbmate = Dbmate(migrations_dir=step_dir, dbmate_bin=dbmate_bin)
                step_conn = step_dbmate.database(down_scratch_url)

                program = SqlProgram(
                    _migration_sql_text(structural, step),
                    engine=structural.engine.value,
                )
                has_down = program.has_executable_down()
                baseline: str | None = None
                if has_down:
                    baseline = _dump_schema(
                        step_conn, context=f"down baseline {step.migration_file}"
                    )

                _require_ok(
                    step_conn.migrate(),
                    context=f"down-roundtrip migrate {step.migration_file}",
                )
                checkpoint_sql_by_file[step.checkpoint_file] = _dump_schema(
                    step_conn,
                    context=f"checkpoint capture {step.migration_file}",
                )

                if not has_down:
                    down_skipped.append(step.migration_file)
                    continue

                _require_ok(
                    step_conn.rollback(1),
                    context=f"down-roundtrip rollback {step.migration_file}",
                )
                after_rollback = _dump_schema(
                    step_conn,
                    context=f"down rollback dump {step.migration_file}",
                )
                if baseline is None:
                    raise SchemaError(
                        f"Down roundtrip mismatch for {step.migration_file}: baseline was not captured."
                    )
                if not compare_schema_sql(
                    baseline,
                    after_rollback,
                    engine=structural.engine.value,
                    left_context_url=down_scratch_url,
                    right_context_url=down_scratch_url,
                ):
                    raise SchemaError(
                        f"Down roundtrip mismatch for {step.migration_file}: rollback state differs from baseline."
                    )

                _require_ok(
                    step_conn.migrate(),
                    context=f"down-roundtrip reapply {step.migration_file}",
                )
                down_checked.append(step.migration_file)

    return checkpoint_sql_by_file, tuple(down_checked), tuple(down_skipped), down_scratch_url


@contextmanager
def _lease_bootstrapped_connection(
    *,
    structural: _StructuralPlan,
    keep_scratch: bool,
    dbmate_bin: Path | None,
    migrations_dir: Path,
    scratch_label: str,
    context: str,
):
    scratch = Scratch()
    scratch_name = f"matey_{structural.target.name}_{scratch_label}_{uuid.uuid4().hex[:8]}"
    with scratch.lease(
        engine=structural.engine,
        scratch_name=scratch_name,
        test_base_url=structural.test_base_url,
        keep=keep_scratch,
    ) as lease:
        dbmate = Dbmate(migrations_dir=migrations_dir, dbmate_bin=dbmate_bin)
        conn = dbmate.database(lease.url)
        _bootstrap_scratch(
            conn=conn,
            engine=structural.engine,
            anchor_sql=structural.anchor_sql,
            context=context,
        )
        yield conn, lease.url


def _bootstrap_scratch(
    *,
    conn: DbConnection,
    engine: Engine,
    anchor_sql: str | None,
    context: str,
) -> None:
    if engine in {Engine.POSTGRES, Engine.MYSQL, Engine.CLICKHOUSE}:
        _require_ok(conn.wait(60), context=f"{context} wait")
    _require_ok(conn.create(), context=f"{context} create")
    if anchor_sql is not None:
        program = SqlProgram(anchor_sql, engine=engine.value)
        try:
            statements = program.anchor_statements(target_url=conn.url)
        except ValueError as error:
            raise SchemaError(f"{context} load anchor failed: {error}") from error
        for index, statement in enumerate(statements, start=1):
            _require_ok(
                conn.load(ensure_newline(f"{statement};")),
                context=f"{context} load anchor statement {index}",
            )


def _write_tail_slice(
    structural: _StructuralPlan, root: Path, *, step_count: int | None = None
) -> Path:
    if step_count is not None and step_count <= 0:
        raise ValueError("step_count must be greater than zero.")
    selected = structural.tail_steps if step_count is None else structural.tail_steps[:step_count]
    migrations_dir = root / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    for step in selected:
        payload = _migration_payload(structural, step)
        rel = Path(step.migration_file).relative_to(structural.policy.migrations_dir)
        output_path = migrations_dir / rel
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(payload)
    return migrations_dir


def _migration_sql_text(structural: _StructuralPlan, step: WorktreeStep) -> str:
    return _migration_payload(structural, step).decode("utf-8")


def _migration_payload(structural: _StructuralPlan, step: WorktreeStep) -> bytes:
    payload = structural.head_snapshot.migrations.get(step.migration_file)
    if payload is None:
        raise SchemaError(f"Missing migration payload for {step.migration_file}.")
    return payload


def _dump_schema(conn: DbConnection, *, context: str) -> str:
    result = conn.dump()
    _require_ok(result, context=f"{context} dump")
    return ensure_newline(result.stdout)


def _require_ok(result: CmdResult, *, context: str) -> None:
    if result.exit_code == 0:
        return
    raise SchemaError(
        f"{context} failed (exit_code={result.exit_code}): "
        f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
    )


def _validate_tail_migration_targets(structural: _StructuralPlan) -> None:
    engine = structural.engine.value
    if engine not in {"bigquery", "mysql", "clickhouse"}:
        return
    for step in structural.tail_steps:
        program = SqlProgram(
            _migration_sql_text(structural, step),
            engine=engine,
        )
        _raise_on_write_violations(
            step.migration_file,
            engine=engine,
            violations=program.migration_write_violations(),
        )


def _worktree_schema_sql(snapshot: Snapshot) -> str:
    payload = snapshot.schema_sql
    return payload.decode("utf-8") if payload is not None else ""


def _schemas_match(
    left_sql: str,
    right_sql: str,
    *,
    engine: Engine,
    context_url: str | None,
) -> bool:
    return compare_schema_sql(
        left_sql,
        right_sql,
        engine=engine.value,
        left_context_url=context_url,
        right_context_url=context_url,
    )


def _raise_on_write_violations(
    migration_file: str,
    *,
    engine: str,
    violations: tuple[WriteViolation, ...],
) -> None:
    if not violations:
        return
    violation = violations[0]
    if violation.reason == "qualified write target":
        reason = f"qualified {engine} write target"
    else:
        reason = f"unsupported {engine} mutating syntax"
    raise SchemaError(
        f"{migration_file} {violation.section} contains a {reason} "
        f"{violation.target!r}. Use unqualified target-local names or split this into "
        f"another matey target. Statement: {violation.excerpt()!r}"
    )


def _build_desired_artifacts(
    *,
    structural: _StructuralPlan,
    checks: _CheckOutcome,
) -> dict[Path, bytes]:
    target = structural.target
    head_steps = structural.head_state.worktree_steps

    checkpoint_texts = _collect_checkpoint_texts(
        structural=structural,
        checks=checks,
    )
    schema_sql = ensure_newline(checks.b_schema_sql)
    lock_sql = _build_lock_toml(
        policy=structural.policy,
        target=target,
        engine=structural.engine,
        steps=head_steps,
        checkpoint_texts=checkpoint_texts,
        schema_sql=schema_sql,
    )

    artifacts: dict[Path, bytes] = {
        target.schema: schema_sql.encode("utf-8"),
        target.lockfile: lock_sql.encode("utf-8"),
    }
    for step in head_steps:
        checkpoint_sql = checkpoint_texts.get(step.checkpoint_file)
        if checkpoint_sql is None:
            raise SchemaError(f"Missing checkpoint SQL for {step.checkpoint_file}.")
        checkpoint_path = target.dir / step.checkpoint_file
        artifacts[checkpoint_path] = ensure_newline(checkpoint_sql).encode("utf-8")
    return artifacts


def _compute_artifact_delta(
    *,
    target: TargetConfig,
    desired_artifacts: Mapping[Path, bytes],
) -> tuple[dict[Path, bytes], tuple[Path, ...]]:
    writes: dict[Path, bytes] = {}
    for path, payload in desired_artifacts.items():
        if path.exists():
            if not path.is_file():
                raise SchemaError(f"Cannot write artifact to non-file path: {path}")
            if path.read_bytes() == payload:
                continue
        writes[path] = payload

    desired_checkpoints = {
        path.resolve()
        for path in desired_artifacts
        if path.resolve().is_relative_to(target.checkpoints.resolve())
    }
    existing_checkpoints: set[Path] = set()
    if target.checkpoints.exists():
        for path in target.checkpoints.rglob("*.sql"):
            if path.is_file():
                existing_checkpoints.add(path.resolve())

    deletes = tuple(
        sorted(
            (path for path in existing_checkpoints if path not in desired_checkpoints),
            key=lambda path: path.as_posix(),
        )
    )
    return writes, deletes


def _apply_artifact_delta(
    *,
    target: TargetConfig,
    writes: Mapping[Path, bytes],
    deletes: tuple[Path, ...],
) -> tuple[str, ...]:
    try:
        changed_paths = commit_artifacts(target.dir, writes=writes, deletes=deletes)
    except TxError as error:
        raise SchemaError(f"apply: artifact commit failed: {error}") from error
    return tuple(sorted(_relative_target_path(path, target) for path in changed_paths))


def _collect_checkpoint_texts(
    *,
    structural: _StructuralPlan,
    checks: _CheckOutcome,
) -> dict[str, str]:
    checkpoints: dict[str, str] = {}

    for step in structural.head_state.worktree_steps[: structural.anchor_index]:
        payload = structural.head_snapshot.checkpoints.get(step.checkpoint_file)
        if payload is None:
            raise SchemaError(f"Missing unchanged checkpoint {step.checkpoint_file}.")
        checkpoints[step.checkpoint_file] = payload.decode("utf-8")

    for step in structural.tail_steps:
        checkpoint_sql = checks.checkpoint_sql_by_file.get(step.checkpoint_file)
        if checkpoint_sql is None:
            raise SchemaError(f"Missing replay checkpoint for {step.checkpoint_file}.")
        checkpoints[step.checkpoint_file] = checkpoint_sql
    return checkpoints


def _build_lock_toml(
    *,
    policy: LockPolicy,
    target: TargetConfig,
    engine: Engine,
    steps: tuple[WorktreeStep, ...],
    checkpoint_texts: Mapping[str, str],
    schema_sql: str,
) -> str:
    chain = policy.chain_seed(engine=engine.value, target=target.name)
    lock_steps: list[LockStep] = []
    for index, step in enumerate(steps, start=1):
        checkpoint_sql = checkpoint_texts.get(step.checkpoint_file)
        if checkpoint_sql is None:
            raise SchemaError(f"Missing checkpoint SQL for lock step {step.checkpoint_file}.")
        checkpoint_digest = generated_sql_digest(checkpoint_sql, policy=policy)
        if checkpoint_digest is None:
            raise SchemaError(f"Unable to digest checkpoint SQL for {step.checkpoint_file}.")
        chain = policy.chain_step(
            previous=chain,
            version=step.version,
            migration_file=step.migration_file,
            migration_digest=step.migration_digest,
        )
        lock_steps.append(
            LockStep(
                index=index,
                version=step.version,
                migration_file=step.migration_file,
                migration_digest=step.migration_digest,
                checkpoint_file=step.checkpoint_file,
                checkpoint_digest=checkpoint_digest,
                schema_digest=checkpoint_digest,
                chain_hash=chain,
            )
        )

    head_schema_digest = generated_sql_digest(schema_sql, policy=policy)
    if head_schema_digest is None:
        raise SchemaError("Unable to digest schema.sql for lockfile output.")

    lock = LockFile(
        lock_version=policy.lock_version,
        hash_algorithm=policy.hash_algorithm,
        canonicalizer=policy.canonicalizer,
        engine=engine.value,
        target=target.name,
        schema_file=policy.schema_file,
        migrations_dir=policy.migrations_dir,
        checkpoints_dir=policy.checkpoints_dir,
        head_index=len(lock_steps),
        head_chain_hash=chain,
        head_schema_digest=head_schema_digest,
        steps=tuple(lock_steps),
    )
    return lock.to_toml()


def _relative_target_path(path: Path, target: TargetConfig) -> str:
    try:
        return path.resolve().relative_to(target.dir.resolve()).as_posix()
    except ValueError:
        return path.resolve().as_posix()


__all__ = [
    "ApplyResult",
    "PlanResult",
    "SchemaError",
    "apply",
    "plan",
    "plan_diff",
    "plan_sql",
    "status",
]
