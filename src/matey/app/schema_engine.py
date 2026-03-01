from __future__ import annotations

import tempfile
import uuid
from dataclasses import dataclass
from pathlib import Path

from matey.app.config_engine import TargetRuntime
from matey.app.protocols import (
    ArtifactDelete,
    ArtifactWrite,
    ScratchHandle,
    cmd_result_to_output,
    require_cmd_success,
)
from matey.app.runtime import AppContext
from matey.domain.dbmate_output import extract_dump_sql
from matey.domain.errors import (
    CheckpointIntegrityError,
    EngineInferenceError,
    ReplayError,
)
from matey.domain.lockfile import (
    LockStep,
    SchemaLock,
    digest_bytes_blake2b256,
    first_divergence_against_lock,
    first_lock_mismatch,
    load_lock_from_text,
    lock_chain_seed,
    lock_chain_step,
    recompute_lock_chains,
)
from matey.domain.migration import (
    MigrationFile,
    parse_down_section_state,
    parse_migration_files,
)
from matey.domain.model import (
    ConfigDefaults,
    Engine,
    SchemaOpContext,
    SchemaPlanResult,
    SchemaStatusResult,
    SchemaStatusRow,
    SqlComparison,
    SqlSource,
    derive_target_key,
)
from matey.infra.engine_policy import detect_engine_from_url
from matey.infra.runtime_io import normalized_optional

_BASE_FALLBACK_ENV_KEYS = (
    "GITHUB_BASE_REF",
    "CI_MERGE_REQUEST_TARGET_BRANCH_NAME",
    "BUILDKITE_PULL_REQUEST_BASE_BRANCH",
)


@dataclass(frozen=True)
class _MigrationState:
    migration: MigrationFile
    migration_digest: str
    checkpoint_rel: str
    checkpoint_path: Path
    checkpoint_exists: bool
    checkpoint_digest: str | None


@dataclass(frozen=True)
class _GitMigrationState:
    migration: MigrationFile
    migration_digest: str
    checkpoint_rel: str
    checkpoint_bytes: bytes


@dataclass(frozen=True)
class _ReplayPlan:
    divergence: int
    anchor_sql: str | None
    tail_states: tuple[_MigrationState, ...]
    base_states: tuple[_GitMigrationState, ...]
    orphan_checkpoints: tuple[str, ...]


@dataclass(frozen=True)
class _PlanComputation:
    op: SchemaOpContext
    replay_plan: _ReplayPlan
    comparison: SqlComparison
    normalized_b_sql: str
    scratch_url: str


def _workspace_coherence_steps(states: tuple[_MigrationState, ...]) -> tuple[LockStep, ...]:
    return tuple(
        LockStep(
            index=i,
            version=state.migration.version,
            migration_file=state.migration.rel_path,
            migration_digest=state.migration_digest,
            checkpoint_file=state.checkpoint_rel,
            checkpoint_digest=state.checkpoint_digest or "",
            schema_digest="",
            chain_hash="",
        )
        for i, state in enumerate(states, start=1)
    )


def _base_coherence_steps(states: tuple[_GitMigrationState, ...]) -> tuple[LockStep, ...]:
    return tuple(
        LockStep(
            index=i,
            version=state.migration.version,
            migration_file=state.migration.rel_path,
            migration_digest=state.migration_digest,
            checkpoint_file=state.checkpoint_rel,
            checkpoint_digest=digest_bytes_blake2b256(state.checkpoint_bytes),
            schema_digest="",
            chain_hash="",
        )
        for i, state in enumerate(states, start=1)
    )


def _down_state(path: Path):
    return parse_down_section_state(path.read_text(encoding="utf-8"))


class SchemaEngine:
    def __init__(self, *, context: AppContext) -> None:
        self._ctx = context

    def schema_status(
        self,
        *,
        runtime: TargetRuntime,
        defaults: ConfigDefaults,
        base_ref: str | None,
    ) -> SchemaStatusResult:
        repo_root = self._ctx.git.repo_root()
        target_key = derive_target_key(repo_root=repo_root, db_dir=runtime.paths.db_dir)
        with self._ctx.scope.open(target_key=target_key, target_root=runtime.paths.db_dir):
            del base_ref, defaults
            return self._evaluate_schema_status(runtime=runtime)

    def schema_plan(
        self,
        *,
        runtime: TargetRuntime,
        defaults: ConfigDefaults,
        base_ref: str | None,
        clean: bool,
        keep_scratch: bool,
        url_override: str | None,
        test_url_override: str | None,
    ) -> SchemaPlanResult:
        op = self._build_schema_op_context(
            runtime=runtime,
            defaults=defaults,
            base_ref=base_ref,
            clean=clean,
            keep_scratch=keep_scratch,
            url_override=url_override,
            test_url_override=test_url_override,
        )
        with self._ctx.scope.open(target_key=op.target_key, target_root=op.target_paths.db_dir):
            computation = self._run_plan(op=op)
            return SchemaPlanResult(
                comparison=computation.comparison,
                replay_scratch_url=computation.scratch_url,
                down_checked=True,
                orphan_checkpoints=computation.replay_plan.orphan_checkpoints,
            )

    def schema_plan_sql(
        self,
        *,
        runtime: TargetRuntime,
        defaults: ConfigDefaults,
        base_ref: str | None,
        clean: bool,
        keep_scratch: bool,
        url_override: str | None,
        test_url_override: str | None,
    ) -> str:
        op = self._build_schema_op_context(
            runtime=runtime,
            defaults=defaults,
            base_ref=base_ref,
            clean=clean,
            keep_scratch=keep_scratch,
            url_override=url_override,
            test_url_override=test_url_override,
        )
        with self._ctx.scope.open(target_key=op.target_key, target_root=op.target_paths.db_dir):
            computation = self._run_plan(op=op)
            return computation.normalized_b_sql

    def schema_apply(
        self,
        *,
        runtime: TargetRuntime,
        defaults: ConfigDefaults,
        base_ref: str | None,
        clean: bool,
        keep_scratch: bool,
        url_override: str | None,
        test_url_override: str | None,
    ) -> None:
        op = self._build_schema_op_context(
            runtime=runtime,
            defaults=defaults,
            base_ref=base_ref,
            clean=clean,
            keep_scratch=keep_scratch,
            url_override=url_override,
            test_url_override=test_url_override,
        )
        with self._ctx.scope.open(target_key=op.target_key, target_root=op.target_paths.db_dir):
            computation = self._run_plan(op=op)
            checkpoint_texts = self._build_checkpoint_texts(op=op, replay_plan=computation.replay_plan)
            self._apply_artifacts(
                op=op,
                normalized_schema_sql=computation.normalized_b_sql,
                checkpoint_texts=checkpoint_texts,
                orphan_checkpoints=computation.replay_plan.orphan_checkpoints,
            )

    def _build_schema_op_context(
        self,
        *,
        runtime: TargetRuntime,
        defaults: ConfigDefaults,
        base_ref: str | None,
        clean: bool,
        keep_scratch: bool,
        url_override: str | None,
        test_url_override: str | None,
    ) -> SchemaOpContext:
        repo_root = self._ctx.git.repo_root()
        target_key = derive_target_key(repo_root=repo_root, db_dir=runtime.paths.db_dir)
        replay_engine, test_url = self._resolve_schema_engine(
            runtime=runtime,
            url_override=url_override,
            test_url_override=test_url_override,
        )
        del defaults
        resolved_base = base_ref
        return SchemaOpContext(
            target_id=runtime.target_id,
            target_key=target_key,
            target_paths=runtime.paths,
            repo_root=repo_root,
            base_ref=resolved_base,
            replay_engine=replay_engine,
            test_url=test_url,
            clean=clean,
            keep_scratch=keep_scratch,
            run_nonce=uuid.uuid4().hex[:10],
        )

    def _resolve_schema_engine(
        self,
        *,
        runtime: TargetRuntime,
        url_override: str | None,
        test_url_override: str | None,
    ) -> tuple[Engine, str | None]:
        chosen_test_url = normalized_optional(test_url_override)
        chosen_url = normalized_optional(url_override)
        env_test = normalized_optional(self._ctx.env.get(runtime.test_url_env))
        env_url = normalized_optional(self._ctx.env.get(runtime.url_env))

        resolved: str | None = chosen_test_url or chosen_url or env_test or env_url
        lock_engine: Engine | None = None
        if runtime.paths.lock_file.exists():
            lock = load_lock_from_text(runtime.paths.lock_file.read_text(encoding="utf-8"))
            lock_engine = Engine(lock.engine)

        if resolved is None and lock_engine is None:
            raise EngineInferenceError(
                "Unable to infer replay engine; provide --test-url/--url or configure env URLs/lockfile engine."
            )

        if resolved is None:
            assert lock_engine is not None
            return lock_engine, chosen_test_url or env_test

        resolved_engine = detect_engine_from_url(resolved)
        if lock_engine is not None and lock_engine != resolved_engine:
            raise EngineInferenceError(
                f"URL engine {resolved_engine.value} does not match lock engine {lock_engine.value}."
            )
        return resolved_engine, chosen_test_url or env_test

    def _evaluate_schema_status(self, *, runtime: TargetRuntime) -> SchemaStatusResult:
        rows: list[SchemaStatusRow] = []

        migrations = self._workspace_migrations(runtime.paths)
        deterministic = {
            migration.migration.rel_path: migration for migration in migrations
        }

        lock: SchemaLock | None = None
        expected_chains: tuple[str, ...] = ()
        if runtime.paths.lock_file.exists():
            lock = load_lock_from_text(runtime.paths.lock_file.read_text(encoding="utf-8"))
            expected_chains = recompute_lock_chains(
                steps=_workspace_coherence_steps(migrations),
                engine=Engine(lock.engine),
                target_key=derive_target_key(
                    repo_root=self._ctx.git.repo_root(),
                    db_dir=runtime.paths.db_dir,
                ),
            )

        lock_step_by_file = {step.migration_file: step for step in (lock.steps if lock else ())}

        for migration in migrations:
            step = lock_step_by_file.get(migration.migration.rel_path)
            if step is None:
                rows.append(
                    SchemaStatusRow(
                        marker="warn",
                        migration_file=migration.migration.rel_path,
                        status="new-in-worktree",
                        detail="Migration exists in workspace but not in lockfile.",
                    )
                )
                continue

            if step.migration_digest != migration.migration_digest:
                rows.append(
                    SchemaStatusRow(
                        marker="warn",
                        migration_file=migration.migration.rel_path,
                        status="migration-changed",
                        detail="Migration digest differs from lock step.",
                    )
                )

            if not migration.checkpoint_exists:
                rows.append(
                    SchemaStatusRow(
                        marker="warn",
                        migration_file=migration.migration.rel_path,
                        status="checkpoint-missing",
                        detail=f"Expected checkpoint missing: {migration.checkpoint_rel}",
                    )
                )
            elif migration.checkpoint_digest != step.checkpoint_digest:
                rows.append(
                    SchemaStatusRow(
                        marker="warn",
                        migration_file=migration.migration.rel_path,
                        status="checkpoint-changed",
                        detail="Checkpoint digest differs from lock step.",
                    )
                )

            out_of_range = step.index <= 0 or step.index > len(expected_chains)
            mismatch = out_of_range or expected_chains[step.index - 1] != step.chain_hash
            if mismatch:
                rows.append(
                    SchemaStatusRow(
                        marker="warn",
                        migration_file=migration.migration.rel_path,
                        status="chain-mismatch",
                        detail="Chain hash differs from deterministic recomputation.",
                    )
                )

        rows.extend(
            SchemaStatusRow(
                marker="warn",
                migration_file=step.migration_file,
                status="removed-from-worktree",
                detail="Lock contains migration that is missing in workspace.",
            )
            for step in (lock.steps if lock else ())
            if step.migration_file not in deterministic
        )

        orphan_checkpoints = self._find_orphan_checkpoints(runtime.paths, migrations)
        rows.extend(
            SchemaStatusRow(
                marker="warn",
                migration_file=orphan,
                status="orphan-checkpoint",
                detail="Checkpoint does not map to any workspace migration.",
            )
            for orphan in orphan_checkpoints
        )

        if lock is not None and runtime.paths.schema_file.exists():
            schema_prepared = self._ctx.sql_pipeline.prepare(
                engine=Engine(lock.engine),
                source=SqlSource(
                    text=runtime.paths.schema_file.read_text(encoding="utf-8"),
                    origin="artifact",
                ),
            )
            if schema_prepared.digest != lock.head_schema_digest:
                rows.append(
                    SchemaStatusRow(
                        marker="warn",
                        migration_file=runtime.paths.schema_file.relative_to(runtime.paths.db_dir).as_posix(),
                        status="schema-digest-mismatch",
                        detail="schema.sql digest differs from lock head_schema_digest.",
                    )
                )

        up_to_date = lock is not None and not rows
        stale = not up_to_date
        summary: list[str] = []
        summary.append(f"target={runtime.target_id.name}")
        summary.append(f"migrations={len(migrations)}")
        summary.append(f"rows={len(rows)}")
        summary.append("state=up-to-date" if up_to_date else "state=stale")

        return SchemaStatusResult(
            up_to_date=up_to_date,
            stale=stale,
            rows=tuple(rows),
            summary=tuple(summary),
        )

    def _workspace_migrations(self, paths) -> tuple[_MigrationState, ...]:
        file_paths: list[str] = []
        if paths.migrations_dir.exists():
            file_paths.extend(
                (Path("migrations") / item.name).as_posix()
                for item in sorted(paths.migrations_dir.iterdir(), key=lambda p: p.name)
                if item.is_file()
            )
        migrations = parse_migration_files(file_paths)
        states: list[_MigrationState] = []
        for migration in migrations:
            migration_path = paths.db_dir / migration.rel_path
            digest = digest_bytes_blake2b256(migration_path.read_bytes())
            checkpoint_rel = (Path("checkpoints") / f"{Path(migration.filename).stem}.sql").as_posix()
            checkpoint_path = paths.db_dir / checkpoint_rel
            checkpoint_exists = checkpoint_path.exists()
            checkpoint_digest = (
                digest_bytes_blake2b256(checkpoint_path.read_bytes()) if checkpoint_exists else None
            )
            states.append(
                _MigrationState(
                    migration=migration,
                    migration_digest=digest,
                    checkpoint_rel=checkpoint_rel,
                    checkpoint_path=checkpoint_path,
                    checkpoint_exists=checkpoint_exists,
                    checkpoint_digest=checkpoint_digest,
                )
            )
        return tuple(states)

    def _find_orphan_checkpoints(
        self,
        paths,
        migrations: tuple[_MigrationState, ...],
    ) -> tuple[str, ...]:
        expected = {m.checkpoint_rel for m in migrations}
        orphan: list[str] = []
        if paths.checkpoints_dir.exists():
            for item in sorted(paths.checkpoints_dir.iterdir(), key=lambda p: p.name):
                if not item.is_file():
                    continue
                rel = (Path("checkpoints") / item.name).as_posix()
                if rel not in expected:
                    orphan.append(rel)
        return tuple(orphan)

    def _run_plan(self, *, op: SchemaOpContext) -> _PlanComputation:
        replay_plan = self._build_replay_plan(op=op)
        comparison, normalized_b_sql, scratch_url = self._execute_replay(
            op=op,
            replay_plan=replay_plan,
        )
        self._execute_down_roundtrip(op=op, replay_plan=replay_plan)
        return _PlanComputation(
            op=op,
            replay_plan=replay_plan,
            comparison=comparison,
            normalized_b_sql=normalized_b_sql,
            scratch_url=scratch_url,
        )

    def _build_replay_plan(self, *, op: SchemaOpContext) -> _ReplayPlan:
        head_migrations = self._workspace_migrations(op.target_paths)
        orphan_checkpoints = self._find_orphan_checkpoints(op.target_paths, head_migrations)

        if op.clean:
            return _ReplayPlan(
                divergence=1,
                anchor_sql=None,
                tail_states=head_migrations,
                base_states=(),
                orphan_checkpoints=orphan_checkpoints,
            )

        requested_base = self._requested_base_ref(op=op)
        if requested_base is None:
            divergence, anchor, tail = self._build_local_divergence(
                op=op,
                head_migrations=head_migrations,
            )
            return _ReplayPlan(
                divergence=divergence,
                anchor_sql=anchor,
                tail_states=tail,
                base_states=(),
                orphan_checkpoints=orphan_checkpoints,
            )

        merge_base = self._ctx.git.merge_base("HEAD", requested_base)
        base_states, base_has_schema = self._load_base_migrations(op=op, merge_base=merge_base)
        base_lock = self._load_base_lock(op=op, merge_base=merge_base)
        if not base_states and not base_has_schema:
            divergence = 1
            anchor = None
            tail = head_migrations
        elif not base_states and base_has_schema:
            raise ReplayError("Invalid base state: schema file exists without migrations.")
        else:
            if base_lock is None:
                raise ReplayError("Base snapshot is missing schema.lock.toml.")
            self._require_base_lock_coherence(
                op=op,
                base_lock=base_lock,
                base_states=base_states,
            )
            self._require_head_lock_coherence(op=op, head_migrations=head_migrations)
            divergence = first_divergence_against_lock(
                lock=base_lock,
                steps=_workspace_coherence_steps(head_migrations),
                engine=Engine(base_lock.engine),
                target_key=op.target_key,
            )
            if divergence <= 1:
                anchor = None
            else:
                anchor = base_states[divergence - 2].checkpoint_bytes.decode("utf-8")
            tail = tuple(head_migrations[divergence - 1 :])

        return _ReplayPlan(
            divergence=divergence,
            anchor_sql=anchor,
            tail_states=tail,
            base_states=base_states,
            orphan_checkpoints=orphan_checkpoints,
        )

    def _requested_base_ref(self, *, op: SchemaOpContext) -> str | None:
        if op.base_ref and op.base_ref.strip():
            return op.base_ref.strip()

        for key in _BASE_FALLBACK_ENV_KEYS:
            value = normalized_optional(self._ctx.env.get(key))
            if value:
                return value

        return None

    def _build_local_divergence(
        self,
        *,
        op: SchemaOpContext,
        head_migrations: tuple[_MigrationState, ...],
    ) -> tuple[int, str | None, tuple[_MigrationState, ...]]:
        if not op.target_paths.lock_file.exists():
            return 1, None, head_migrations

        lock = load_lock_from_text(op.target_paths.lock_file.read_text(encoding="utf-8"))
        divergence = first_divergence_against_lock(
            lock=lock,
            steps=_workspace_coherence_steps(head_migrations),
            engine=Engine(lock.engine),
            target_key=op.target_key,
        )
        if divergence <= 1:
            anchor_sql = None
        else:
            anchor_state = head_migrations[divergence - 2]
            if not anchor_state.checkpoint_exists:
                raise ReplayError(
                    f"Missing deterministic checkpoint for anchor migration {anchor_state.migration.rel_path}: "
                    f"{anchor_state.checkpoint_rel}"
                )
            anchor_sql = anchor_state.checkpoint_path.read_text(encoding="utf-8")
        tail = tuple(head_migrations[divergence - 1 :])
        return divergence, anchor_sql, tail

    def _load_base_migrations(
        self,
        *,
        op: SchemaOpContext,
        merge_base: str,
    ) -> tuple[tuple[_GitMigrationState, ...], bool]:
        db_rel = op.target_paths.db_dir.resolve().relative_to(op.repo_root.resolve())
        migrations_rel_dir = db_rel / "migrations"
        checkpoints_rel_dir = db_rel / "checkpoints"
        schema_rel = db_rel / "schema.sql"

        migration_paths = self._ctx.git.list_tree_paths(merge_base, migrations_rel_dir)
        checkpoint_paths = self._ctx.git.list_tree_paths(merge_base, checkpoints_rel_dir)
        schema_blob = self._ctx.git.read_blob_bytes(merge_base, schema_rel)

        if not migration_paths and checkpoint_paths:
            raise ReplayError("Invalid base state: checkpoints exist without migrations.")

        if not migration_paths:
            return (), schema_blob is not None

        migration_rel = [
            path.relative_to(db_rel).as_posix() for path in migration_paths if path.suffix == ".sql"
        ]

        migrations = parse_migration_files(migration_rel)
        checkpoint_set = {path.relative_to(db_rel).as_posix() for path in checkpoint_paths}

        states: list[_GitMigrationState] = []
        for migration in migrations:
            migration_blob = self._ctx.git.read_blob_bytes(merge_base, db_rel / migration.rel_path)
            if migration_blob is None:
                raise ReplayError(f"Missing base migration blob: {migration.rel_path}")
            checkpoint_rel = (Path("checkpoints") / f"{Path(migration.filename).stem}.sql").as_posix()
            if checkpoint_rel not in checkpoint_set:
                raise ReplayError(
                    f"Missing deterministic base checkpoint for migration {migration.rel_path}: {checkpoint_rel}"
                )
            checkpoint_blob = self._ctx.git.read_blob_bytes(merge_base, db_rel / checkpoint_rel)
            if checkpoint_blob is None:
                raise ReplayError(f"Missing base checkpoint blob: {checkpoint_rel}")
            states.append(
                _GitMigrationState(
                    migration=migration,
                    migration_digest=digest_bytes_blake2b256(migration_blob),
                    checkpoint_rel=checkpoint_rel,
                    checkpoint_bytes=checkpoint_blob,
                )
            )

        expected_checkpoint_names = {state.checkpoint_rel for state in states}
        extras = sorted(checkpoint_set - expected_checkpoint_names)
        if extras:
            raise ReplayError(f"Base checkpoints contain unmapped files: {', '.join(extras)}")

        return tuple(states), schema_blob is not None

    def _load_base_lock(self, *, op: SchemaOpContext, merge_base: str) -> SchemaLock | None:
        db_rel = op.target_paths.db_dir.resolve().relative_to(op.repo_root.resolve())
        lock_blob = self._ctx.git.read_blob_bytes(merge_base, db_rel / "schema.lock.toml")
        if lock_blob is None:
            return None
        return load_lock_from_text(lock_blob.decode("utf-8"))

    def _require_base_lock_coherence(
        self,
        *,
        op: SchemaOpContext,
        base_lock: SchemaLock,
        base_states: tuple[_GitMigrationState, ...],
    ) -> None:
        if base_lock.engine != op.replay_engine.value:
            raise ReplayError(
                f"Base lock engine mismatch: expected {op.replay_engine.value}, got {base_lock.engine}."
            )
        if base_lock.target != op.target_id.name:
            raise ReplayError(
                f"Base lock target mismatch: expected {op.target_id.name}, got {base_lock.target}."
            )
        if len(base_lock.steps) != len(base_states):
            raise ReplayError(
                "Base lock step count does not match base migration set."
            )
        coherence_steps = _base_coherence_steps(base_states)
        mismatch = first_lock_mismatch(
            lock=base_lock,
            steps=coherence_steps,
            engine=Engine(base_lock.engine),
            target_key=op.target_key,
            compare_checkpoints=True,
        )
        if mismatch is None:
            return

        idx, field = mismatch
        lock_step = base_lock.steps[idx - 1]
        expected = coherence_steps[idx - 1]
        if field == "migration_file":
            raise ReplayError(
                f"Base lock migration mismatch at step {idx}: "
                f"{lock_step.migration_file} != {expected.migration_file}"
            )
        if field == "migration_digest":
            raise ReplayError(
                f"Base lock migration digest mismatch at step {idx}: {lock_step.migration_file}"
            )
        if field == "checkpoint_file":
            raise ReplayError(
                f"Base lock checkpoint mapping mismatch at step {idx}: "
                f"{lock_step.checkpoint_file} != {expected.checkpoint_file}"
            )
        if field == "checkpoint_digest":
            raise ReplayError(
                f"Base lock checkpoint digest mismatch at step {idx}: {lock_step.checkpoint_file}"
            )
        if field == "chain_hash":
            raise ReplayError(
                f"Base lock chain mismatch at step {idx}: {lock_step.migration_file}"
            )
        raise ReplayError(f"Base lock mismatch at step {idx}: {field}")

    def _require_head_lock_coherence(
        self,
        *,
        op: SchemaOpContext,
        head_migrations: tuple[_MigrationState, ...],
    ) -> None:
        if not op.target_paths.lock_file.exists():
            raise ReplayError("Head worktree is missing schema.lock.toml for base-aware mode.")
        lock = load_lock_from_text(op.target_paths.lock_file.read_text(encoding="utf-8"))
        if lock.target != op.target_id.name:
            raise ReplayError(
                f"Head lock target mismatch: expected {op.target_id.name}, got {lock.target}."
            )
        if len(lock.steps) != len(head_migrations):
            raise ReplayError("Head lock step count does not match worktree migration set.")
        divergence = first_divergence_against_lock(
            lock=lock,
            steps=_workspace_coherence_steps(head_migrations),
            engine=Engine(lock.engine),
            target_key=op.target_key,
        )
        if divergence <= len(head_migrations):
            step = head_migrations[divergence - 1]
            raise ReplayError(
                "Head lock is not coherent with worktree at "
                f"{step.migration.rel_path}; run schema apply."
            )

    def _execute_replay(
        self,
        *,
        op: SchemaOpContext,
        replay_plan: _ReplayPlan,
    ) -> tuple[SqlComparison, str, str]:
        scratch = self._ctx.scratch.prepare(
            engine=op.replay_engine,
            scratch_name=f"matey_{op.target_id.name}_schema_replay_{op.run_nonce}",
            purpose="schema_replay",
            test_base_url=op.test_url,
            keep=op.keep_scratch,
        )
        b_raw: str | None = None
        try:
            self._ensure_scratch_ready(op=op, handle=scratch)
            if replay_plan.anchor_sql is not None:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as handle:
                    anchor_path = Path(handle.name)
                    handle.write(replay_plan.anchor_sql)
                try:
                    result = self._ctx.dbmate.load_schema(
                        url=scratch.url,
                        schema_path=anchor_path,
                        migrations_dir=op.target_paths.migrations_dir,
                    )
                finally:
                    anchor_path.unlink(missing_ok=True)
                require_cmd_success(result, "dbmate load failed while loading replay anchor checkpoint")

            if replay_plan.tail_states:
                with tempfile.TemporaryDirectory(prefix="matey-tail-") as tmp_name:
                    temp_dir = Path(tmp_name)
                    for state in replay_plan.tail_states:
                        src = op.target_paths.db_dir / state.migration.rel_path
                        dst = temp_dir / state.migration.filename
                        dst.write_bytes(src.read_bytes())
                    up_result = self._ctx.dbmate.up(url=scratch.url, migrations_dir=temp_dir)
                    require_cmd_success(up_result, "dbmate up failed while replaying tail migrations")

            dump_result = self._ctx.dbmate.dump(url=scratch.url, migrations_dir=op.target_paths.migrations_dir)
            require_cmd_success(dump_result, "dbmate dump failed for replay scratch")
            b_raw = extract_dump_sql(cmd_result_to_output(dump_result))

            a_text = op.target_paths.schema_file.read_text(encoding="utf-8") if op.target_paths.schema_file.exists() else ""
            comparison = self._ctx.sql_pipeline.compare(
                engine=op.replay_engine,
                expected=SqlSource(text=a_text, origin="artifact"),
                actual=SqlSource(text=b_raw, origin="scratch_dump", context_url=scratch.url),
            )
            return comparison, comparison.actual.normalized, scratch.url
        finally:
            self._cleanup_scratch(op=op, handle=scratch)

    def _execute_down_roundtrip(self, *, op: SchemaOpContext, replay_plan: _ReplayPlan) -> None:
        if not replay_plan.tail_states:
            return

        scratch = self._ctx.scratch.prepare(
            engine=op.replay_engine,
            scratch_name=f"matey_{op.target_id.name}_schema_down_{op.run_nonce}",
            purpose="schema_down",
            test_base_url=op.test_url,
            keep=op.keep_scratch,
        )

        try:
            self._ensure_scratch_ready(op=op, handle=scratch)

            if replay_plan.anchor_sql is not None:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as handle:
                    anchor_path = Path(handle.name)
                    handle.write(replay_plan.anchor_sql)
                try:
                    load_result = self._ctx.dbmate.load_schema(
                        url=scratch.url,
                        schema_path=anchor_path,
                        migrations_dir=op.target_paths.migrations_dir,
                    )
                finally:
                    anchor_path.unlink(missing_ok=True)
                require_cmd_success(load_result, "dbmate load failed for down-roundtrip anchor")

            for state in replay_plan.tail_states:
                src_path = op.target_paths.db_dir / state.migration.rel_path
                down_state = _down_state(src_path)
                baseline_raw: str | None = None
                if down_state.has_executable_sql:
                    dump_before = self._ctx.dbmate.dump(
                        url=scratch.url,
                        migrations_dir=op.target_paths.migrations_dir,
                    )
                    require_cmd_success(dump_before, "dbmate dump failed before down-roundtrip step")
                    baseline_raw = extract_dump_sql(cmd_result_to_output(dump_before))

                with tempfile.TemporaryDirectory(prefix="matey-down-step-") as tmp_name:
                    temp_dir = Path(tmp_name)
                    (temp_dir / state.migration.filename).write_bytes(src_path.read_bytes())

                    up_result = self._ctx.dbmate.up(url=scratch.url, migrations_dir=temp_dir)
                    require_cmd_success(
                        up_result,
                        f"dbmate up failed for down-roundtrip step {state.migration.rel_path}",
                    )

                    if not down_state.has_executable_sql:
                        continue

                    rollback_result = self._ctx.dbmate.rollback(url=scratch.url, migrations_dir=temp_dir, steps=1)
                    require_cmd_success(
                        rollback_result,
                        f"dbmate rollback failed for down-roundtrip step {state.migration.rel_path}",
                    )

                    dump_after = self._ctx.dbmate.dump(
                        url=scratch.url,
                        migrations_dir=op.target_paths.migrations_dir,
                    )
                    require_cmd_success(
                        dump_after,
                        f"dbmate dump failed after rollback for step {state.migration.rel_path}",
                    )
                    after_raw = extract_dump_sql(cmd_result_to_output(dump_after))
                    assert baseline_raw is not None
                    comparison = self._ctx.sql_pipeline.compare(
                        engine=op.replay_engine,
                        expected=SqlSource(text=baseline_raw, origin="scratch_dump", context_url=scratch.url),
                        actual=SqlSource(text=after_raw, origin="scratch_dump", context_url=scratch.url),
                    )
                    if not comparison.equal:
                        raise ReplayError(
                            f"Down roundtrip mismatch for {state.migration.rel_path}:\n{comparison.diff or ''}".rstrip()
                        )

                    reapply_result = self._ctx.dbmate.up(url=scratch.url, migrations_dir=temp_dir)
                    require_cmd_success(
                        reapply_result,
                        f"dbmate up failed while re-applying {state.migration.rel_path}",
                    )
        finally:
            self._cleanup_scratch(op=op, handle=scratch)

    def _build_checkpoint_texts(
        self,
        *,
        op: SchemaOpContext,
        replay_plan: _ReplayPlan,
    ) -> dict[str, str]:
        head_states = self._workspace_migrations(op.target_paths)
        prefix_count = 0 if op.clean else max(0, replay_plan.divergence - 1)
        checkpoint_texts: dict[str, str] = {}

        lock: SchemaLock | None = None
        if op.target_paths.lock_file.exists():
            lock = load_lock_from_text(op.target_paths.lock_file.read_text(encoding="utf-8"))

        for state in head_states[:prefix_count]:
            if not state.checkpoint_exists:
                raise CheckpointIntegrityError(
                    f"Missing unchanged-prefix checkpoint: {state.checkpoint_rel}. Run schema apply --clean."
                )
            if lock is None:
                raise CheckpointIntegrityError("Missing lockfile for prefix checkpoint validation.")
            step = next((s for s in lock.steps if s.migration_file == state.migration.rel_path), None)
            if step is None or step.checkpoint_digest != state.checkpoint_digest:
                raise CheckpointIntegrityError(
                    f"Prefix checkpoint digest mismatch for {state.checkpoint_rel}. Run schema apply --clean."
                )
            checkpoint_texts[state.checkpoint_rel] = state.checkpoint_path.read_text(encoding="utf-8")

        tail_states = head_states[prefix_count:]
        if tail_states:
            captured = self._capture_tail_checkpoints(
                op=op,
                anchor_sql=replay_plan.anchor_sql if prefix_count > 0 else None,
                states=tuple(tail_states),
            )
            checkpoint_texts.update(captured)

        return checkpoint_texts

    def _capture_tail_checkpoints(
        self,
        *,
        op: SchemaOpContext,
        anchor_sql: str | None,
        states: tuple[_MigrationState, ...],
    ) -> dict[str, str]:
        scratch = self._ctx.scratch.prepare(
            engine=op.replay_engine,
            scratch_name=f"matey_{op.target_id.name}_checkpoint_{op.run_nonce}",
            purpose="checkpoint_capture",
            test_base_url=op.test_url,
            keep=op.keep_scratch,
        )
        checkpoints: dict[str, str] = {}
        try:
            self._ensure_scratch_ready(op=op, handle=scratch)

            if anchor_sql is not None:
                with tempfile.NamedTemporaryFile(mode="w", suffix=".sql", delete=False) as handle:
                    anchor_path = Path(handle.name)
                    handle.write(anchor_sql)
                try:
                    load_result = self._ctx.dbmate.load_schema(
                        url=scratch.url,
                        schema_path=anchor_path,
                        migrations_dir=op.target_paths.migrations_dir,
                    )
                finally:
                    anchor_path.unlink(missing_ok=True)
                require_cmd_success(load_result, "dbmate load failed for checkpoint capture anchor")

            for state in states:
                src = op.target_paths.db_dir / state.migration.rel_path
                with tempfile.TemporaryDirectory(prefix="matey-checkpoint-step-") as tmp_name:
                    temp_dir = Path(tmp_name)
                    (temp_dir / state.migration.filename).write_bytes(src.read_bytes())
                    up_result = self._ctx.dbmate.up(url=scratch.url, migrations_dir=temp_dir)
                    require_cmd_success(up_result, "dbmate up failed during checkpoint capture")
                dump_result = self._ctx.dbmate.dump(url=scratch.url, migrations_dir=op.target_paths.migrations_dir)
                require_cmd_success(dump_result, "dbmate dump failed during checkpoint capture")
                checkpoints[state.checkpoint_rel] = extract_dump_sql(cmd_result_to_output(dump_result))
            return checkpoints
        finally:
            self._cleanup_scratch(op=op, handle=scratch)

    def _apply_artifacts(
        self,
        *,
        op: SchemaOpContext,
        normalized_schema_sql: str,
        checkpoint_texts: dict[str, str],
        orphan_checkpoints: tuple[str, ...],
    ) -> None:
        head_states = self._workspace_migrations(op.target_paths)
        checkpoint_digest_by_migration: dict[str, str] = {}
        schema_digest_by_migration: dict[str, str] = {}

        writes: list[ArtifactWrite] = [
            ArtifactWrite(rel_path="schema.sql", content=normalized_schema_sql.encode("utf-8"))
        ]

        for state in head_states:
            checkpoint_text = checkpoint_texts.get(state.checkpoint_rel)
            if checkpoint_text is None:
                raise CheckpointIntegrityError(
                    f"Missing checkpoint text for migration {state.migration.rel_path}."
                )
            writes.append(
                ArtifactWrite(rel_path=state.checkpoint_rel, content=checkpoint_text.encode("utf-8"))
            )
            checkpoint_digest_by_migration[state.migration.rel_path] = digest_bytes_blake2b256(
                checkpoint_text.encode("utf-8")
            )
            prepared = self._ctx.sql_pipeline.prepare(
                engine=op.replay_engine,
                source=SqlSource(text=checkpoint_text, origin="artifact"),
            )
            schema_digest_by_migration[state.migration.rel_path] = prepared.digest

        seed = lock_chain_seed(op.replay_engine, op.target_key)
        chain = seed
        steps: list[LockStep] = []
        for index, state in enumerate(head_states, start=1):
            chain = lock_chain_step(
                chain,
                state.migration.version,
                state.migration.rel_path,
                state.migration_digest,
            )
            checkpoint_rel = state.checkpoint_rel
            steps.append(
                LockStep(
                    index=index,
                    version=state.migration.version,
                    migration_file=state.migration.rel_path,
                    migration_digest=state.migration_digest,
                    checkpoint_file=checkpoint_rel,
                    checkpoint_digest=checkpoint_digest_by_migration[state.migration.rel_path],
                    schema_digest=schema_digest_by_migration[state.migration.rel_path],
                    chain_hash=chain,
                )
            )

        schema_digest = self._ctx.sql_pipeline.prepare(
            engine=op.replay_engine,
            source=SqlSource(text=normalized_schema_sql, origin="artifact"),
        ).digest

        lock = SchemaLock(
            lock_version=0,
            hash_algorithm="blake2b-256",
            canonicalizer="matey-sql-v0",
            engine=op.replay_engine.value,
            target=op.target_id.name,
            schema_file="schema.sql",
            migrations_dir="migrations",
            checkpoints_dir="checkpoints",
            head_index=len(steps),
            head_chain_hash=chain if steps else seed,
            head_schema_digest=schema_digest,
            steps=tuple(steps),
        )

        writes.append(ArtifactWrite(rel_path="schema.lock.toml", content=lock.to_toml().encode("utf-8")))

        deletes = tuple(ArtifactDelete(rel_path=rel) for rel in sorted(set(orphan_checkpoints)))

        txid = self._ctx.artifact_store.begin(
            target_key=op.target_key,
            target_root=op.target_paths.db_dir,
            writes=tuple(writes),
            deletes=deletes,
        )
        self._ctx.artifact_store.apply(txid=txid)
        self._ctx.artifact_store.finalize(txid=txid)

    def _ensure_scratch_ready(self, *, op: SchemaOpContext, handle: ScratchHandle) -> None:
        policy = self._ctx.engine_policies.get(op.replay_engine)
        if policy.wait_required:
            wait = self._ctx.dbmate.wait(url=handle.url, timeout_seconds=60)
            require_cmd_success(wait, "dbmate wait failed for scratch")
        create = self._ctx.dbmate.create(url=handle.url, migrations_dir=op.target_paths.migrations_dir)
        require_cmd_success(create, "dbmate create failed for scratch")

    def _cleanup_scratch(self, *, op: SchemaOpContext, handle: ScratchHandle) -> None:
        cleanup_errors: list[str] = []
        if handle.cleanup_required and not op.keep_scratch:
            result = self._ctx.dbmate.drop(url=handle.url, migrations_dir=op.target_paths.migrations_dir)
            if result.exit_code != 0:
                cleanup_errors.append((result.stderr or result.stdout or "dbmate drop failed").strip())
        try:
            self._ctx.scratch.cleanup(handle)
        except Exception as error:
            cleanup_errors.append(str(error))
        if cleanup_errors:
            raise ReplayError("Scratch cleanup failed: " + "; ".join(cleanup_errors))
