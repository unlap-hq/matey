from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path

from matey.engine import classify_create_outcome, classify_missing_db, detect_engine_from_url
from matey.errors import (
    BigQueryPreflightError,
    EngineInferenceError,
    ExternalCommandError,
    LiveDriftError,
    LiveHistoryMismatchError,
    LockfileError,
    SchemaMismatchError,
)
from matey.lock import SchemaLock, load_lock_from_text
from matey.models import (
    ConfigDefaults,
    DbOpContext,
    DbPlanResult,
    Engine,
    SqlSource,
    derive_target_key,
)
from matey.parsing import (
    DbStatusSnapshot,
    cmd_result_to_output,
    extract_dump_sql,
    extract_status_text,
    parse_status_output,
    require_cmd_success,
)
from matey.platform import normalized_optional
from matey.runtime import AppContext
from matey.schema import SchemaEngine


@dataclass(frozen=True)
class DbPlanOutput:
    result: DbPlanResult
    expected_sql: str


class DbEngine:
    def __init__(self, *, context: AppContext, schema_engine: SchemaEngine) -> None:
        self._ctx = context
        self._schema_engine = schema_engine

    def db_new(self, *, runtime, name: str) -> str:
        with self._target_scope(runtime):
            result = self._ctx.dbmate.new(name=name, migrations_dir=runtime.paths.migrations_dir)
            require_cmd_success(result, "dbmate new failed")
            return result.stdout

    def db_create(self, *, runtime, url_override: str | None) -> str:
        with self._target_scope(runtime):
            url = self._resolve_live_url(runtime=runtime, url_override=url_override)
            result = self._ctx.dbmate.create(url=url, migrations_dir=runtime.paths.migrations_dir)
            require_cmd_success(result, "dbmate create failed")
            return result.stdout

    def db_wait(
        self,
        *,
        runtime,
        url_override: str | None,
        timeout_seconds: int = 60,
    ) -> str:
        with self._target_scope(runtime):
            url = self._resolve_live_url(runtime=runtime, url_override=url_override)
            result = self._ctx.dbmate.wait(url=url, timeout_seconds=timeout_seconds)
            require_cmd_success(result, "dbmate wait failed")
            return result.stdout

    def db_status(self, *, runtime, url_override: str | None) -> str:
        with self._target_scope(runtime):
            url = self._resolve_live_url(runtime=runtime, url_override=url_override)
            result = self._ctx.dbmate.status(url=url, migrations_dir=runtime.paths.migrations_dir)
            require_cmd_success(result, "dbmate status failed")
            payload = result.stdout
            if payload:
                return payload
            return result.stderr

    def db_load(
        self,
        *,
        runtime,
        url_override: str | None,
        schema_path: Path,
    ) -> str:
        with self._target_scope(runtime):
            url = self._resolve_live_url(runtime=runtime, url_override=url_override)
            result = self._ctx.dbmate.load_schema(
                url=url,
                schema_path=schema_path,
                migrations_dir=runtime.paths.migrations_dir,
            )
            require_cmd_success(result, "dbmate load failed")
            return result.stdout

    def db_dump(self, *, runtime, url_override: str | None) -> str:
        with self._target_scope(runtime):
            url = self._resolve_live_url(runtime=runtime, url_override=url_override)
            result = self._ctx.dbmate.dump(url=url, migrations_dir=runtime.paths.migrations_dir)
            require_cmd_success(result, "dbmate dump failed")
            return extract_dump_sql(cmd_result_to_output(result))

    def db_drop(self, *, runtime, url_override: str | None) -> str:
        with self._target_scope(runtime):
            url = self._resolve_live_url(runtime=runtime, url_override=url_override)
            result = self._ctx.dbmate.drop(url=url, migrations_dir=runtime.paths.migrations_dir)
            require_cmd_success(result, "dbmate drop failed")
            return result.stdout

    def db_raw(
        self,
        *,
        runtime,
        url_override: str | None,
        argv_suffix: tuple[str, ...],
    ) -> str:
        with self._target_scope(runtime):
            url = self._resolve_live_url(runtime=runtime, url_override=url_override)
            result = self._ctx.dbmate.raw(argv_suffix=argv_suffix, url=url, migrations_dir=runtime.paths.migrations_dir)
            require_cmd_success(result, "dbmate command failed")
            return result.stdout

    def db_up(
        self,
        *,
        runtime,
        defaults: ConfigDefaults,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
    ) -> str:
        return self._run_guarded_mutation(
            runtime=runtime,
            defaults=defaults,
            verb="up",
            steps=1,
            url_override=url_override,
            test_url_override=test_url_override,
            keep_scratch=keep_scratch,
        )

    def db_migrate(
        self,
        *,
        runtime,
        defaults: ConfigDefaults,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
    ) -> str:
        return self._run_guarded_mutation(
            runtime=runtime,
            defaults=defaults,
            verb="migrate",
            steps=1,
            url_override=url_override,
            test_url_override=test_url_override,
            keep_scratch=keep_scratch,
        )

    def db_down(
        self,
        *,
        runtime,
        defaults: ConfigDefaults,
        steps: int,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
    ) -> str:
        if steps <= 0:
            raise SchemaMismatchError("db down requires steps > 0")
        return self._run_guarded_mutation(
            runtime=runtime,
            defaults=defaults,
            verb="down",
            steps=steps,
            url_override=url_override,
            test_url_override=test_url_override,
            keep_scratch=keep_scratch,
        )

    def db_drift(
        self,
        *,
        runtime,
        defaults: ConfigDefaults,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
    ) -> DbPlanOutput:
        return self._run_db_compare(
            runtime=runtime,
            defaults=defaults,
            mode="current",
            url_override=url_override,
            test_url_override=test_url_override,
            keep_scratch=keep_scratch,
            enforce_equal=True,
        )

    def db_plan(
        self,
        *,
        runtime,
        defaults: ConfigDefaults,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
    ) -> DbPlanOutput:
        return self._run_db_compare(
            runtime=runtime,
            defaults=defaults,
            mode="head",
            url_override=url_override,
            test_url_override=test_url_override,
            keep_scratch=keep_scratch,
            enforce_equal=False,
        )

    def db_plan_sql(self, *, runtime, defaults: ConfigDefaults) -> str:
        status = self._schema_engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None)
        if status.stale:
            raise SchemaMismatchError(
                "schema status is stale; run schema apply before db plan sql"
            )
        if not runtime.paths.schema_file.exists():
            return ""
        return runtime.paths.schema_file.read_text(encoding="utf-8")

    def _run_guarded_mutation(
        self,
        *,
        runtime,
        defaults: ConfigDefaults,
        verb: str,
        steps: int,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
    ) -> str:
        status = self._schema_engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None)
        if status.stale:
            raise SchemaMismatchError("schema status must be up-to-date before live mutations")

        op, lock = self._build_db_context(
            runtime=runtime,
            url_override=url_override,
            test_url_override=test_url_override,
            keep_scratch=keep_scratch,
        )

        with self._ctx.scope.open(target_key=op.target_key, target_root=op.target_paths.db_dir):
            pre_snapshot = self._preflight_status(op=op, lock=lock, verb=verb)
            pre_index = pre_snapshot.applied_count
            self._require_lock_prefix(lock=lock, snapshot=pre_snapshot)
            self._run_pre_or_post_compare(op=op, lock=lock, index=pre_index)

            if verb == "up":
                mutate = self._ctx.dbmate.up(url=op.live_url, migrations_dir=op.target_paths.migrations_dir)
                require_cmd_success(mutate, "dbmate up failed")
            elif verb == "migrate":
                mutate = self._ctx.dbmate.migrate(
                    url=op.live_url,
                    migrations_dir=op.target_paths.migrations_dir,
                )
                require_cmd_success(mutate, "dbmate migrate failed")
            else:
                mutate = self._ctx.dbmate.rollback(
                    url=op.live_url,
                    migrations_dir=op.target_paths.migrations_dir,
                    steps=steps,
                )
                require_cmd_success(mutate, "dbmate rollback failed")

            post_snapshot = self._require_status_snapshot(op=op)
            expected_index = len(lock.steps) if verb in {"up", "migrate"} else max(0, pre_index - steps)
            if post_snapshot.applied_count != expected_index:
                raise LiveHistoryMismatchError(
                    f"Unexpected applied index after {verb}: expected {expected_index}, got {post_snapshot.applied_count}"
                )
            self._require_lock_prefix(lock=lock, snapshot=post_snapshot)
            self._run_pre_or_post_compare(op=op, lock=lock, index=post_snapshot.applied_count)
            payload = mutate.stdout
            if payload:
                return payload
            return mutate.stderr

    def _run_db_compare(
        self,
        *,
        runtime,
        defaults: ConfigDefaults,
        mode: str,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
        enforce_equal: bool,
    ) -> DbPlanOutput:
        status = self._schema_engine.schema_status(runtime=runtime, defaults=defaults, base_ref=None)
        if status.stale:
            raise SchemaMismatchError("schema status must be up-to-date before db comparisons")

        op, lock = self._build_db_context(
            runtime=runtime,
            url_override=url_override,
            test_url_override=test_url_override,
            keep_scratch=keep_scratch,
        )
        with self._ctx.scope.open(target_key=op.target_key, target_root=op.target_paths.db_dir):
            snapshot = self._require_status_snapshot(op=op)
            self._require_lock_prefix(lock=lock, snapshot=snapshot)

            live_dump = self._ctx.dbmate.dump(url=op.live_url, migrations_dir=op.target_paths.migrations_dir)
            require_cmd_success(live_dump, "dbmate dump failed")
            live_sql = extract_dump_sql(cmd_result_to_output(live_dump))

            if mode == "current":
                expected_sql = self._expected_sql_for_index(op=op, lock=lock, index=snapshot.applied_count)
            elif mode == "head":
                expected_sql = op.target_paths.schema_file.read_text(encoding="utf-8")
            else:
                raise SchemaMismatchError(f"Unsupported compare mode: {mode}")

            comparison = self._ctx.sql_pipeline.compare(
                engine=op.live_engine,
                expected=SqlSource(text=expected_sql, origin="artifact"),
                actual=SqlSource(text=live_sql, origin="live_dump", context_url=op.live_url),
            )
            if enforce_equal and not comparison.equal:
                raise LiveDriftError(comparison.diff or "Live schema drift detected.")

            return DbPlanOutput(
                result=DbPlanResult(comparison=comparison, live_applied_index=snapshot.applied_count),
                expected_sql=expected_sql,
            )

    def _target_scope(self, runtime):
        return self._ctx.scope.open(
            target_key=derive_target_key(repo_root=self._ctx.git.repo_root(), db_dir=runtime.paths.db_dir),
            target_root=runtime.paths.db_dir,
        )

    def _build_db_context(
        self,
        *,
        runtime,
        url_override: str | None,
        test_url_override: str | None,
        keep_scratch: bool,
    ) -> tuple[DbOpContext, SchemaLock]:
        live_url = self._resolve_live_url(runtime=runtime, url_override=url_override)
        live_engine = detect_engine_from_url(live_url)
        lock = self._load_lock(runtime=runtime)
        lock_engine = Engine(lock.engine)
        if lock_engine != live_engine:
            raise EngineInferenceError(
                f"Live engine {live_engine.value} does not match lock engine {lock_engine.value}."
            )
        test_url = normalized_optional(test_url_override) or normalized_optional(self._ctx.env.get(runtime.test_url_env))
        op = DbOpContext(
            target_id=runtime.target_id,
            target_key=derive_target_key(repo_root=self._ctx.git.repo_root(), db_dir=runtime.paths.db_dir),
            target_paths=runtime.paths,
            live_engine=live_engine,
            live_url=live_url,
            test_url=test_url,
            keep_scratch=keep_scratch,
            run_nonce=uuid.uuid4().hex[:10],
        )
        return op, lock

    def _resolve_live_url(self, *, runtime, url_override: str | None) -> str:
        live_url = normalized_optional(url_override) or normalized_optional(self._ctx.env.get(runtime.url_env))
        if live_url is None:
            raise EngineInferenceError("Live database URL is required (--url or target url_env).")
        return live_url

    def _load_lock(self, *, runtime) -> SchemaLock:
        if not runtime.paths.lock_file.exists():
            raise LockfileError(f"Lockfile missing: {runtime.paths.lock_file}")
        return load_lock_from_text(runtime.paths.lock_file.read_text(encoding="utf-8"))

    def _preflight_status(self, *, op: DbOpContext, lock: SchemaLock, verb: str) -> DbStatusSnapshot:
        policy = self._ctx.engine_policies.get(op.live_engine)

        if verb == "up" and op.live_engine is Engine.BIGQUERY:
            create_result = self._ctx.dbmate.create(url=op.live_url, migrations_dir=op.target_paths.migrations_dir)
            if create_result.exit_code != 0:
                detail = f"{create_result.stderr}\n{create_result.stdout}".strip().lower()
                outcome = classify_create_outcome(policy, detail)
                if outcome == "fatal":
                    raise BigQueryPreflightError(f"BigQuery preflight create failed: {detail}")
            return self._require_status_snapshot(op=op)

        status_result = self._ctx.dbmate.status(url=op.live_url, migrations_dir=op.target_paths.migrations_dir)
        if status_result.exit_code == 0:
            return parse_status_output(extract_status_text(cmd_result_to_output(status_result)))

        if verb == "up":
            detail = f"{status_result.stderr}\n{status_result.stdout}".strip().lower()
            if classify_missing_db(policy, detail):
                create_result = self._ctx.dbmate.create(
                    url=op.live_url,
                    migrations_dir=op.target_paths.migrations_dir,
                )
                require_cmd_success(create_result, "dbmate create failed for missing database")
                return self._require_status_snapshot(op=op)

        require_cmd_success(status_result, "dbmate status failed")
        return self._require_status_snapshot(op=op)

    def _require_status_snapshot(self, *, op: DbOpContext) -> DbStatusSnapshot:
        status_result = self._ctx.dbmate.status(url=op.live_url, migrations_dir=op.target_paths.migrations_dir)
        require_cmd_success(status_result, "dbmate status failed")
        return parse_status_output(extract_status_text(cmd_result_to_output(status_result)))

    @staticmethod
    def _require_lock_prefix(*, lock: SchemaLock, snapshot: DbStatusSnapshot) -> None:
        raw_applied = snapshot.applied_files
        normalized_applied = tuple(file if "/" in file else f"migrations/{file}" for file in raw_applied)
        expected_prefix = tuple(step.migration_file for step in lock.steps[: len(normalized_applied)])
        if normalized_applied != expected_prefix:
            raise LiveHistoryMismatchError(
                "Live migration history does not match lock prefix. "
                f"applied={raw_applied} normalized_applied={normalized_applied} expected_prefix={expected_prefix}"
            )

    def _expected_sql_for_index(self, *, op: DbOpContext, lock: SchemaLock, index: int) -> str:
        if index < 0 or index > len(lock.steps):
            raise LiveHistoryMismatchError(
                f"Live applied index {index} is out of range for lock length {len(lock.steps)}."
            )
        if index == 0:
            return self._empty_baseline_sql(op=op)
        checkpoint_rel = lock.steps[index - 1].checkpoint_file
        checkpoint_path = op.target_paths.db_dir / checkpoint_rel
        if not checkpoint_path.exists():
            raise LockfileError(f"Expected checkpoint not found for index {index}: {checkpoint_path}")
        return checkpoint_path.read_text(encoding="utf-8")

    def _empty_baseline_sql(self, *, op: DbOpContext) -> str:
        policy = self._ctx.engine_policies.get(op.live_engine)
        if policy.requires_test_url_for_index0 and not op.test_url:
            raise SchemaMismatchError(
                "Index-0 baseline for this engine requires --test-url or test_url_env."
            )
        scratch = self._ctx.scratch.prepare(
            engine=op.live_engine,
            scratch_name=f"matey_{op.target_id.name}_baseline_{op.run_nonce}",
            purpose="db_baseline",
            test_base_url=op.test_url,
            keep=op.keep_scratch,
        )
        primary_error: BaseException | None = None
        try:
            if policy.wait_required:
                wait = self._ctx.dbmate.wait(url=scratch.url, timeout_seconds=60)
                require_cmd_success(wait, "dbmate wait failed for baseline scratch")
            create = self._ctx.dbmate.create(url=scratch.url, migrations_dir=op.target_paths.migrations_dir)
            require_cmd_success(create, "dbmate create failed for baseline scratch")
            dump = self._ctx.dbmate.dump(url=scratch.url, migrations_dir=op.target_paths.migrations_dir)
            require_cmd_success(dump, "dbmate dump failed for baseline scratch")
            return extract_dump_sql(cmd_result_to_output(dump))
        except BaseException as error:
            primary_error = error
            raise
        finally:
            cleanup_errors: list[str] = []
            if not op.keep_scratch:
                if scratch.cleanup_required:
                    drop = self._ctx.dbmate.drop(url=scratch.url, migrations_dir=op.target_paths.migrations_dir)
                    if drop.exit_code != 0:
                        cleanup_errors.append((drop.stderr or drop.stdout or "dbmate drop failed").strip())
                try:
                    self._ctx.scratch.cleanup(scratch)
                except Exception as error:  # pragma: no cover - defensive branch
                    cleanup_errors.append(str(error))
            if cleanup_errors:
                message = "Scratch cleanup failed: " + "; ".join(cleanup_errors)
                if primary_error is not None:
                    primary_error.add_note(message)
                else:
                    raise ExternalCommandError(message)

    def _run_pre_or_post_compare(self, *, op: DbOpContext, lock: SchemaLock, index: int) -> None:
        expected_sql = self._expected_sql_for_index(op=op, lock=lock, index=index)
        live_dump = self._ctx.dbmate.dump(url=op.live_url, migrations_dir=op.target_paths.migrations_dir)
        require_cmd_success(live_dump, "dbmate dump failed for live comparison")
        live_sql = extract_dump_sql(cmd_result_to_output(live_dump))
        comparison = self._ctx.sql_pipeline.compare(
            engine=op.live_engine,
            expected=SqlSource(text=expected_sql, origin="artifact"),
            actual=SqlSource(text=live_sql, origin="live_dump", context_url=op.live_url),
        )
        if not comparison.equal:
            raise LiveDriftError(comparison.diff or "Live schema drift detected.")
