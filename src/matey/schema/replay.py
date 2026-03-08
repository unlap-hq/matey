from __future__ import annotations

import tempfile
import uuid
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TypeVar
from urllib.parse import urlsplit

from matey.dbmate import CmdResult, DbConnection, Dbmate, DbmateError
from matey.lockfile import WorktreeStep
from matey.scratch import Engine, ScratchLease
from matey.sql import (
    SqlError,
    SqlProgram,
    ensure_newline,
    first_migration_violation_message,
)

from .plan import SchemaError, StructuralPlan

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class ReplayOutcome:
    replay_scratch_url: str
    down_scratch_url: str | None
    replay_schema_sql: str
    checkpoint_sql_by_file: Mapping[str, str]
    down_checked: tuple[str, ...]
    down_skipped: tuple[str, ...]


def run_replay_checks(
    structural: StructuralPlan,
    *,
    keep_scratch: bool,
    dbmate_bin: Path | None,
    after_replay: Callable[[DbConnection, str], T] | None = None,
) -> tuple[ReplayOutcome, T | None]:
    validate_tail_migration_targets(structural)
    with tempfile.TemporaryDirectory(prefix="matey-schema-tail-") as temp_root:
        tail_dir = write_tail_slice(structural, Path(temp_root))
        replay_extra: T | None = None
        with lease_bootstrapped_connection(
            structural=structural,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            migrations_dir=tail_dir,
            scratch_label="schema_replay",
            context="replay",
        ) as (replay_conn, replay_scratch_url):
            if structural.tail_steps:
                require_ok(
                    replay_conn.migrate(),
                    context="replay tail migrations",
                )
            replay_schema_sql = dump_schema(replay_conn, context="replay")
            if after_replay is not None:
                replay_extra = after_replay(replay_conn, replay_scratch_url)

        checkpoint_sql_by_file, down_checked, down_skipped, down_scratch_url = run_down_roundtrip(
            structural,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
        )
        return (
            ReplayOutcome(
                replay_scratch_url=replay_scratch_url,
                down_scratch_url=down_scratch_url,
                replay_schema_sql=replay_schema_sql,
                checkpoint_sql_by_file=checkpoint_sql_by_file,
                down_checked=down_checked,
                down_skipped=down_skipped,
            ),
            replay_extra,
        )


def run_down_roundtrip(
    structural: StructuralPlan,
    *,
    keep_scratch: bool,
    dbmate_bin: Path | None,
) -> tuple[dict[str, str], tuple[str, ...], tuple[str, ...], str | None]:
    """Validate reversible tail steps incrementally on one scratch connection.

    The database state moves forward cumulatively through the tail so dbmate only
    ever applies the newly-written migration file. For reversible steps we dump a
    baseline before applying, roll back one step, and compare the rollback state
    to that baseline before re-applying the step.
    """
    if not structural.tail_steps:
        return {}, (), (), None

    down_checked: list[str] = []
    down_skipped: list[str] = []
    checkpoint_sql_by_file: dict[str, str] = {}
    with tempfile.TemporaryDirectory(prefix="matey-schema-down-") as temp_root:
        migrations_dir = Path(temp_root) / "migrations"
        migrations_dir.mkdir(parents=True, exist_ok=True)
        with lease_bootstrapped_connection(
            structural=structural,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
            migrations_dir=migrations_dir,
            scratch_label="schema_down",
            context="down-roundtrip",
        ) as (step_conn, down_scratch_url):
            for step in structural.tail_steps:
                write_tail_migration(migrations_dir, structural, step)
                try:
                    program = SqlProgram(
                        migration_sql_text(structural, step),
                        engine=structural.engine.value,
                    )
                    has_down = program.has_executable_down()
                except SqlError as error:
                    raise SchemaError(
                        f"Down roundtrip SQL analysis failed for {step.migration_file}: {error}"
                    ) from error

                baseline: str | None = None
                if has_down:
                    baseline = dump_schema(
                        step_conn, context=f"down baseline {step.migration_file}"
                    )

                require_ok(
                    step_conn.migrate(),
                    context=f"down-roundtrip migrate {step.migration_file}",
                )
                checkpoint_sql_by_file[step.checkpoint_file] = dump_schema(
                    step_conn,
                    context=f"checkpoint capture {step.migration_file}",
                )

                if not has_down:
                    down_skipped.append(step.migration_file)
                    continue

                require_ok(
                    step_conn.rollback(1),
                    context=f"down-roundtrip rollback {step.migration_file}",
                )
                after_rollback = dump_schema(
                    step_conn,
                    context=f"down rollback dump {step.migration_file}",
                )
                if baseline is None:
                    raise SchemaError(
                        f"Down roundtrip mismatch for {step.migration_file}: baseline was not captured."
                    )
                try:
                    matches = SqlProgram(
                        baseline,
                        engine=structural.engine.value,
                    ).schema_equals(
                        SqlProgram(after_rollback, engine=structural.engine.value),
                        left_context_url=down_scratch_url,
                        right_context_url=down_scratch_url,
                    )
                except SqlError as error:
                    raise SchemaError(
                        f"Down roundtrip SQL analysis failed for {step.migration_file}: {error}"
                    ) from error
                if not matches:
                    raise SchemaError(
                        f"Down roundtrip mismatch for {step.migration_file}: rollback state differs from baseline."
                    )

                require_ok(
                    step_conn.migrate(),
                    context=f"down-roundtrip reapply {step.migration_file}",
                )
                down_checked.append(step.migration_file)

    return checkpoint_sql_by_file, tuple(down_checked), tuple(down_skipped), down_scratch_url


@contextmanager
def lease_bootstrapped_connection(
    *,
    structural: StructuralPlan,
    keep_scratch: bool,
    dbmate_bin: Path | None,
    migrations_dir: Path,
    scratch_label: str,
    context: str,
) -> Iterator[tuple[DbConnection, str]]:
    from matey.scratch import Scratch

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
        cleanup = explicit_scratch_cleanup(
            conn=conn,
            engine=structural.engine,
            lease=lease,
            keep_scratch=keep_scratch,
        )
        active_error: Exception | None = None
        try:
            bootstrap_scratch(
                conn=conn,
                engine=structural.engine,
                anchor_sql=structural.anchor_sql,
                context=context,
            )
            yield conn, lease.url
        except Exception as error:
            active_error = error
            raise
        finally:
            if cleanup is not None:
                try:
                    cleanup()
                except SchemaError:
                    if active_error is None:
                        raise


def bootstrap_scratch(
    *,
    conn: DbConnection,
    engine: Engine,
    anchor_sql: str | None,
    context: str,
) -> None:
    if engine in {Engine.POSTGRES, Engine.MYSQL, Engine.CLICKHOUSE}:
        require_ok(conn.wait(60), context=f"{context} wait")
    require_ok(conn.create(), context=f"{context} create")
    if anchor_sql is not None:
        program = SqlProgram(anchor_sql, engine=engine.value)
        try:
            statements = program.anchor_statements(target_url=conn.url)
        except SqlError as error:
            raise SchemaError(f"{context} load anchor failed: {error}") from error
        for index, statement in enumerate(statements, start=1):
            require_ok(
                conn.load(ensure_newline(f"{statement};")),
                context=f"{context} load anchor statement {index}",
            )


def explicit_scratch_cleanup(
    *,
    conn: DbConnection,
    engine: Engine,
    lease: ScratchLease,
    keep_scratch: bool,
) -> Callable[[], None] | None:
    if keep_scratch or lease.auto_provisioned:
        return None

    def _cleanup() -> None:
        if engine is Engine.SQLITE:
            sqlite_path = _sqlite_file_from_url(conn.url)
            if sqlite_path is not None:
                sqlite_path.unlink(missing_ok=True)
            return
        require_ok(conn.drop(), context="scratch cleanup drop")

    return _cleanup


def _sqlite_file_from_url(url: str) -> Path | None:
    if not url.startswith("sqlite3:"):
        return None
    raw_path = url[len("sqlite3:") :]
    if not raw_path:
        return None
    return Path(urlsplit(raw_path).path or raw_path)


def write_tail_slice(
    structural: StructuralPlan,
    root: Path,
    *,
    step_count: int | None = None,
) -> Path:
    if step_count is not None and step_count <= 0:
        raise ValueError("step_count must be greater than zero.")
    selected = structural.tail_steps if step_count is None else structural.tail_steps[:step_count]
    migrations_dir = root / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    for step in selected:
        write_tail_migration(migrations_dir, structural, step)
    return migrations_dir


def write_tail_migration(
    migrations_dir: Path,
    structural: StructuralPlan,
    step: WorktreeStep,
) -> None:
    payload = migration_payload(structural, step)
    rel = Path(step.migration_file).relative_to(structural.policy.migrations_dir)
    output_path = migrations_dir / rel
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(payload)


def migration_payload(structural: StructuralPlan, step: WorktreeStep) -> bytes:
    payload = structural.head_snapshot.migrations.get(step.migration_file)
    if payload is None:
        raise SchemaError(f"Missing migration payload for {step.migration_file}.")
    return payload


def migration_sql_text(structural: StructuralPlan, step: WorktreeStep) -> str:
    try:
        return migration_payload(structural, step).decode("utf-8")
    except UnicodeDecodeError as error:
        raise SchemaError(f"Unable to decode migration {step.migration_file} as UTF-8.") from error


def dump_schema(conn: DbConnection, *, context: str) -> str:
    try:
        result = conn.dump()
    except DbmateError as error:
        raise SchemaError(f"{context} dump failed: {error}") from error
    require_ok(result, context=f"{context} dump")
    return ensure_newline(result.stdout)


def require_ok(result: CmdResult, *, context: str) -> None:
    if result.exit_code == 0:
        return
    raise SchemaError(
        f"{context} failed (exit_code={result.exit_code}): "
        f"argv={' '.join(result.argv)}; stderr={result.stderr.strip()!r}; stdout={result.stdout.strip()!r}"
    )


def validate_tail_migration_targets(structural: StructuralPlan) -> None:
    engine = structural.engine.value
    if engine not in {"bigquery", "bigquery-emulator", "mysql", "clickhouse"}:
        return
    message = first_migration_violation_message(
        entries=(
            (step.migration_file, migration_payload(structural, step))
            for step in structural.tail_steps
        ),
        engine=engine,
        section="migration",
    )
    if message is not None:
        raise SchemaError(message)


__all__ = [
    "ReplayOutcome",
    "bootstrap_scratch",
    "dump_schema",
    "lease_bootstrapped_connection",
    "migration_payload",
    "migration_sql_text",
    "require_ok",
    "run_down_roundtrip",
    "run_replay_checks",
    "validate_tail_migration_targets",
    "write_tail_migration",
    "write_tail_slice",
]
