from __future__ import annotations

import tempfile
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from matey.dbmate import CmdResult, DbConnection, Dbmate
from matey.sql import SqlProgram, WriteViolation, ensure_newline

from .plan import SchemaError, StructuralPlan


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
) -> ReplayOutcome:
    validate_tail_migration_targets(structural)
    with tempfile.TemporaryDirectory(prefix="matey-schema-tail-") as temp_root:
        tail_dir = write_tail_slice(structural, Path(temp_root))
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

        checkpoint_sql_by_file, down_checked, down_skipped, down_scratch_url = run_down_roundtrip(
            structural,
            keep_scratch=keep_scratch,
            dbmate_bin=dbmate_bin,
        )
        return ReplayOutcome(
            replay_scratch_url=replay_scratch_url,
            down_scratch_url=down_scratch_url,
            replay_schema_sql=replay_schema_sql,
            checkpoint_sql_by_file=checkpoint_sql_by_file,
            down_checked=down_checked,
            down_skipped=down_skipped,
        )


def run_down_roundtrip(
    structural: StructuralPlan,
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
        lease_bootstrapped_connection(
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
                step_dir = write_tail_slice(
                    structural,
                    Path(temp_root),
                    step_count=index,
                )
                step_dbmate = Dbmate(migrations_dir=step_dir, dbmate_bin=dbmate_bin)
                step_conn = step_dbmate.database(down_scratch_url)

                program = SqlProgram(
                    migration_payload(structural, step).decode("utf-8"),
                    engine=structural.engine.value,
                )
                has_down = program.has_executable_down()
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
                if not SqlProgram(baseline, engine=structural.engine.value).schema_equals(
                    SqlProgram(after_rollback, engine=structural.engine.value),
                    left_context_url=down_scratch_url,
                    right_context_url=down_scratch_url,
                ):
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
        bootstrap_scratch(
            conn=conn,
            engine=structural.engine,
            anchor_sql=structural.anchor_sql,
            context=context,
        )
        yield conn, lease.url


def bootstrap_scratch(
    *,
    conn: DbConnection,
    engine: object,
    anchor_sql: str | None,
    context: str,
) -> None:
    from matey.scratch import Engine

    if engine in {Engine.POSTGRES, Engine.MYSQL, Engine.CLICKHOUSE}:
        require_ok(conn.wait(60), context=f"{context} wait")
    require_ok(conn.create(), context=f"{context} create")
    if anchor_sql is not None:
        program = SqlProgram(anchor_sql, engine=engine.value)
        try:
            statements = program.anchor_statements(target_url=conn.url)
        except ValueError as error:
            raise SchemaError(f"{context} load anchor failed: {error}") from error
        for index, statement in enumerate(statements, start=1):
            require_ok(
                conn.load(ensure_newline(f"{statement};")),
                context=f"{context} load anchor statement {index}",
            )


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
        payload = migration_payload(structural, step)
        rel = Path(step.migration_file).relative_to(structural.policy.migrations_dir)
        output_path = migrations_dir / rel
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(payload)
    return migrations_dir


def migration_payload(structural: StructuralPlan, step: object) -> bytes:
    payload = structural.head_snapshot.migrations.get(step.migration_file)
    if payload is None:
        raise SchemaError(f"Missing migration payload for {step.migration_file}.")
    return payload


def dump_schema(conn: DbConnection, *, context: str) -> str:
    result = conn.dump()
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
    if engine not in {"bigquery", "mysql", "clickhouse"}:
        return
    for step in structural.tail_steps:
        program = SqlProgram(
            migration_payload(structural, step).decode("utf-8"),
            engine=engine,
        )
        raise_on_write_violations(
            step.migration_file,
            engine=engine,
            violations=program.migration_write_violations(),
        )


def raise_on_write_violations(
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


__all__ = [
    "ReplayOutcome",
    "bootstrap_scratch",
    "dump_schema",
    "lease_bootstrapped_connection",
    "migration_payload",
    "raise_on_write_violations",
    "require_ok",
    "run_down_roundtrip",
    "run_replay_checks",
    "validate_tail_migration_targets",
    "write_tail_slice",
]
