from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

TargetKind = Literal["none", "database", "bigquery"]
BIGQUERY_FAMILY = frozenset({"bigquery", "bigquery-emulator"})


@dataclass(frozen=True, slots=True)
class EnginePolicy:
    name: str
    dialect: str
    guarded_writes: bool
    checkpoint_retarget: bool
    target_kind: TargetKind
    nonsemantic_sql_prefixes: tuple[str, ...] = ()
    nonsemantic_command_prefixes: tuple[str, ...] = ()


_POLICIES: dict[str, EnginePolicy] = {
    "": EnginePolicy(
        name="",
        dialect="",
        guarded_writes=False,
        checkpoint_retarget=False,
        target_kind="none",
    ),
    "sqlite": EnginePolicy(
        name="sqlite",
        dialect="sqlite",
        guarded_writes=False,
        checkpoint_retarget=False,
        target_kind="none",
    ),
    "postgres": EnginePolicy(
        name="postgres",
        dialect="postgres",
        guarded_writes=False,
        checkpoint_retarget=False,
        target_kind="none",
        nonsemantic_sql_prefixes=("SET ",),
    ),
    "mysql": EnginePolicy(
        name="mysql",
        dialect="mysql",
        guarded_writes=True,
        checkpoint_retarget=False,
        target_kind="database",
        nonsemantic_sql_prefixes=("SET ",),
        nonsemantic_command_prefixes=("LOCK TABLES", "UNLOCK TABLES"),
    ),
    "clickhouse": EnginePolicy(
        name="clickhouse",
        dialect="clickhouse",
        guarded_writes=True,
        checkpoint_retarget=True,
        target_kind="database",
        nonsemantic_sql_prefixes=("SET ",),
    ),
    "bigquery": EnginePolicy(
        name="bigquery",
        dialect="bigquery",
        guarded_writes=True,
        checkpoint_retarget=True,
        target_kind="bigquery",
    ),
    "bigquery-emulator": EnginePolicy(
        name="bigquery-emulator",
        dialect="bigquery",
        guarded_writes=True,
        checkpoint_retarget=True,
        target_kind="bigquery",
    ),
}


def normalize_engine(engine: str | None) -> str:
    if not engine:
        return ""
    lowered = engine.lower()
    if lowered == "postgresql":
        return "postgres"
    if lowered == "bigquery_emulator":
        return "bigquery-emulator"
    return lowered


def policy_for_engine(engine: str | None) -> EnginePolicy:
    normalized = normalize_engine(engine)
    return _POLICIES.get(
        normalized,
        EnginePolicy(
            name=normalized,
            dialect=normalized,
            guarded_writes=False,
            checkpoint_retarget=False,
            target_kind="none",
        ),
    )


def is_bigquery_family(engine: str | None) -> bool:
    return normalize_engine(engine) in BIGQUERY_FAMILY


__all__ = [
    "BIGQUERY_FAMILY",
    "EnginePolicy",
    "TargetKind",
    "is_bigquery_family",
    "normalize_engine",
    "policy_for_engine",
]
