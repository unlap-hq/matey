from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from matey.domain.sql import SqlComparison


@dataclass(frozen=True)
class SchemaStatusRow:
    marker: Literal["ok", "warn", "error"]
    migration_file: str
    status: str
    detail: str


@dataclass(frozen=True)
class SchemaStatusResult:
    up_to_date: bool
    stale: bool
    rows: tuple[SchemaStatusRow, ...]
    summary: tuple[str, ...]


@dataclass(frozen=True)
class SchemaPlanResult:
    comparison: SqlComparison
    replay_scratch_url: str
    down_checked: bool
    orphan_checkpoints: tuple[str, ...]


@dataclass(frozen=True)
class DbPlanResult:
    comparison: SqlComparison
    live_applied_index: int
