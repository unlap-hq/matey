from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

SqlOrigin = Literal["artifact", "scratch_dump", "live_dump"]


@dataclass(frozen=True)
class SqlSource:
    text: str
    origin: SqlOrigin
    context_url: str | None = None


@dataclass(frozen=True)
class PreparedSql:
    normalized: str
    digest: str


@dataclass(frozen=True)
class SqlComparison:
    expected: PreparedSql
    actual: PreparedSql
    equal: bool
    diff: str | None
