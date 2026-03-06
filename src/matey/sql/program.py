from __future__ import annotations

from functools import cached_property
from typing import Literal

from .ast import (
    SqlError,
    WriteViolation,
    anchor_statements,
    has_executable_sql,
    schema_fingerprint,
    section_write_violations,
)
from .source import split_migration_sections, unified_sql_diff


class SqlProgram:
    def __init__(self, text: str, *, engine: str) -> None:
        self._text = text
        self._engine = engine

    @property
    def engine(self) -> str:
        return self._engine

    @property
    def up_sql(self) -> str:
        return self._sections[0]

    @property
    def down_sql(self) -> str:
        return self._sections[1]

    @cached_property
    def _sections(self) -> tuple[str, str]:
        return split_migration_sections(self._text)

    def has_executable_down(self) -> bool:
        return has_executable_sql(self.down_sql, engine=self._engine)

    def section_write_violations(
        self,
        section: Literal["up", "down"],
    ) -> tuple[WriteViolation, ...]:
        sql = self.up_sql if section == "up" else self.down_sql
        if section == "down" and not has_executable_sql(sql, engine=self._engine):
            return ()
        return section_write_violations(sql, engine=self._engine, section=section)

    def migration_write_violations(self) -> tuple[WriteViolation, ...]:
        return self.section_write_violations("up") + self.section_write_violations("down")

    def schema_fingerprint(self, *, context_url: str | None = None) -> str:
        return schema_fingerprint(self._text, engine=self._engine, context_url=context_url)

    def schema_equals(
        self,
        other: SqlProgram,
        *,
        left_context_url: str | None = None,
        right_context_url: str | None = None,
    ) -> bool:
        return self.schema_fingerprint(context_url=left_context_url) == other.schema_fingerprint(
            context_url=right_context_url
        )

    def schema_diff(
        self,
        other: SqlProgram,
        *,
        left_label: str,
        right_label: str,
        left_context_url: str | None = None,
        right_context_url: str | None = None,
    ) -> str:
        return unified_sql_diff(
            left_sql=self.schema_fingerprint(context_url=left_context_url),
            right_sql=other.schema_fingerprint(context_url=right_context_url),
            left_label=left_label,
            right_label=right_label,
        )

    def anchor_statements(self, *, target_url: str) -> tuple[str, ...]:
        return anchor_statements(self._text, engine=self._engine, target_url=target_url)


__all__ = ["SqlError", "SqlProgram"]
