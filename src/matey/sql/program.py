from __future__ import annotations

from collections.abc import Iterable
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
from .source import SqlTextDecodeError, decode_sql_text, split_migration_sections, unified_sql_diff


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


class MigrationSqlError(SqlError):
    def __init__(self, *, migration_file: str, detail: str) -> None:
        self.migration_file = migration_file
        super().__init__(detail)


def describe_write_violation(
    *,
    migration_file: str,
    engine: str,
    violation: WriteViolation,
    context: str | None = None,
) -> str:
    reason = (
        f"qualified {engine} write target"
        if violation.reason == "qualified write target"
        else f"unsupported {engine} mutating syntax"
    )
    prefix = f"{context} failed: " if context else ""
    return (
        f"{prefix}{migration_file} {violation.section} contains a {reason} "
        f"{violation.target!r}. Use unqualified target-local names or split this into "
        f"another matey target. Statement: {violation.excerpt()!r}"
    )


def first_write_violation_message(
    *,
    sql_text: str,
    engine: str,
    migration_file: str,
    section: Literal["up", "down", "migration"],
    context: str | None = None,
) -> str | None:
    program = SqlProgram(sql_text, engine=engine)
    violations = (
        program.migration_write_violations()
        if section == "migration"
        else program.section_write_violations(section)
    )
    if not violations:
        return None
    return describe_write_violation(
        migration_file=migration_file,
        engine=engine,
        violation=violations[0],
        context=context,
    )


def first_migration_violation_message(
    *,
    entries: Iterable[tuple[str, bytes]],
    engine: str,
    section: Literal["up", "down", "migration"],
    context: str | None = None,
) -> str | None:
    for migration_file, payload in entries:
        try:
            sql_text = decode_sql_text(payload, label=f"migration {migration_file}")
            message = first_write_violation_message(
                sql_text=sql_text,
                engine=engine,
                migration_file=migration_file,
                section=section,
                context=context,
            )
        except SqlTextDecodeError as error:
            raise MigrationSqlError(
                migration_file=migration_file,
                detail=str(error),
            ) from error
        except SqlError as error:
            raise MigrationSqlError(
                migration_file=migration_file,
                detail=f"{migration_file} SQL analysis failed: {error}",
            ) from error
        if message is not None:
            return message
    return None


__all__ = [
    "MigrationSqlError",
    "SqlError",
    "SqlProgram",
    "SqlTextDecodeError",
    "describe_write_violation",
    "first_migration_violation_message",
]
