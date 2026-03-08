from __future__ import annotations

from collections.abc import Iterable

from .io import resolve_order_by
from .model import DataError, DataFile


def validate_export_columns(*, data_file: DataFile, columns: Iterable[str]) -> tuple[str, ...]:
    known = set(columns)
    order_by = resolve_order_by(data_file)
    missing = [column for column in order_by if column not in known]
    if missing:
        rendered = ", ".join(repr(column) for column in missing)
        raise DataError(
            f"Data file {data_file.name!r} references missing export ordering column(s): {rendered}."
        )
    if data_file.on:
        missing_keys = [column for column in data_file.on if column not in known]
        if missing_keys:
            rendered = ", ".join(repr(column) for column in missing_keys)
            raise DataError(
                f"Data file {data_file.name!r} references missing upsert key column(s): {rendered}."
            )
    return order_by


def validate_apply_rows(
    *,
    data_file: DataFile,
    columns: Iterable[str],
    rows: list[dict[str, object]],
) -> None:
    known = set(columns)
    if data_file.on:
        missing_keys = [column for column in data_file.on if column not in known]
        if missing_keys:
            rendered = ", ".join(repr(column) for column in missing_keys)
            raise DataError(
                f"Data file {data_file.name!r} references missing upsert key column(s): {rendered}."
            )
    if data_file.order_by:
        missing_order = [column for column in data_file.order_by if column not in known]
        if missing_order:
            rendered = ", ".join(repr(column) for column in missing_order)
            raise DataError(
                f"Data file {data_file.name!r} references missing order_by column(s): {rendered}."
            )
    if not rows:
        return
    referenced = set().union(*(row.keys() for row in rows))
    unknown = sorted(referenced - known)
    if unknown:
        rendered = ", ".join(repr(column) for column in unknown)
        raise DataError(f"Data file {data_file.name!r} contains unknown column(s): {rendered}.")
