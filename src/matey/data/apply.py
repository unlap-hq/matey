from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from google.cloud import bigquery

from matey.db import runtime
from matey.db_urls import IbisTarget, ibis_target
from matey.project import TargetConfig
from matey.scratch import engine_from_url as scratch_engine_from_url

from .io import load_data_sets, read_jsonl, select_data_set
from .model import DataApplyResult, DataError, DataFile, DataFileResult


def apply(
    *,
    target: TargetConfig,
    url: str | None,
    set_name: str | None,
) -> DataApplyResult:
    with _data_runtime(target=target, url=url, context="data apply") as rt:
        data_set = select_data_set(load_data_sets(target), set_name=set_name)
        engine = scratch_engine_from_url(rt.conn.url)
        handle = ibis_target(engine=engine, url=rt.conn.url)
        results = tuple(
            _apply_data_file(
                handle=handle,
                data_file=data_file,
            )
            for data_file in data_set.files
        )
        return DataApplyResult(target_name=target.name, set_name=data_set.name, files=results)


@contextmanager
def _data_runtime(
    *,
    target: TargetConfig,
    url: str | None,
    context: str,
) -> Iterator[runtime.RuntimeContext]:
    with runtime.open_runtime(target=target, url=url, dbmate_bin=None) as rt:
        live = runtime.inspect_live(rt, context=f"{context} status")
        runtime.ensure_live_not_ahead(
            state=rt.state,
            live=live,
            context=f"{context} status",
        )
        schema_match, _expected_sql, _live_sql = runtime.compare_expected_schema(
            runtime=rt,
            expected_index=len(rt.state.worktree_steps),
            context=context,
        )
        if schema_match is False:
            raise DataError(
                f"{context} requires the live database schema to match the current worktree head."
            )
        yield rt


def _apply_data_file(
    *,
    handle: IbisTarget,
    data_file: DataFile,
) -> DataFileResult:
    rows = read_jsonl(data_file.path)
    _apply_rows(
        handle=handle,
        data_file=data_file,
        rows=rows,
    )
    return DataFileResult(
        name=data_file.name,
        table=data_file.table,
        mode=data_file.mode,
        rows=len(rows),
    )


def _apply_rows(
    *,
    handle: IbisTarget,
    data_file: DataFile,
    rows: list[dict[str, object]],
) -> None:
    if handle.kind == "bigquery-emulator-client":
        _apply_bigquery_emulator_rows(
            client=handle.backend,
            database=handle.database,
            data_file=data_file,
            rows=rows,
        )
        return
    if data_file.mode == "replace":
        if rows:
            handle.backend.insert(data_file.table, rows, database=handle.database, overwrite=True)  # type: ignore[attr-defined]
        else:
            handle.backend.truncate_table(data_file.table, database=handle.database)  # type: ignore[attr-defined]
        return
    if data_file.mode == "insert":
        if rows:
            handle.backend.insert(data_file.table, rows, database=handle.database, overwrite=False)  # type: ignore[attr-defined]
        return
    if data_file.mode == "upsert":
        if rows:
            handle.backend.upsert(data_file.table, rows, on=data_file.on, database=handle.database)  # type: ignore[attr-defined]
        return
    raise DataError(f"Unsupported data mode {data_file.mode!r} for {data_file.name}.")


def _apply_bigquery_emulator_rows(
    *,
    client: bigquery.Client,
    database: tuple[str, str] | None,
    data_file: DataFile,
    rows: list[dict[str, object]],
) -> None:
    if database is None:
        raise DataError("BigQuery emulator data apply requires a project/dataset.")
    project, dataset = database
    table_ref = f"{project}.{dataset}.{data_file.table}"
    if data_file.mode == "replace":
        client.query(f"TRUNCATE TABLE `{table_ref}`").result()
        if rows:
            errors = client.insert_rows_json(table_ref, rows)
            if errors:
                raise DataError(f"BigQuery emulator insert failed for {data_file.table}: {errors}")
        return
    if data_file.mode == "insert":
        if rows:
            errors = client.insert_rows_json(table_ref, rows)
            if errors:
                raise DataError(f"BigQuery emulator insert failed for {data_file.table}: {errors}")
        return
    if data_file.mode == "upsert":
        if not data_file.on:
            raise DataError(f"Data file {data_file.name!r} with mode='upsert' requires on.")
        if rows:
            values = ", ".join(json_value(row.get(data_file.on)) for row in rows)
            client.query(f"DELETE FROM `{table_ref}` WHERE {data_file.on} IN ({values})").result()
            errors = client.insert_rows_json(table_ref, rows)
            if errors:
                raise DataError(
                    f"BigQuery emulator upsert insert failed for {data_file.table}: {errors}"
                )
        return
    raise DataError(f"Unsupported data mode {data_file.mode!r} for {data_file.name}.")


def json_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value).replace("'", "''")
    return f"'{text}'"
