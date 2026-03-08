from __future__ import annotations

from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery

from matey import Engine
from matey.bqemu import parse_bigquery_emulator_url
from matey.db_urls import ibis_target
from matey.project import TargetConfig
from matey.scratch import engine_from_url as scratch_engine_from_url

from .apply import _data_runtime
from .io import load_data_sets, select_data_set, write_jsonl
from .model import DataExportResult, DataFile, DataFileResult
from .validate import validate_export_columns


def export(
    *,
    target: TargetConfig,
    url: str | None,
    set_name: str | None,
) -> DataExportResult:
    with _data_runtime(target=target, url=url, context="data export") as rt:
        data_set = select_data_set(load_data_sets(target), set_name=set_name)
        engine = scratch_engine_from_url(rt.conn.url)
        results = tuple(
            _export_data_file(
                engine=engine,
                url=rt.conn.url,
                data_file=data_file,
            )
            for data_file in data_set.files
        )
        return DataExportResult(target_name=target.name, set_name=data_set.name, files=results)


def _export_data_file(
    *,
    engine: Engine,
    url: str,
    data_file: DataFile,
) -> DataFileResult:
    if engine is Engine.BIGQUERY_EMULATOR:
        rows = _export_bigquery_emulator_rows(url=url, data_file=data_file)
    else:
        handle = ibis_target(engine=engine, url=url)
        table = handle.backend.table(data_file.table, database=handle.database)  # type: ignore[attr-defined]
        order_by = validate_export_columns(data_file=data_file, columns=table.schema().names)
        expression = table.order_by([table[column] for column in order_by])
        rows = handle.backend.execute(expression).to_dict("records")  # type: ignore[attr-defined]
    row_count = write_jsonl(data_file.path, rows)
    return DataFileResult(
        name=data_file.name,
        table=data_file.table,
        mode=data_file.mode,
        rows=row_count,
    )


def _export_bigquery_emulator_rows(*, url: str, data_file: DataFile) -> list[dict[str, object]]:
    hostport, project, _location, dataset = parse_bigquery_emulator_url(url)
    client = bigquery.Client(
        project=project,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": f"http://{hostport}"},
    )
    table = client.get_table(f"{project}.{dataset}.{data_file.table}")
    order_by = validate_export_columns(
        data_file=data_file,
        columns=tuple(field.name for field in table.schema),
    )
    rendered_order = ", ".join(f"`{column}`" for column in order_by)
    result = client.query(
        f"SELECT * FROM `{project}.{dataset}.{data_file.table}` ORDER BY {rendered_order}"
    ).result()
    return [dict(row.items()) for row in result]
