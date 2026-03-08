from __future__ import annotations

import os
import platform
import tempfile
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from plumbum import local

from matey.bqemu import is_bigquery_emulator_url, parse_bigquery_emulator_url
from matey.db_urls import dbmate_target
from matey.paths import PathBoundaryError, describe_path_boundary_error, ensure_non_symlink_path
from matey.sql import decode_sql_text

if TYPE_CHECKING:
    from collections.abc import Sequence

    from google.cloud.bigquery import SchemaField


@dataclass(frozen=True, slots=True)
class CmdResult:
    argv: tuple[str, ...]
    exit_code: int
    stdout: str
    stderr: str


class DbmateError(RuntimeError):
    pass


class DbmateConfigError(DbmateError):
    pass


def _platform_tag() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    goos_map = {
        "linux": "linux",
        "darwin": "darwin",
        "windows": "windows",
    }
    arch_map = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }
    return f"{goos_map.get(system, system)}-{arch_map.get(machine, machine)}"


def default_dbmate_binary() -> Path:
    binary_name = "dbmate.exe" if platform.system().lower() == "windows" else "dbmate"
    return Path(__file__).resolve().parent / "_vendor" / "dbmate" / _platform_tag() / binary_name


def passthrough(
    *args: str,
    dbmate_bin: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> CmdResult:
    # Raw passthrough inherits the caller environment unless an explicit env mapping is supplied.
    binary = Dbmate._resolve_dbmate_binary(dbmate_bin=dbmate_bin)
    argv = (str(binary), *args)
    cmd = local[argv[0]][argv[1:]]
    run_env = dict(os.environ) if env is None else dict(env)
    exit_code, stdout, stderr = cmd.run(retcode=None, env=run_env)
    return CmdResult(
        argv=argv,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
    )


class Dbmate:
    def __init__(
        self,
        *,
        migrations_dir: Path,
        dbmate_bin: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        self._migrations_dir = self._resolve_migrations_dir(migrations_dir)
        # Internal matey dbmate execution is hermetic by default; only explicit overrides are applied.
        self._env = dict(env) if env is not None else {}
        self._dbmate_bin = self._resolve_dbmate_binary(
            dbmate_bin=dbmate_bin,
        )

    @property
    def dbmate_bin(self) -> Path:
        return self._dbmate_bin

    @property
    def migrations_dir(self) -> Path:
        return self._migrations_dir

    @classmethod
    def _resolve_migrations_dir(cls, migrations_dir: Path) -> Path:
        try:
            resolved = ensure_non_symlink_path(
                migrations_dir,
                label="migrations_dir",
                allow_missing_leaf=True,
                expected_kind="dir",
            )
        except PathBoundaryError as error:
            raise DbmateConfigError(describe_path_boundary_error(error)) from error
        if not resolved.exists():
            raise DbmateConfigError(f"migrations_dir does not exist: {resolved}")
        if not resolved.is_dir():
            raise DbmateConfigError(f"migrations_dir is not a directory: {resolved}")
        return resolved

    @classmethod
    def _resolve_dbmate_binary(
        cls,
        *,
        dbmate_bin: Path | None,
    ) -> Path:
        candidate = (dbmate_bin if dbmate_bin is not None else default_dbmate_binary()).resolve()
        if not candidate.exists():
            raise DbmateConfigError(f"dbmate binary not found: {candidate}")
        if not candidate.is_file():
            raise DbmateConfigError(f"dbmate binary path is not a file: {candidate}")
        if not os.access(candidate, os.X_OK):
            raise DbmateConfigError(f"dbmate binary is not executable: {candidate}")
        return candidate

    def new(self, name: str) -> CmdResult:
        argv = (
            str(self._dbmate_bin),
            "--migrations-dir",
            str(self._migrations_dir),
            "new",
            name,
        )
        return self._run(argv)

    def database(self, url: str) -> DbConnection:
        return DbConnection(dbmate=self, url=url)

    def _base_args(self, url: str) -> tuple[str, ...]:
        dbmate_url = dbmate_target(url)
        return ("--url", dbmate_url, "--migrations-dir", str(self._migrations_dir))

    def _run_url_verb(
        self,
        *,
        url: str,
        verb: str,
        no_dump_schema: bool,
        extra_args: tuple[str, ...] = (),
        global_args: tuple[str, ...] = (),
    ) -> CmdResult:
        argv: list[str] = [
            str(self._dbmate_bin),
            *self._base_args(url),
            *global_args,
        ]
        if no_dump_schema:
            argv.append("--no-dump-schema")
        argv.append(verb)
        argv.extend(extra_args)
        return self._run(tuple(argv))

    def _run(self, argv: tuple[str, ...]) -> CmdResult:
        cmd = local[argv[0]][argv[1:]]
        exit_code, stdout, stderr = cmd.run(retcode=None, env=self._env)
        return CmdResult(
            argv=argv,
            exit_code=exit_code,
            stdout=stdout,
            stderr=stderr,
        )

    @contextmanager
    def _temp_schema_file(self, *, initial_text: str | None = None) -> Iterator[Path]:
        with tempfile.TemporaryDirectory(prefix="matey-dbmate-schema-") as temp_dir:
            schema_path = Path(temp_dir) / "schema.sql"
            if initial_text is not None:
                schema_path.write_text(initial_text, encoding="utf-8")
            yield schema_path


@dataclass(frozen=True, slots=True)
class DbConnection:
    dbmate: Dbmate
    url: str

    def wait(self, timeout_seconds: int) -> CmdResult:
        if timeout_seconds <= 0:
            raise DbmateError("wait timeout_seconds must be greater than zero.")
        argv = (
            str(self.dbmate.dbmate_bin),
            "--url",
            self.url,
            "--wait-timeout",
            f"{timeout_seconds}s",
            "wait",
        )
        return self.dbmate._run(argv)

    def create(self) -> CmdResult:
        return self.dbmate._run_url_verb(
            url=self.url,
            verb="create",
            no_dump_schema=True,
        )

    def drop(self) -> CmdResult:
        return self.dbmate._run_url_verb(
            url=self.url,
            verb="drop",
            no_dump_schema=True,
        )

    def up(self) -> CmdResult:
        return self.dbmate._run_url_verb(
            url=self.url,
            verb="up",
            no_dump_schema=True,
        )

    def migrate(self) -> CmdResult:
        return self.dbmate._run_url_verb(
            url=self.url,
            verb="migrate",
            no_dump_schema=True,
        )

    def rollback(self, steps: int = 1) -> CmdResult:
        if steps <= 0:
            raise DbmateError("rollback steps must be greater than zero.")
        return self.dbmate._run_url_verb(
            url=self.url,
            verb="rollback",
            no_dump_schema=True,
            extra_args=(str(steps),),
        )

    def status(self) -> CmdResult:
        return self.dbmate._run_url_verb(
            url=self.url,
            verb="status",
            no_dump_schema=True,
        )

    def dump(self) -> CmdResult:
        if is_bigquery_emulator_url(self.url):
            return _dump_bigquery_emulator(self)
        with self.dbmate._temp_schema_file() as schema_path:
            result = self.dbmate._run_url_verb(
                url=self.url,
                verb="dump",
                no_dump_schema=False,
                global_args=("--schema-file", str(schema_path)),
            )
            if result.exit_code != 0:
                return result
            if not schema_path.exists():
                raise DbmateError("dbmate dump completed without producing a schema file.")
            dump_text = decode_sql_text(
                schema_path.read_bytes(),
                label="dbmate dump schema file",
            )
            return CmdResult(
                argv=result.argv,
                exit_code=result.exit_code,
                stdout=dump_text,
                stderr=result.stderr,
            )

    def load(self, schema_sql: str) -> CmdResult:
        with self.dbmate._temp_schema_file(initial_text=schema_sql) as schema_path:
            return self.dbmate._run_url_verb(
                url=self.url,
                verb="load",
                no_dump_schema=True,
                global_args=("--schema-file", str(schema_path)),
            )


def _dump_bigquery_emulator(conn: DbConnection) -> CmdResult:
    dbmate_url = dbmate_target(conn.url)
    argv = (
        str(conn.dbmate.dbmate_bin),
        "--url",
        dbmate_url,
        "--migrations-dir",
        str(conn.dbmate.migrations_dir),
        "dump",
    )
    try:
        schema_sql = _bigquery_emulator_dump_sql(conn.url)
    except Exception as error:
        return CmdResult(argv=argv, exit_code=2, stdout="", stderr=f"Error: {error}")
    return CmdResult(argv=argv, exit_code=0, stdout=schema_sql, stderr="")


def _bigquery_emulator_dump_sql(url: str) -> str:
    # Compatibility shim: dbmate's real BigQuery dump path does not work against
    # the emulator, so this path emits a table-only schema dump. Views are still
    # rejected here because the emulator metadata surfaces currently expose
    # rewritten/internal SQL rather than a stable source definition.
    from google.auth.credentials import AnonymousCredentials
    from google.cloud import bigquery

    hostport, project, _location, dataset = parse_bigquery_emulator_url(url)
    client = bigquery.Client(
        project=project,
        credentials=AnonymousCredentials(),
        client_options={"api_endpoint": f"http://{hostport}"},
    )

    lines: list[str] = []
    table_items = sorted(client.list_tables(dataset), key=lambda item: item.table_id)
    table_names = {item.table_id for item in table_items}
    migrations_table = None
    for item in table_items:
        table = client.get_table(item.reference)
        if table.table_type == "VIEW":
            raise RuntimeError(
                "bigquery-emulator view dumping is unsupported: the emulator does not expose "
                "a stable source view definition for schema artifacts."
            )
        if table.table_type != "TABLE":
            continue
        if table.table_id == "schema_migrations":
            migrations_table = table
            continue
        lines.append(_render_bigquery_emulator_table_ddl(table.table_id, table.schema))

    versions: list[str] = []
    if "schema_migrations" in table_names:
        rows = client.query(
            f"SELECT version FROM `{project}.{dataset}.schema_migrations` ORDER BY version ASC"
        ).result()
        versions = [str(row["version"]) for row in rows]

    if versions and migrations_table is not None:
        lines.append(
            _render_bigquery_emulator_table_ddl("schema_migrations", migrations_table.schema)
        )
        values = ",\n    ".join(f"('{version}')" for version in versions)
        lines.append(f"INSERT INTO schema_migrations (version) VALUES\n    {values};")

    output = "\n\n".join(lines).strip()
    return f"{output}\n" if output else ""


def _render_bigquery_emulator_table_ddl(
    table_name: str,
    schema: Sequence[SchemaField],
) -> str:
    fields = ", ".join(
        f"{field.name} {_render_bigquery_emulator_field_type(field)}"
        + (" NOT NULL" if getattr(field, "mode", "").upper() == "REQUIRED" else "")
        for field in schema
    )
    return f"CREATE TABLE {table_name} ({fields});"


def _render_bigquery_emulator_field_type(field: SchemaField) -> str:
    field_type = str(getattr(field, "field_type", "")).upper()
    inner = _BIGQUERY_EMULATOR_TYPE_MAP.get(field_type, field_type)
    if field_type in {"RECORD", "STRUCT"}:
        subfields = ", ".join(
            f"{subfield.name} {_render_bigquery_emulator_field_type(subfield)}"
            + (" NOT NULL" if getattr(subfield, "mode", "").upper() == "REQUIRED" else "")
            for subfield in getattr(field, "fields", ())
        )
        inner = f"STRUCT<{subfields}>"
    if str(getattr(field, "mode", "")).upper() == "REPEATED":
        return f"ARRAY<{inner}>"
    return inner


_BIGQUERY_EMULATOR_TYPE_MAP = {
    "INTEGER": "INT64",
    "INT64": "INT64",
    "FLOAT": "FLOAT64",
    "FLOAT64": "FLOAT64",
    "BOOLEAN": "BOOL",
    "BOOL": "BOOL",
    "STRING": "STRING",
    "BYTES": "BYTES",
    "DATE": "DATE",
    "DATETIME": "DATETIME",
    "TIME": "TIME",
    "TIMESTAMP": "TIMESTAMP",
    "NUMERIC": "NUMERIC",
    "BIGNUMERIC": "BIGNUMERIC",
    "GEOGRAPHY": "GEOGRAPHY",
    "JSON": "JSON",
    "RECORD": "STRUCT",
    "STRUCT": "STRUCT",
}


__all__ = [
    "CmdResult",
    "DbConnection",
    "Dbmate",
    "DbmateConfigError",
    "DbmateError",
    "default_dbmate_binary",
    "passthrough",
]
