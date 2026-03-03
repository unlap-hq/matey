from __future__ import annotations

import platform
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from plumbum import local


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


class Dbmate:
    def __init__(
        self,
        *,
        migrations_dir: Path,
        dbmate_bin: Path | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        self._migrations_dir = self._resolve_migrations_dir(migrations_dir)
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
        resolved = migrations_dir.resolve()
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
        candidate = dbmate_bin if dbmate_bin is not None else default_dbmate_binary()
        if not candidate.exists():
            raise DbmateConfigError(f"dbmate binary not found: {candidate}")
        if not candidate.is_file():
            raise DbmateConfigError(f"dbmate binary path is not a file: {candidate}")
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
        return ("--url", url, "--migrations-dir", str(self._migrations_dir))

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
            raise ValueError("wait timeout_seconds must be greater than zero.")
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
            no_dump_schema=False,
        )

    def drop(self) -> CmdResult:
        return self.dbmate._run_url_verb(
            url=self.url,
            verb="drop",
            no_dump_schema=False,
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
            raise ValueError("rollback steps must be greater than zero.")
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
        with self.dbmate._temp_schema_file() as schema_path:
            result = self.dbmate._run_url_verb(
                url=self.url,
                verb="dump",
                no_dump_schema=False,
                global_args=("--schema-file", str(schema_path)),
            )
            dump_text = schema_path.read_text(encoding="utf-8") if schema_path.exists() else ""
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

    def raw(self, *args: str) -> CmdResult:
        argv = (
            str(self.dbmate.dbmate_bin),
            *self.dbmate._base_args(self.url),
            *args,
        )
        return self.dbmate._run(argv)


__all__ = [
    "CmdResult",
    "DbConnection",
    "Dbmate",
    "DbmateConfigError",
    "DbmateError",
    "default_dbmate_binary",
]
