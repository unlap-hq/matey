from __future__ import annotations

import platform
from pathlib import Path

from matey.app.protocols import CmdResult, IDbmateGateway, IEnvProvider, IProcessRunner


def _platform_tag() -> str:
    system = platform.system().lower()
    machine = platform.machine().lower()
    if system == "linux":
        goos = "linux"
    elif system == "darwin":
        goos = "darwin"
    elif system == "windows":
        goos = "windows"
    else:
        goos = system

    arch_aliases = {
        "x86_64": "amd64",
        "amd64": "amd64",
        "aarch64": "arm64",
        "arm64": "arm64",
    }
    goarch = arch_aliases.get(machine, machine)
    return f"{goos}-{goarch}"


def default_dbmate_binary() -> Path:
    vendor_root = Path(__file__).resolve().parents[1] / "_vendor" / "dbmate"
    platform_dir = vendor_root / _platform_tag()
    binary_name = "dbmate.exe" if platform.system().lower() == "windows" else "dbmate"
    return platform_dir / binary_name


class DbmateGateway(IDbmateGateway):
    def __init__(
        self,
        *,
        runner: IProcessRunner,
        env: IEnvProvider,
        dbmate_binary: Path | None = None,
    ) -> None:
        self._runner = runner
        if dbmate_binary is not None:
            self._dbmate_binary = dbmate_binary
        else:
            configured = env.get("MATEY_DBMATE_BIN", "")
            if configured is not None and configured.strip():
                self._dbmate_binary = Path(configured.strip())
            else:
                self._dbmate_binary = default_dbmate_binary()

    @property
    def dbmate_binary(self) -> Path:
        return self._dbmate_binary

    def _run_dbmate(
        self,
        *,
        url: str,
        migrations_dir: Path,
        verb: str,
        no_dump_schema: bool = True,
        extra_args: tuple[str, ...] = (),
        global_args: tuple[str, ...] = (),
    ) -> CmdResult:
        argv: list[str] = [str(self._dbmate_binary)]
        argv.extend(["--url", url])
        argv.extend(["--migrations-dir", str(migrations_dir)])
        argv.extend(global_args)
        if no_dump_schema:
            argv.append("--no-dump-schema")
        argv.append(verb)
        argv.extend(extra_args)
        return self._runner.run(tuple(argv), cwd=migrations_dir.parent)

    def new(self, name: str, migrations_dir: Path) -> CmdResult:
        argv = (
            str(self._dbmate_binary),
            "--migrations-dir",
            str(migrations_dir),
            "new",
            name,
        )
        return self._runner.run(argv, cwd=migrations_dir.parent)

    def wait(self, url: str, timeout_seconds: int) -> CmdResult:
        return self._runner.run(
            (
                str(self._dbmate_binary),
                "--url",
                url,
                "--wait-timeout",
                f"{timeout_seconds}s",
                "wait",
            )
        )

    def create(self, url: str, migrations_dir: Path) -> CmdResult:
        return self._run_dbmate(url=url, migrations_dir=migrations_dir, verb="create")

    def drop(self, url: str, migrations_dir: Path) -> CmdResult:
        return self._run_dbmate(url=url, migrations_dir=migrations_dir, verb="drop")

    def up(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult:
        return self._run_dbmate(
            url=url,
            migrations_dir=migrations_dir,
            verb="up",
            no_dump_schema=no_dump_schema,
        )

    def migrate(self, url: str, migrations_dir: Path, no_dump_schema: bool = True) -> CmdResult:
        return self._run_dbmate(
            url=url,
            migrations_dir=migrations_dir,
            verb="migrate",
            no_dump_schema=no_dump_schema,
        )

    def rollback(
        self,
        url: str,
        migrations_dir: Path,
        steps: int,
        no_dump_schema: bool = True,
    ) -> CmdResult:
        return self._run_dbmate(
            url=url,
            migrations_dir=migrations_dir,
            verb="rollback",
            no_dump_schema=no_dump_schema,
            extra_args=(str(steps),),
        )

    def load_schema(
        self,
        url: str,
        schema_path: Path,
        migrations_dir: Path,
        no_dump_schema: bool = True,
    ) -> CmdResult:
        return self._run_dbmate(
            url=url,
            migrations_dir=migrations_dir,
            verb="load",
            no_dump_schema=no_dump_schema,
            global_args=("--schema-file", str(schema_path)),
        )

    def dump(self, url: str, migrations_dir: Path) -> CmdResult:
        return self._run_dbmate(
            url=url,
            migrations_dir=migrations_dir,
            verb="dump",
            no_dump_schema=False,
        )

    def status(self, url: str, migrations_dir: Path) -> CmdResult:
        return self._run_dbmate(
            url=url,
            migrations_dir=migrations_dir,
            verb="status",
            no_dump_schema=True,
        )

    def raw(self, argv_suffix: tuple[str, ...], url: str, migrations_dir: Path) -> CmdResult:
        argv = [str(self._dbmate_binary), "--url", url, "--migrations-dir", str(migrations_dir)]
        argv.extend(argv_suffix)
        return self._runner.run(tuple(argv), cwd=migrations_dir.parent)
