from __future__ import annotations

from pathlib import Path

from matey.dbmate import DbmateGateway
from matey.models import CmdResult


class _Runner:
    def __init__(self) -> None:
        self.calls: list[tuple[tuple[str, ...], Path | None]] = []

    def run(self, argv: tuple[str, ...], cwd: Path | None = None) -> CmdResult:
        self.calls.append((argv, cwd))
        return CmdResult(argv=argv, exit_code=0, stdout="", stderr="")


class _Env:
    def get(self, key: str, default: str | None = None) -> str | None:
        del key
        return default

    def require(self, key: str) -> str:
        raise KeyError(key)


def test_status_uses_no_dump_and_target_cwd(tmp_path: Path) -> None:
    runner = _Runner()
    gateway = DbmateGateway(
        runner=runner,
        env=_Env(),
        dbmate_binary=Path("/tmp/dbmate"),
    )
    migrations_dir = tmp_path / "db" / "core" / "migrations"
    migrations_dir.mkdir(parents=True)

    gateway.status("sqlite3:/tmp/live.db", migrations_dir)

    argv, cwd = runner.calls[-1]
    assert cwd == migrations_dir.parent
    assert "--no-dump-schema" in argv
    assert argv[-1] == "status"


def test_dump_uses_target_cwd(tmp_path: Path) -> None:
    runner = _Runner()
    gateway = DbmateGateway(
        runner=runner,
        env=_Env(),
        dbmate_binary=Path("/tmp/dbmate"),
    )
    migrations_dir = tmp_path / "db" / "core" / "migrations"
    migrations_dir.mkdir(parents=True)

    gateway.dump("sqlite3:/tmp/live.db", migrations_dir)

    argv, cwd = runner.calls[-1]
    assert cwd == migrations_dir.parent
    assert "--no-dump-schema" not in argv
    assert argv[-1] == "dump"
