from __future__ import annotations

import stat
from pathlib import Path

import pytest

from matey.dbmate import CmdResult, Dbmate, DbmateConfigError


def _make_dbmate(tmp_path: Path) -> Dbmate:
    migrations_dir = tmp_path / "db" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    dbmate_bin = tmp_path / "bin" / "dbmate"
    dbmate_bin.parent.mkdir(parents=True, exist_ok=True)
    dbmate_bin.write_text("#!/bin/sh\n", encoding="utf-8")
    return Dbmate(migrations_dir=migrations_dir, dbmate_bin=dbmate_bin)


def test_up_injects_no_dump_schema(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")
    captured: dict[str, object] = {}

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        captured["argv"] = argv
        return CmdResult(argv=argv, exit_code=0, stdout="", stderr="")

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    result = db.up()

    assert isinstance(result, CmdResult)
    argv = captured["argv"]
    assert isinstance(argv, tuple)
    assert "--no-dump-schema" in argv
    assert argv[-1] == "up"


def test_status_returns_raw_result(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        return CmdResult(
            argv=argv,
            exit_code=0,
            stdout="[X] 202601010101_init.sql\n[ ] 202601010102_next.sql\nApplied: 1\n",
            stderr="",
        )

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    result = db.status()
    assert isinstance(result, CmdResult)
    assert result.exit_code == 0
    assert "Applied: 1" in result.stdout


def test_dump_reads_temp_schema_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        schema_arg = argv.index("--schema-file")
        schema_path = Path(argv[schema_arg + 1])
        schema_path.write_text("CREATE TABLE widget(id INTEGER);\n", encoding="utf-8")
        return CmdResult(argv=argv, exit_code=0, stdout="", stderr="")

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    result = db.dump()

    assert isinstance(result, CmdResult)
    assert result.stdout == "CREATE TABLE widget(id INTEGER);\n"


def test_load_writes_sql_to_temp_schema_file(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")
    observed: dict[str, object] = {}

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        schema_arg = argv.index("--schema-file")
        schema_path = Path(argv[schema_arg + 1])
        observed["sql"] = schema_path.read_text(encoding="utf-8")
        observed["argv"] = argv
        return CmdResult(argv=argv, exit_code=0, stdout="", stderr="")

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    result = db.load("CREATE TABLE t(id INTEGER);\n")

    assert isinstance(result, CmdResult)
    assert observed["sql"] == "CREATE TABLE t(id INTEGER);\n"
    argv = observed["argv"]
    assert isinstance(argv, tuple)
    assert "--no-dump-schema" in argv
    assert argv[-1] == "load"


def test_rollback_steps_must_be_positive(tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    with pytest.raises(ValueError):
        db.rollback(0)


def test_wait_timeout_must_be_positive(tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    with pytest.raises(ValueError):
        db.wait(0)


def test_raw_passes_suffix_verbatim(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")
    captured: dict[str, object] = {}

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        captured["argv"] = argv
        return CmdResult(argv=argv, exit_code=0, stdout="", stderr="")

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    result = db.raw("status", "--quiet")

    assert isinstance(result, CmdResult)
    argv = captured["argv"]
    assert isinstance(argv, tuple)
    assert argv[-2:] == ("status", "--quiet")


def test_constructor_raises_for_missing_binary(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "db" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    missing = tmp_path / "missing-dbmate"

    with pytest.raises(DbmateConfigError):
        Dbmate(migrations_dir=migrations_dir, dbmate_bin=missing)


def test_constructor_raises_for_missing_migrations_dir(tmp_path: Path) -> None:
    missing_dir = tmp_path / "db" / "migrations"
    dbmate_bin = tmp_path / "bin" / "dbmate"
    dbmate_bin.parent.mkdir(parents=True, exist_ok=True)
    dbmate_bin.write_text("#!/bin/sh\n", encoding="utf-8")

    with pytest.raises(DbmateConfigError):
        Dbmate(migrations_dir=missing_dir, dbmate_bin=dbmate_bin)


def test_constructor_raises_for_non_directory_migrations_dir(tmp_path: Path) -> None:
    migrations_file = tmp_path / "db" / "migrations"
    migrations_file.parent.mkdir(parents=True, exist_ok=True)
    migrations_file.write_text("not a dir", encoding="utf-8")
    dbmate_bin = tmp_path / "bin" / "dbmate"
    dbmate_bin.parent.mkdir(parents=True, exist_ok=True)
    dbmate_bin.write_text("#!/bin/sh\n", encoding="utf-8")

    with pytest.raises(DbmateConfigError):
        Dbmate(migrations_dir=migrations_file, dbmate_bin=dbmate_bin)


def test_run_uses_explicit_env_without_ambient_inheritance(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    script = tmp_path / "print_env.sh"
    script.write_text(
        "#!/bin/sh\n"
        'printf "ambient=%s\\n" "${MATEY_TEST_AMBIENT}"\n'
        'printf "explicit=%s\\n" "${MATEY_TEST_EXPLICIT}"\n',
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)

    migrations_dir = tmp_path / "db" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    dbmate_bin = tmp_path / "bin" / "dbmate"
    dbmate_bin.parent.mkdir(parents=True, exist_ok=True)
    dbmate_bin.write_text("#!/bin/sh\n", encoding="utf-8")

    monkeypatch.setenv("MATEY_TEST_AMBIENT", "ambient-value")
    dbmate = Dbmate(
        migrations_dir=migrations_dir,
        dbmate_bin=dbmate_bin,
        env={"MATEY_TEST_EXPLICIT": "explicit-value"},
    )

    result = dbmate._run((str(script),))

    assert result.exit_code == 0
    assert "ambient=" in result.stdout
    assert "ambient=ambient-value" not in result.stdout
    assert "explicit=explicit-value" in result.stdout
