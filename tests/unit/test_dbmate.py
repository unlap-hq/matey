from __future__ import annotations

import stat
from pathlib import Path

import pytest

from matey.bqemu import to_dbmate_bigquery_url
from matey.dbmate import CmdResult, Dbmate, DbmateConfigError, DbmateError, passthrough
from matey.sql import SqlTextDecodeError


def _make_dbmate(tmp_path: Path) -> Dbmate:
    migrations_dir = tmp_path / "db" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    dbmate_bin = tmp_path / "bin" / "dbmate"
    dbmate_bin.parent.mkdir(parents=True, exist_ok=True)
    dbmate_bin.write_text("#!/bin/sh\n", encoding="utf-8")
    dbmate_bin.chmod(dbmate_bin.stat().st_mode | stat.S_IEXEC)
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


def test_dump_requires_schema_file_output(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        return CmdResult(argv=argv, exit_code=0, stdout="", stderr="")

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    with pytest.raises(DbmateError, match="completed without producing a schema file"):
        db.dump()


def test_dump_propagates_invalid_utf8_schema_file(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        schema_arg = argv.index("--schema-file")
        schema_path = Path(argv[schema_arg + 1])
        schema_path.write_bytes(b"\xff\xfe\x00")
        return CmdResult(argv=argv, exit_code=0, stdout="", stderr="")

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    with pytest.raises(SqlTextDecodeError, match="Unable to decode dbmate dump schema file as UTF-8"):
        db.dump()


def test_dump_nonzero_exit_does_not_mask_dbmate_failure_with_decode_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    def fake_run(self: Dbmate, argv: tuple[str, ...]) -> CmdResult:
        schema_arg = argv.index("--schema-file")
        schema_path = Path(argv[schema_arg + 1])
        schema_path.write_bytes(b"\xff\xfe\x00")
        return CmdResult(argv=argv, exit_code=1, stdout="", stderr="real failure")

    monkeypatch.setattr(Dbmate, "_run", fake_run)

    result = db.dump()

    assert result.exit_code == 1
    assert result.stderr == "real failure"
    assert result.stdout == ""


def test_load_writes_sql_to_temp_schema_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
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

    with pytest.raises(DbmateError, match="rollback steps must be greater than zero"):
        db.rollback(0)


def test_wait_timeout_must_be_positive(tmp_path: Path) -> None:
    dbmate = _make_dbmate(tmp_path)
    db = dbmate.database("postgres://db")

    with pytest.raises(DbmateError, match="wait timeout_seconds must be greater than zero"):
        db.wait(0)


def test_bigquery_emulator_url_translates_to_dbmate_bigquery_dsn() -> None:
    assert to_dbmate_bigquery_url(
        "bigquery-emulator://127.0.0.1:9050/matey/us/scratch_ds"
    ) == (
        "bigquery://matey/us/scratch_ds?"
        "disable_auth=true&endpoint=http%3A%2F%2F127.0.0.1%3A9050"
    )


def test_bigquery_emulator_url_preserves_existing_query_params() -> None:
    assert to_dbmate_bigquery_url(
        "bigquery-emulator://127.0.0.1:9050/matey/scratch_ds?foo=bar"
    ) == (
        "bigquery://matey/scratch_ds?"
        "foo=bar&disable_auth=true&endpoint=http%3A%2F%2F127.0.0.1%3A9050"
    )


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
    dbmate_bin.chmod(dbmate_bin.stat().st_mode | stat.S_IEXEC)

    with pytest.raises(DbmateConfigError):
        Dbmate(migrations_dir=missing_dir, dbmate_bin=dbmate_bin)


def test_constructor_raises_for_non_directory_migrations_dir(tmp_path: Path) -> None:
    migrations_file = tmp_path / "db" / "migrations"
    migrations_file.parent.mkdir(parents=True, exist_ok=True)
    migrations_file.write_text("not a dir", encoding="utf-8")
    dbmate_bin = tmp_path / "bin" / "dbmate"
    dbmate_bin.parent.mkdir(parents=True, exist_ok=True)
    dbmate_bin.write_text("#!/bin/sh\n", encoding="utf-8")
    dbmate_bin.chmod(dbmate_bin.stat().st_mode | stat.S_IEXEC)

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
    dbmate_bin.chmod(dbmate_bin.stat().st_mode | stat.S_IEXEC)

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


def test_constructor_raises_for_non_executable_binary(tmp_path: Path) -> None:
    migrations_dir = tmp_path / "db" / "migrations"
    migrations_dir.mkdir(parents=True, exist_ok=True)
    dbmate_bin = tmp_path / "bin" / "dbmate"
    dbmate_bin.parent.mkdir(parents=True, exist_ok=True)
    dbmate_bin.write_text("#!/bin/sh\n", encoding="utf-8")
    dbmate_bin.chmod(stat.S_IRUSR | stat.S_IWUSR)

    with pytest.raises(DbmateConfigError, match="not executable"):
        Dbmate(migrations_dir=migrations_dir, dbmate_bin=dbmate_bin)


def test_passthrough_runs_verbatim_and_inherits_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    script = tmp_path / "dbmate"
    script.write_text(
        "#!/bin/sh\n"
        'printf "args:%s\\n" "$*"\n'
        'printf "env:%s\\n" "${MATEY_DBMATE_TEST_ENV}"\n',
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC)
    monkeypatch.setenv("MATEY_DBMATE_TEST_ENV", "ok")

    result = passthrough("status", "--wait", dbmate_bin=script)

    assert result.exit_code == 0
    assert "args:status --wait" in result.stdout
    assert "env:ok" in result.stdout
