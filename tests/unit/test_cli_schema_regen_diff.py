from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from typer.testing import CliRunner

from matey.cli import app
from matey.domain import LockfileError
from matey.workflows.schema_lock import SchemaReplayResult

runner = CliRunner()


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def test_schema_regen_writes_replay_schema_and_syncs_lock(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    schema_file = tmp_path / "db" / "schema.sql"
    _write(schema_file, "CREATE TABLE old_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=False,
            head_schema_sql="CREATE TABLE old_table (id INT);\n",
            expected_schema_sql="CREATE TABLE new_table (id INT);\n",
            diff_text="--- old\n+++ new\n",
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )
    monkeypatch.setattr(
        "matey.cli.commands.schema._atomic_write_schema_and_lock",
        lambda **kwargs: (
            _write(kwargs["paths"].schema_file, kwargs["schema_sql"]) or True,
            SimpleNamespace(engine="postgres", head_index=1),
        ),
    )

    result = runner.invoke(app, ["schema", "regen"], env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"})

    assert result.exit_code == 0
    assert "schema replay diff" in result.output
    assert schema_file.read_text(encoding="utf-8") == "CREATE TABLE new_table (id INT);\n"


def test_schema_regen_fails_when_lock_sync_fails(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    schema_file = tmp_path / "db" / "schema.sql"
    _write(schema_file, "CREATE TABLE old_table (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=True,
            head_schema_sql="CREATE TABLE old_table (id INT);\n",
            expected_schema_sql="CREATE TABLE old_table (id INT);\n",
            diff_text=None,
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )
    monkeypatch.setattr(
        "matey.cli.commands.schema._atomic_write_schema_and_lock",
        lambda **_: (_ for _ in ()).throw(LockfileError("sync boom")),
    )

    result = runner.invoke(app, ["schema", "regen"], env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"})

    assert result.exit_code == 1
    assert "sync boom" in result.output


def test_schema_diff_exits_nonzero_when_diff_present(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=False,
            head_schema_sql="CREATE TABLE t (id INT);\n",
            expected_schema_sql="CREATE TABLE u (id INT);\n",
            diff_text="--- old\n+++ new\n",
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )

    result = runner.invoke(app, ["schema", "diff"], env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"})

    assert result.exit_code == 1
    assert "schema replay diff" in result.output


def test_schema_diff_reports_no_differences(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))
    monkeypatch.setattr(
        "matey.cli.commands.schema.evaluate_schema_lock_target",
        lambda **_: SchemaReplayResult(
            target_name="default",
            success=True,
            head_schema_sql="CREATE TABLE t (id INT);\n",
            expected_schema_sql="CREATE TABLE t (id INT);\n",
            diff_text=None,
            scratch_url="postgres://u:p@localhost/scratch",
        ),
    )

    result = runner.invoke(app, ["schema", "diff"], env={"MATEY_TEST_URL": "postgres://u:p@localhost/test"})
    assert result.exit_code == 0
    assert "no schema differences found" in result.output


def test_schema_commands_reject_legacy_flags(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write(tmp_path / "db" / "schema.sql", "CREATE TABLE t (id INT);\n")
    monkeypatch.setattr("matey.cli.common.resolve_dbmate_binary", lambda _: Path("/tmp/dbmate"))

    result = runner.invoke(app, ["schema", "validate", "--schema-only"])
    assert result.exit_code != 0
    assert "No such option: --schema-only" in result.output

    result = runner.invoke(app, ["schema", "diff", "--live"])
    assert result.exit_code != 0
    assert "No such option: --live" in result.output

    result = runner.invoke(app, ["schema", "diff", "--expected", "repo"])
    assert result.exit_code != 0
    assert "No such option: --expected" in result.output
