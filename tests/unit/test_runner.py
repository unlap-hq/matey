from __future__ import annotations

import subprocess
from pathlib import Path

from matey.drivers.dbmate import (
    DbmateLogContext,
    build_dbmate_argv,
    redact_url,
    run_dbmate,
    run_dbmate_capture,
)


def test_redact_url_masks_password() -> None:
    original = "postgres://alice:secret@db.example.com:5432/main"
    redacted = redact_url(original)
    assert redacted == "postgres://alice:***@db.example.com:5432/main"


def test_build_dbmate_argv() -> None:
    argv = build_dbmate_argv(
        dbmate_binary=Path("/bin/dbmate"),
        url="postgres://db",
        migrations_dir=Path("/repo/db/migrations"),
        schema_file=Path("/repo/db/schema.sql"),
        verb="up",
        extra_args=["--foo", "bar"],
    )
    assert argv == [
        "/bin/dbmate",
        "--url",
        "postgres://db",
        "--migrations-dir",
        "/repo/db/migrations",
        "--schema-file",
        "/repo/db/schema.sql",
        "up",
        "--foo",
        "bar",
    ]


def test_run_dbmate_returns_subprocess_exit_code(monkeypatch) -> None:
    seen: dict[str, object] = {}

    def _fake_run(
        command: list[str],
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        seen["command"] = command
        seen["check"] = check
        seen["capture_output"] = capture_output
        seen["text"] = text
        return subprocess.CompletedProcess(args=command, returncode=3, stdout="", stderr="")

    monkeypatch.setattr("matey.drivers.dbmate.subprocess.run", _fake_run)

    code = run_dbmate(
        dbmate_binary=Path("/bin/dbmate"),
        url="postgres://db",
        migrations_dir=Path("/repo/db/migrations"),
        schema_file=Path("/repo/db/schema.sql"),
        verb="status",
    )
    assert code == 3
    assert seen["command"][0] == "/bin/dbmate"
    assert seen["check"] is False
    assert seen["capture_output"] is True
    assert seen["text"] is True


def test_run_dbmate_capture_returns_completed_process(monkeypatch) -> None:
    def _fake_run(
        command: list[str],
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert command[0] == "/bin/dbmate"
        assert check is False
        assert capture_output is True
        assert text is True
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr("matey.drivers.dbmate.subprocess.run", _fake_run)

    result = run_dbmate_capture(
        dbmate_binary=Path("/bin/dbmate"),
        url="postgres://db",
        migrations_dir=Path("/repo/db/migrations"),
        schema_file=Path("/repo/db/schema.sql"),
        verb="dump",
    )
    assert result.returncode == 0
    assert result.stdout == "ok"


def test_run_dbmate_emits_execution_result_to_callback(monkeypatch) -> None:
    emitted = []

    def _fake_run(
        command: list[str],
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert command[0] == "/bin/dbmate"
        assert check is False
        assert capture_output is True
        assert text is True
        return subprocess.CompletedProcess(args=command, returncode=2, stdout="out", stderr="err")

    monkeypatch.setattr("matey.drivers.dbmate.subprocess.run", _fake_run)

    code = run_dbmate(
        dbmate_binary=Path("/bin/dbmate"),
        url="postgres://alice:secret@db.example.com:5432/main",
        migrations_dir=Path("/repo/db/migrations"),
        schema_file=Path("/repo/db/schema.sql"),
        verb="up",
        log_context=DbmateLogContext(target="core", phase="direct", step="up"),
        on_result=emitted.append,
    )
    assert code == 2
    assert len(emitted) == 1
    event = emitted[0]
    assert event.returncode == 2
    assert event.context == DbmateLogContext(target="core", phase="direct", step="up")
    assert event.stdout == "out"
    assert event.stderr == "err"
    assert event.verb == "up"
    assert event.captured is False
    assert event.command[2] == "postgres://alice:secret@db.example.com:5432/main"
    assert event.display_command[2] == "postgres://alice:***@db.example.com:5432/main"


def test_run_dbmate_capture_emits_execution_result_to_callback(monkeypatch) -> None:
    emitted = []

    def _fake_run(
        command: list[str],
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert command[0] == "/bin/dbmate"
        assert check is False
        assert capture_output is True
        assert text is True
        return subprocess.CompletedProcess(args=command, returncode=0, stdout="a\nb\n", stderr="")

    monkeypatch.setattr("matey.drivers.dbmate.subprocess.run", _fake_run)

    completed = run_dbmate_capture(
        dbmate_binary=Path("/bin/dbmate"),
        url="postgres://db",
        migrations_dir=Path("/repo/db/migrations"),
        schema_file=Path("/repo/db/schema.sql"),
        verb="dump",
        on_result=emitted.append,
    )
    assert completed.returncode == 0
    assert len(emitted) == 1
    event = emitted[0]
    assert event.returncode == 0
    assert event.stdout == "a\nb\n"
    assert event.stderr == ""
    assert event.captured is True
    assert event.verb == "dump"
