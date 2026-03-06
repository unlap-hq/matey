from __future__ import annotations

import re
from pathlib import Path

from matey import __main__ as cli
from matey.db import MutationResult
from matey.dbmate import CmdResult
from matey.lockfile import LockState

_HELP_COMMAND_ROW = re.compile(r"^│\s+([a-z][a-z0-9-]*)\s{2,}.*$")


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _help_command_names(output: str) -> list[str]:
    names: list[str] = []
    for line in output.splitlines():
        match = _HELP_COMMAND_ROW.match(line.strip())
        if match is None:
            continue
        names.append(match.group(1))
    return names


def test_db_up_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "DATABASE_URL"
test_url_env = "TEST_DATABASE_URL"

[core]
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    captured: dict[str, object] = {}

    def _fake_up(target, *, url, dbmate_bin):
        captured["target"] = target.name
        captured["url"] = url
        captured["dbmate_bin"] = dbmate_bin
        return MutationResult(target_name=target.name, before_index=0, after_index=1)

    monkeypatch.setattr(cli.db_api, "up", _fake_up)
    rc = cli.main(["db", "up", "--target", "core", "--url", "sqlite:///tmp.db"])
    assert rc == 0
    assert captured == {
        "target": "core",
        "url": "sqlite:///tmp.db",
        "dbmate_bin": None,
    }


def test_db_status_nonzero_exit_maps_to_user_error(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        cli.db_api,
        "status_raw",
        lambda target, *, url, dbmate_bin: CmdResult(
            argv=("dbmate", "status"),
            exit_code=1,
            stdout="",
            stderr="boom",
        ),
    )
    rc = cli.main(["db", "status", "--target", "core"])
    assert rc == 2


def test_schema_status_runs_all_targets(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
dir = "db"
url_env = "DATABASE_URL"
test_url_env = "TEST_DATABASE_URL"

[core]
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"

[analytics]
url_env = "ANALYTICS_DATABASE_URL"
test_url_env = "ANALYTICS_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    calls: list[str] = []

    def _fake_status(target):
        calls.append(target.name)
        return LockState(
            target_name=target.name,
            lock=None,
            worktree_steps=(),
            schema_digest=None,
            orphan_checkpoints=(),
            diagnostics=(),
        )

    monkeypatch.setattr(cli.schema_api, "status", _fake_status)
    rc = cli.main(["schema", "status", "--all"])
    assert rc == 0
    assert calls == ["analytics", "core"]


def test_mutating_command_rejects_all_targets(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"

[analytics]
dir = "db/analytics"
url_env = "ANALYTICS_DATABASE_URL"
test_url_env = "ANALYTICS_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    rc = cli.main(["db", "up", "--all"])
    assert rc == 2


def test_help_commands_exit_zero() -> None:
    assert cli.main(["--help"]) == 0
    assert cli.main(["dbmate", "--help"]) == 0
    assert cli.main(["db", "--help"]) == 0
    assert cli.main(["db", "plan", "--help"]) == 0
    assert cli.main(["schema", "--help"]) == 0
    assert cli.main(["schema", "plan", "--help"]) == 0
    assert cli.main(["template", "--help"]) == 0


def test_db_help_command_order_semantic(capsys) -> None:
    rc = cli.main(["db", "--help"])
    assert rc == 0
    captured = capsys.readouterr()
    assert _help_command_names(captured.out) == [
        "status",
        "up",
        "migrate",
        "down",
        "drift",
        "plan",
        "new",
    ]


def test_db_help_descriptions_match_semantics(capsys) -> None:
    rc = cli.main(["db", "--help"])
    assert rc == 0
    captured = capsys.readouterr()
    output = captured.out
    assert "Create DB if missing, then apply pending migrations." in output
    assert "Apply pending migrations (no create-if-needed)." in output
    assert "Compare live schema to expected worktree target schema." in output


def test_schema_help_command_order_semantic(capsys) -> None:
    rc = cli.main(["schema", "--help"])
    assert rc == 0
    captured = capsys.readouterr()
    assert _help_command_names(captured.out) == ["status", "plan", "apply"]


def test_db_new_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_new(target, *, name, dbmate_bin):
        called["target"] = target.name
        called["name"] = name
        called["dbmate_bin"] = dbmate_bin
        return CmdResult(
            argv=("dbmate", "new", name),
            exit_code=0,
            stdout="db/core/migrations/202601010101_name.sql\n",
            stderr="",
        )

    monkeypatch.setattr(cli.db_api, "new", _fake_new)
    rc = cli.main(["db", "new", "add_users", "--target", "core"])
    assert rc == 0
    assert called == {"target": "core", "name": "add_users", "dbmate_bin": None}


def test_dbmate_passthrough_routes_verbatim(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_passthrough(*args: str, dbmate_bin):
        captured["args"] = args
        captured["dbmate_bin"] = dbmate_bin
        return CmdResult(
            argv=("dbmate", *args),
            exit_code=0,
            stdout="ok\n",
            stderr="",
        )

    monkeypatch.setattr(cli.dbmate_api, "passthrough", _fake_passthrough)
    rc = cli.main(["dbmate", "--", "status", "--wait"])
    assert rc == 0
    assert captured == {"args": ("status", "--wait"), "dbmate_bin": None}


def test_dbmate_passthrough_propagates_exit_code(monkeypatch) -> None:
    monkeypatch.setattr(
        cli.dbmate_api,
        "passthrough",
        lambda *args, dbmate_bin: CmdResult(
            argv=("dbmate", *args),
            exit_code=7,
            stdout="",
            stderr="boom\n",
        ),
    )
    rc = cli.main(["dbmate", "--", "status"])
    assert rc == 7


def test_dbmate_passthrough_defaults_to_help_when_empty(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_passthrough(*args: str, dbmate_bin):
        captured["args"] = args
        captured["dbmate_bin"] = dbmate_bin
        return CmdResult(
            argv=("dbmate", *args),
            exit_code=0,
            stdout="dbmate help\n",
            stderr="",
        )

    monkeypatch.setattr(cli.dbmate_api, "passthrough", _fake_passthrough)
    rc = cli.main(["dbmate"])
    assert rc == 0
    assert captured == {"args": ("--help",), "dbmate_bin": None}


def test_dbmate_dash_help_is_forwarded_to_dbmate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_passthrough(*args: str, dbmate_bin):
        captured["args"] = args
        captured["dbmate_bin"] = dbmate_bin
        return CmdResult(
            argv=("dbmate", *args),
            exit_code=0,
            stdout="dbmate help\n",
            stderr="",
        )

    monkeypatch.setattr(cli.dbmate_api, "passthrough", _fake_passthrough)
    rc = cli.main(["dbmate", "--help"])
    assert rc == 0
    assert captured == {"args": ("--help",), "dbmate_bin": None}


def test_schema_plan_sql_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_plan_sql(
        target,
        *,
        base_ref,
        clean,
        test_base_url,
        keep_scratch,
        dbmate_bin,
        policy=None,
    ):
        del policy
        called["target"] = target.name
        called["base_ref"] = base_ref
        called["clean"] = clean
        called["test_base_url"] = test_base_url
        called["keep_scratch"] = keep_scratch
        called["dbmate_bin"] = dbmate_bin
        return "CREATE TABLE x(id INTEGER);\n"

    monkeypatch.setattr(cli.schema_api, "plan_sql", _fake_plan_sql)
    rc = cli.main(
        [
            "schema",
            "plan",
            "--sql",
            "--target",
            "core",
            "--base",
            "origin/main",
            "--clean",
            "--test-url",
            "sqlite3:/tmp/scratch.sqlite3",
            "--keep-scratch",
        ]
    )
    assert rc == 0
    assert called == {
        "target": "core",
        "base_ref": "origin/main",
        "clean": True,
        "test_base_url": "sqlite3:/tmp/scratch.sqlite3",
        "keep_scratch": True,
        "dbmate_bin": None,
    }


def test_schema_plan_diff_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_plan_diff(
        target,
        *,
        base_ref,
        clean,
        test_base_url,
        keep_scratch,
        dbmate_bin,
        policy=None,
    ):
        del policy
        called["target"] = target.name
        called["base_ref"] = base_ref
        called["clean"] = clean
        called["test_base_url"] = test_base_url
        called["keep_scratch"] = keep_scratch
        called["dbmate_bin"] = dbmate_bin
        return ""

    monkeypatch.setattr(cli.schema_api, "plan_diff", _fake_plan_diff)
    rc = cli.main(["schema", "plan", "--diff", "--target", "core"])
    assert rc == 0
    assert called["target"] == "core"
    assert called["base_ref"] is None
    assert called["clean"] is False
    assert called["test_base_url"] is None
    assert called["keep_scratch"] is False
    assert called["dbmate_bin"] is None


def test_db_plan_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_db_plan(target, *, url, dbmate_bin):
        called["target"] = target.name
        called["url"] = url
        called["dbmate_bin"] = dbmate_bin
        from matey.db import PlanResult

        return PlanResult(
            target_name=target.name,
            applied_index=1,
            target_index=2,
            matches=False,
        )

    monkeypatch.setattr(cli.db_api, "plan", _fake_db_plan)
    rc = cli.main(["db", "plan", "--target", "core", "--url", "sqlite:///tmp.db"])
    assert rc == 0
    assert called == {"target": "core", "url": "sqlite:///tmp.db", "dbmate_bin": None}


def test_db_plan_sql_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_db_plan_sql(target, *, url, dbmate_bin):
        called["target"] = target.name
        called["url"] = url
        called["dbmate_bin"] = dbmate_bin
        return "CREATE TABLE x(id INTEGER);\n"

    monkeypatch.setattr(cli.db_api, "plan_sql", _fake_db_plan_sql)
    rc = cli.main(["db", "plan", "--sql", "--target", "core"])
    assert rc == 0
    assert called == {"target": "core", "url": None, "dbmate_bin": None}


def test_db_plan_diff_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_db_plan_diff(target, *, url, dbmate_bin):
        called["target"] = target.name
        called["url"] = url
        called["dbmate_bin"] = dbmate_bin
        return ""

    monkeypatch.setattr(cli.db_api, "plan_diff", _fake_db_plan_diff)
    rc = cli.main(["db", "plan", "--diff", "--target", "core"])
    assert rc == 0
    assert called == {"target": "core", "url": None, "dbmate_bin": None}


def test_plan_rejects_sql_and_diff_together(monkeypatch, tmp_path: Path) -> None:
    _write(
        tmp_path / "matey.toml",
        """
[core]
dir = "db/core"
url_env = "CORE_DATABASE_URL"
test_url_env = "CORE_TEST_DATABASE_URL"
""".strip(),
    )
    monkeypatch.chdir(tmp_path)

    rc_schema = cli.main(["schema", "plan", "--sql", "--diff", "--target", "core"])
    rc_db = cli.main(["db", "plan", "--sql", "--diff", "--target", "core"])

    assert rc_schema == 2
    assert rc_db == 2
