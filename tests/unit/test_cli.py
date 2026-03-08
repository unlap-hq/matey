from __future__ import annotations

import re
from importlib import import_module
from pathlib import Path

import pygit2
import pytest

from matey.db import MutationResult
from matey.db import PlanResult as DbPlanResult
from matey.dbmate import CmdResult
from matey.lockfile import LockState
from matey.scratch import Engine

cli = import_module("matey.cli.app")

_HELP_COMMAND_ROW = re.compile(r"^│\s+([a-z][a-z0-9-]*)\s{2,}.*$")


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _init_repo(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    pygit2.init_repository(str(path), initial_head="main")


def _write_workspace(repo_root: Path, targets: tuple[str, ...]) -> None:
    _write(repo_root / "matey.toml", "targets = [\n" + "".join(f'  \"{target}\",\n' for target in targets) + "]\n")


def _write_target_config(repo_root: Path, rel_path: str, *, engine: str = "sqlite", url_env: str | None = None, test_url_env: str | None = None) -> None:
    stem = rel_path.replace("/", "_").replace("-", "_").upper()
    url_env = url_env or ("DATABASE_URL" if rel_path == "." else f"{stem}_DATABASE_URL")
    test_url_env = test_url_env or ("TEST_DATABASE_URL" if rel_path == "." else f"{stem}_TEST_DATABASE_URL")
    _write(
        repo_root / rel_path / "config.toml",
        f'engine = "{engine}"\nurl_env = "{url_env}"\ntest_url_env = "{test_url_env}"\n',
    )


def _help_command_names(output: str) -> list[str]:
    names: list[str] = []
    for line in output.splitlines():
        match = _HELP_COMMAND_ROW.match(line.strip())
        if match is not None:
            names.append(match.group(1))
    return names


def test_db_up_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core",))
    _write_target_config(tmp_path, "db/core")
    monkeypatch.chdir(tmp_path)
    captured: dict[str, object] = {}

    def _fake_up(target, *, url, dbmate_bin):
        captured["target"] = target.name
        captured["url"] = url
        captured["dbmate_bin"] = dbmate_bin
        return MutationResult(target_name=target.name, before_index=0, after_index=1)

    monkeypatch.setattr(cli.db.db_api, "up", _fake_up)
    rc = cli.main(["db", "up", "--path", "db/core", "--url", "sqlite:///tmp.db"])

    assert rc == 0
    assert captured == {
        "target": "db/core",
        "url": "sqlite:///tmp.db",
        "dbmate_bin": None,
    }


def test_db_bootstrap_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core",))
    _write_target_config(tmp_path, "db/core")
    monkeypatch.chdir(tmp_path)
    captured: dict[str, object] = {}

    def _fake_bootstrap(target, *, url, dbmate_bin):
        captured["target"] = target.name
        captured["url"] = url
        captured["dbmate_bin"] = dbmate_bin
        return MutationResult(target_name=target.name, before_index=0, after_index=1)

    monkeypatch.setattr(cli.db.db_api, "bootstrap", _fake_bootstrap)
    rc = cli.main(["db", "bootstrap", "--path", "db/core", "--url", "sqlite:///tmp.db"])

    assert rc == 0
    assert captured == {
        "target": "db/core",
        "url": "sqlite:///tmp.db",
        "dbmate_bin": None,
    }


def test_init_target_routes_path(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    monkeypatch.chdir(tmp_path)
    captured: dict[str, object] = {}

    def _fake_prepare_init_target(target, *, engine, force, policy=None):
        del policy
        captured["target"] = target.name
        captured["dir"] = target.dir
        captured["engine"] = engine
        captured["force"] = force
        return cli.init.schema_api.InitPlan(
            target=target,
            engine=Engine.SQLITE,
            writes={},
            deletes=(),
            created_dirs=(),
        )

    monkeypatch.setattr(cli.init.schema_api, "prepare_init_target", _fake_prepare_init_target)
    monkeypatch.setattr(
        cli.init.schema_api,
        "apply_init_target",
        lambda plan: cli.init.schema_api.InitResult(
            target_name=plan.target.name,
            engine=plan.engine.value,
            wrote=False,
            changed_files=(),
        ),
    )
    rc = cli.main([
        "init",
        "--path",
        "db/core",
        "--engine",
        "sqlite",
        "--url-env",
        "CORE_DATABASE_URL",
        "--test-url-env",
        "CORE_TEST_DATABASE_URL",
        "--force",
    ])

    assert rc == 0
    assert captured == {
        "target": "db/core",
        "dir": (tmp_path / "db" / "core").resolve(),
        "engine": "sqlite",
        "force": True,
    }


def test_db_status_nonzero_exit_maps_to_user_error(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core",))
    _write_target_config(tmp_path, "db/core")
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        cli.db.db_api,
        "status_raw",
        lambda target, *, url, dbmate_bin: CmdResult(
            argv=("dbmate", "status"),
            exit_code=1,
            stdout="",
            stderr="boom",
        ),
    )
    rc = cli.main(["db", "status", "--path", "db/core"])
    assert rc == 2


def test_schema_status_requires_git_or_explicit_config(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.chdir(tmp_path)

    rc = cli.main(["schema", "status"])

    assert rc == 2


def test_schema_status_runs_all_targets(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core", "db/analytics"))
    _write_target_config(tmp_path, "db/core")
    _write_target_config(tmp_path, "db/analytics")
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

    monkeypatch.setattr(cli.schema.schema_api, "status", _fake_status)
    rc = cli.main(["schema", "status", "--all"])

    assert rc == 0
    assert calls == ["db/analytics", "db/core"]


def test_mutating_command_rejects_all_targets(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core", "db/analytics"))
    _write_target_config(tmp_path, "db/core")
    _write_target_config(tmp_path, "db/analytics")
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
    assert cli.main(["init", "--help"]) == 0
    assert cli.main(["lint", "--help"]) == 0


def test_root_help_command_order(capsys) -> None:
    rc = cli.main(["--help"])
    assert rc == 0
    captured = capsys.readouterr()
    assert _help_command_names(captured.out) == ["init", "lint", "schema", "db", "dbmate"]


def test_db_help_command_order_semantic(capsys) -> None:
    rc = cli.main(["db", "--help"])
    assert rc == 0
    captured = capsys.readouterr()
    assert _help_command_names(captured.out) == [
        "status",
        "bootstrap",
        "up",
        "migrate",
        "down",
        "drift",
        "plan",
        "new",
    ]


def test_load_config_resolves_workspace_file_from_config_location(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    workspace_path = repo_root / "matey.toml"
    _init_repo(repo_root)
    _write(workspace_path, 'targets = ["db/core"]\n')
    _write_target_config(repo_root, "db/core")
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.chdir(outside)

    config = cli.common.load_config(repo_root)

    assert tuple(config.targets.keys()) == ("db/core",)


def test_load_config_preserves_pyproject_workspace_fallback(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    pyproject_path = repo_root / "pyproject.toml"
    _init_repo(repo_root)
    _write(pyproject_path, '[tool.matey]\ntargets = ["db/core"]\n')
    _write_target_config(repo_root, "db/core")
    outside = tmp_path / "outside"
    outside.mkdir()
    monkeypatch.chdir(outside)

    config = cli.common.load_config(repo_root)

    assert tuple(config.targets.keys()) == ("db/core",)


def test_load_config_prefers_local_workspace_over_git_root(monkeypatch, tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    nested = repo_root / "nested"
    _init_repo(repo_root)
    _write(repo_root / "matey.toml", 'targets = ["db/root"]\n')
    _write_target_config(repo_root, "db/root")
    _write(nested / "matey.toml", 'targets = ["db/local"]\n')
    _write_target_config(nested, "db/local")
    monkeypatch.chdir(nested)

    config = cli.common.load_config(None)

    assert tuple(config.targets.keys()) == ("db/local",)


def test_load_config_rejects_workspace_file_path(tmp_path: Path) -> None:
    repo_root = tmp_path / "repo"
    workspace_path = repo_root / "matey.toml"
    _init_repo(repo_root)
    _write(workspace_path, 'targets = []\n')

    with pytest.raises(cli.common.CliUsageError, match="--workspace must point to a directory"):
        cli.common.load_config(workspace_path)


def test_db_new_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core",))
    _write_target_config(tmp_path, "db/core")
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

    monkeypatch.setattr(cli.db.db_api, "new", _fake_new)
    rc = cli.main(["db", "new", "add_users", "--path", "db/core"])
    assert rc == 0
    assert called == {"target": "db/core", "name": "add_users", "dbmate_bin": None}


def test_dbmate_passthrough_routes_verbatim(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_passthrough(*args: str, dbmate_bin):
        captured["args"] = args
        captured["dbmate_bin"] = dbmate_bin
        return CmdResult(argv=("dbmate", *args), exit_code=0, stdout="ok\n", stderr="")

    monkeypatch.setattr(cli.common.dbmate_api, "passthrough", _fake_passthrough)
    rc = cli.main(["dbmate", "--", "status", "--wait"])
    assert rc == 0
    assert captured == {"args": ("status", "--wait"), "dbmate_bin": None}


def test_dbmate_top_level_intercept_uses_shared_helper(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run(*, argv, dbmate_bin=None, renderer):
        captured["argv"] = argv
        captured["dbmate_bin"] = dbmate_bin
        captured["renderer"] = renderer
        return 0

    monkeypatch.setattr(cli.common, "handle_dbmate_passthrough", _fake_run)

    rc = cli.main(["dbmate", "--", "status"])

    assert rc == 0
    assert captured["argv"] == ("dbmate", "--", "status")
    assert captured["dbmate_bin"] is None


def test_dbmate_registered_command_uses_shared_helper(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_run(*, argv, dbmate_bin=None, renderer):
        captured["argv"] = argv
        captured["dbmate_bin"] = dbmate_bin
        captured["renderer"] = renderer
        return 0

    monkeypatch.setattr(cli.common, "handle_dbmate_passthrough", _fake_run)

    with pytest.raises(SystemExit) as excinfo:
        cli.app(["dbmate", "status"])

    assert excinfo.value.code == 0
    assert captured["argv"] == ("status",)
    assert captured["dbmate_bin"] is None


def test_schema_plan_sql_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core",))
    _write_target_config(tmp_path, "db/core")
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_plan_sql(target, *, base_ref, clean, test_base_url, keep_scratch, dbmate_bin, policy=None):
        del policy
        called.update(
            {
                "target": target.name,
                "base_ref": base_ref,
                "clean": clean,
                "test_base_url": test_base_url,
                "keep_scratch": keep_scratch,
                "dbmate_bin": dbmate_bin,
            }
        )
        return "CREATE TABLE x(id INTEGER);\n"

    monkeypatch.setattr(cli.schema.schema_api, "plan_sql", _fake_plan_sql)
    rc = cli.main([
        "schema", "plan", "--sql", "--path", "db/core", "--base", "origin/main", "--clean", "--test-url", "sqlite3:/tmp/scratch.sqlite3", "--keep-scratch"
    ])
    assert rc == 0
    assert called == {
        "target": "db/core",
        "base_ref": "origin/main",
        "clean": True,
        "test_base_url": "sqlite3:/tmp/scratch.sqlite3",
        "keep_scratch": True,
        "dbmate_bin": None,
    }


def test_db_plan_routes_to_engine(monkeypatch, tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _write_workspace(tmp_path, ("db/core",))
    _write_target_config(tmp_path, "db/core")
    monkeypatch.chdir(tmp_path)
    called: dict[str, object] = {}

    def _fake_db_plan(target, *, url, dbmate_bin):
        called["target"] = target.name
        called["url"] = url
        called["dbmate_bin"] = dbmate_bin
        return DbPlanResult(target_name=target.name, applied_index=1, target_index=2, matches=False)

    monkeypatch.setattr(cli.db.db_api, "plan", _fake_db_plan)
    rc = cli.main(["db", "plan", "--path", "db/core", "--url", "sqlite:///tmp.db"])
    assert rc == 0
    assert called == {"target": "db/core", "url": "sqlite:///tmp.db", "dbmate_bin": None}
