from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
import typer
from typer.testing import CliRunner

import matey.__main__ as main_module
from matey.cli.groups.db import register_db_group
from matey.cli.groups.schema import register_schema_group
from matey.cli.groups.template import register_template_group
from matey.cli.help import group_meta, root_help_text
from matey.domain.errors import CliUsageError, MateyError
from matey.domain.model import (
    ConfigDefaults,
    PreparedSql,
    SchemaPlanResult,
    SqlComparison,
    TargetId,
    TargetPaths,
)


@dataclass
class _Options:
    base_ref: str | None = "origin/main"
    url: str | None = "sqlite3:/tmp/live.db"
    test_url: str | None = "sqlite3:/tmp/test.db"
    keep_scratch: bool = True


def _runtime() -> object:
    paths = TargetPaths(
        db_dir=Path("/tmp/db"),
        migrations_dir=Path("/tmp/db/migrations"),
        checkpoints_dir=Path("/tmp/db/checkpoints"),
        schema_file=Path("/tmp/db/schema.sql"),
        lock_file=Path("/tmp/db/schema.lock.toml"),
    )
    return type(
        "Runtime",
        (),
        {
            "target_id": TargetId("core"),
            "paths": paths,
            "url_env": "MATEY_URL",
            "test_url_env": "MATEY_TEST_URL",
        },
    )()


def test_root_help_text_includes_group_and_subgroup_lists() -> None:
    text = root_help_text()
    assert "db: new, create, wait, up, migrate, status, drift, plan, load, dump, down, drop, dbmate" in text
    assert "db.plan: diff, sql" in text
    assert "schema.plan: diff, sql" in text


def test_group_help_is_non_empty_for_all_groups() -> None:
    from matey.cli.app import app

    runner = CliRunner()
    for command, expected in (
        ("db", "Live database commands."),
        ("schema", "Schema artifact workflows."),
        ("template", "Template helpers."),
    ):
        result = runner.invoke(app, [command, "--help"])
        assert result.exit_code == 0
        assert expected in result.output


def test_db_registry_order_matches_expected_lifecycle() -> None:
    db = group_meta("db")
    assert [command.name for command in db.commands] == [
        "new",
        "create",
        "wait",
        "up",
        "migrate",
        "status",
        "drift",
        "plan",
        "load",
        "dump",
        "down",
        "drop",
        "dbmate",
    ]


def test_main_exit_mapping_for_typed_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def _raise_usage() -> None:
        raise CliUsageError("bad args")

    monkeypatch.setattr(main_module, "app", _raise_usage)
    assert main_module.main() == 2

    def _raise_domain() -> None:
        raise MateyError("failure")

    monkeypatch.setattr(main_module, "app", _raise_domain)
    assert main_module.main() == 1

    def _raise_unknown() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(main_module, "app", _raise_unknown)
    assert main_module.main() == 1


def test_schema_plan_option_propagation() -> None:
    class FakeSchemaEngine:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def schema_plan(self, **kwargs) -> SchemaPlanResult:
            self.calls.append(kwargs)
            prepared = PreparedSql(normalized="x\n", digest="d")
            comparison = SqlComparison(expected=prepared, actual=prepared, equal=True, diff=None)
            return SchemaPlanResult(
                comparison=comparison,
                replay_scratch_url="sqlite3:/tmp/scratch.db",
                down_checked=True,
                orphan_checkpoints=(),
            )

    fake = FakeSchemaEngine()
    runner = CliRunner()
    app = typer.Typer()
    register_schema_group(
        parent=app,
        schema_engine_for_ctx=lambda _ctx: fake,
        resolve_targets=lambda _ctx: (("core", _runtime(), ConfigDefaults()),),
        options_for_ctx=lambda _ctx: _Options(),
        group_meta=group_meta("schema"),
    )

    result = runner.invoke(app, ["schema", "plan", "--clean"])
    assert result.exit_code == 0
    assert len(fake.calls) == 1
    call = fake.calls[0]
    assert call["clean"] is True
    assert call["base_ref"] == "origin/main"
    assert call["url_override"] == "sqlite3:/tmp/live.db"
    assert call["test_url_override"] == "sqlite3:/tmp/test.db"
    assert call["keep_scratch"] is True


def test_db_status_cli_uses_raw_passthrough() -> None:
    class FakeDbEngine:
        def db_status(self, **kwargs) -> str:
            del kwargs
            return "??weird status@@\nline2\n"

    app = typer.Typer()
    register_db_group(
        parent=app,
        db_engine_for_ctx=lambda _ctx: FakeDbEngine(),
        resolve_targets=lambda _ctx: (("core", _runtime(), ConfigDefaults()),),
        options_for_ctx=lambda _ctx: _Options(),
        group_meta=group_meta("db"),
    )

    runner = CliRunner()
    result = runner.invoke(app, ["db", "status"])
    assert result.exit_code == 0
    assert result.output == "??weird status@@\nline2\n"


def test_template_commands_print_by_default_and_write_when_path_is_set(tmp_path: Path) -> None:
    class FakeTemplateEngine:
        def __init__(self, rendered: str) -> None:
            self.rendered = rendered
            self.write_calls: list[tuple[Path, bool]] = []

        def render(self) -> str:
            return self.rendered

        def write(self, *, path: Path, overwrite: bool) -> None:
            self.write_calls.append((path, overwrite))
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(self.rendered, encoding="utf-8")

    config_engine = FakeTemplateEngine("dir = \"db\"\n")
    ci_engine = FakeTemplateEngine("name: matey\n")
    app = typer.Typer()
    register_template_group(
        parent=app,
        config_engine=config_engine,
        ci_engine=ci_engine,
        group_meta=group_meta("template"),
    )
    runner = CliRunner()

    print_result = runner.invoke(app, ["template", "config"])
    assert print_result.exit_code == 0
    assert print_result.output == "dir = \"db\"\n\n"
    assert config_engine.write_calls == []

    config_path = tmp_path / "matey.toml"
    write_result = runner.invoke(app, ["template", "config", "--path", str(config_path)])
    assert write_result.exit_code == 0
    assert config_path.read_text(encoding="utf-8") == "dir = \"db\"\n"
    assert config_engine.write_calls == [(config_path, False)]

    ci_result = runner.invoke(app, ["template", "ci"])
    assert ci_result.exit_code == 0
    assert ci_result.output == "name: matey\n\n"
    assert ci_engine.write_calls == []
