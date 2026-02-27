from __future__ import annotations

import subprocess
from pathlib import Path
from typing import Any

from matey.domain import ResolvedPaths, ScratchTarget
from matey.drivers.scratch import ScratchPlan
from matey.workflows.schema import _run_check_on_scratch
from matey.workflows.schema_lock import _scratch_schema_from_replay


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _paths(tmp_path: Path) -> ResolvedPaths:
    db_dir = tmp_path / "db"
    migrations_dir = db_dir / "migrations"
    schema_file = db_dir / "schema.sql"
    _write(
        migrations_dir / "202601010000_init.sql",
        "-- migrate:up\nCREATE TABLE widgets (id INT);\n-- migrate:down\nDROP TABLE widgets;\n",
    )
    _write(schema_file, "CREATE TABLE widgets (id INT);\n")
    return ResolvedPaths(
        db_dir=db_dir,
        migrations_dir=migrations_dir,
        schema_file=schema_file,
    )


def test_clean_check_uses_temp_schema_files(monkeypatch, tmp_path: Path) -> None:
    head_paths = _paths(tmp_path)
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        "matey.workflows.schema.plan_scratch_target",
        lambda **kwargs: ScratchPlan(
            target=ScratchTarget(
                engine="postgres",
                scratch_name=kwargs["scratch_name"],
                scratch_url="postgres://u:p@localhost/scratch",
                cleanup_required=True,
                auto_provisioned=False,
            ),
            cleanup=lambda: None,
        ),
    )

    def _fake_run_dbmate(**kwargs: Any) -> int:
        calls.append(kwargs)
        return 0

    monkeypatch.setattr("matey.workflows.schema.run_dbmate", _fake_run_dbmate)
    monkeypatch.setattr(
        "matey.workflows.schema.run_dbmate_capture",
        lambda **kwargs: subprocess.CompletedProcess(
            args=["dbmate"],
            returncode=0,
            stdout="CREATE TABLE widgets (id INT);\n",
            stderr="",
        ),
    )

    result = _run_check_on_scratch(
        target_name="default",
        engine="postgres",
        dbmate_binary=Path("/tmp/dbmate"),
        head_paths=head_paths,
        apply_phases=[("head", head_paths)],
        test_url="postgres://u:p@localhost/base",
        keep_scratch=False,
        wait_timeout="1s",
        check_name="clean",
        on_dbmate_result=None,
    )

    assert result.error is None
    assert result.schema_sql is not None
    assert calls
    for call in calls:
        assert call["schema_file"] != head_paths.schema_file
    up_calls = [call for call in calls if call["verb"] == "up"]
    assert up_calls
    assert all("--no-dump-schema" in call["global_args"] for call in up_calls)


def test_replay_uses_temp_schema_files(monkeypatch, tmp_path: Path) -> None:
    head_paths = _paths(tmp_path)
    calls: list[dict[str, Any]] = []

    monkeypatch.setattr(
        "matey.workflows.schema_lock.plan_scratch_target",
        lambda **kwargs: ScratchPlan(
            target=ScratchTarget(
                engine="postgres",
                scratch_name=kwargs["scratch_name"],
                scratch_url="postgres://u:p@localhost/scratch",
                cleanup_required=True,
                auto_provisioned=False,
            ),
            cleanup=lambda: None,
        ),
    )

    def _fake_run_dbmate(**kwargs: Any) -> int:
        calls.append(kwargs)
        return 0

    monkeypatch.setattr("matey.workflows.schema_lock.run_dbmate", _fake_run_dbmate)
    monkeypatch.setattr(
        "matey.workflows.schema_lock.run_dbmate_capture",
        lambda **kwargs: subprocess.CompletedProcess(
            args=["dbmate"],
            returncode=0,
            stdout="CREATE TABLE widgets (id INT);\n",
            stderr="",
        ),
    )

    replay_sql, _scratch_url = _scratch_schema_from_replay(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        engine="postgres",
        head_paths=head_paths,
        test_url="postgres://u:p@localhost/base",
        keep_scratch=False,
        wait_timeout="1s",
        anchor_checkpoint_file=None,
        tail_migration_file_names=["202601010000_init.sql"],
        on_dbmate_result=None,
    )

    assert replay_sql
    assert calls
    for call in calls:
        assert call["schema_file"] != head_paths.schema_file
    up_calls = [call for call in calls if call["verb"] == "up"]
    assert up_calls
    assert all("--no-dump-schema" in call["global_args"] for call in up_calls)
