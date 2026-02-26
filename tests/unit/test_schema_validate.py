from __future__ import annotations

import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest

from matey.domain import ResolvedPaths, SchemaValidationError, ScratchTarget
from matey.drivers.scratch import ScratchPlan
from matey.workflows.schema import validate_schema_clean_target


def _paths(tmp_path: Path) -> ResolvedPaths:
    return ResolvedPaths(
        db_dir=tmp_path / "db",
        migrations_dir=tmp_path / "db" / "migrations",
        schema_file=tmp_path / "db" / "schema.sql",
    )


def test_validate_schema_clean_success(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    paths.schema_file.parent.mkdir(parents=True, exist_ok=True)
    paths.schema_file.write_text("CREATE TABLE t (id INT);\n", encoding="utf-8")

    cleanup_called = {"called": False}

    def _cleanup() -> None:
        cleanup_called["called"] = True

    monkeypatch.setattr(
        "matey.workflows.schema.plan_scratch_target",
        lambda **_: ScratchPlan(
            target=ScratchTarget(
                engine="postgres",
                scratch_name="scratch",
                scratch_url="postgres://u:p@localhost/scratch",
                cleanup_required=True,
                auto_provisioned=False,
            ),
            cleanup=_cleanup,
        ),
    )
    monkeypatch.setattr("matey.workflows.schema.run_dbmate", lambda **_: 0)
    monkeypatch.setattr(
        "matey.workflows.schema.run_dbmate_capture",
        lambda **_: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="CREATE TABLE t (id INT);\n",
            stderr="",
        ),
    )

    result = validate_schema_clean_target(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url="postgres://u:p@localhost/app",
        test_url=None,
        keep_scratch=False,
        no_repo_check=False,
    )
    assert result.success is True
    assert result.diff_text is None
    assert cleanup_called["called"] is True


def test_validate_schema_clean_reports_diff(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    paths.schema_file.parent.mkdir(parents=True, exist_ok=True)
    paths.schema_file.write_text("CREATE TABLE t (id INT);\n", encoding="utf-8")

    monkeypatch.setattr(
        "matey.workflows.schema.plan_scratch_target",
        lambda **_: ScratchPlan(
            target=ScratchTarget(
                engine="postgres",
                scratch_name="scratch",
                scratch_url="postgres://u:p@localhost/scratch",
                cleanup_required=True,
                auto_provisioned=False,
            ),
            cleanup=lambda: None,
        ),
    )
    monkeypatch.setattr("matey.workflows.schema.run_dbmate", lambda **_: 0)
    monkeypatch.setattr(
        "matey.workflows.schema.run_dbmate_capture",
        lambda **_: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="CREATE TABLE t (id INT PRIMARY KEY);\n",
            stderr="",
        ),
    )

    result = validate_schema_clean_target(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url="postgres://u:p@localhost/app",
        test_url=None,
        keep_scratch=False,
        no_repo_check=False,
    )
    assert result.success is False
    assert result.diff_text is not None
    assert "---" in result.diff_text
    assert "+++" in result.diff_text


def test_validate_schema_clean_no_repo_check_ignores_repo_file(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    paths.schema_file.parent.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(
        "matey.workflows.schema.plan_scratch_target",
        lambda **_: ScratchPlan(
            target=ScratchTarget(
                engine="postgres",
                scratch_name="scratch",
                scratch_url="postgres://u:p@localhost/scratch",
                cleanup_required=True,
                auto_provisioned=False,
            ),
            cleanup=lambda: None,
        ),
    )
    monkeypatch.setattr("matey.workflows.schema.run_dbmate", lambda **_: 0)
    monkeypatch.setattr(
        "matey.workflows.schema.run_dbmate_capture",
        lambda **_: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="CREATE TABLE t (id INT PRIMARY KEY);\n",
            stderr="",
        ),
    )

    result = validate_schema_clean_target(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url="postgres://u:p@localhost/app",
        test_url=None,
        keep_scratch=False,
        no_repo_check=True,
    )
    assert result.success is True


def test_validate_schema_clean_requires_url_to_infer_engine(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(SchemaValidationError, match="Cannot infer database engine"):
        validate_schema_clean_target(
            target_name="default",
            dbmate_binary=Path("/tmp/dbmate"),
            paths=paths,
            real_url=None,
            test_url=None,
            keep_scratch=False,
            no_repo_check=False,
        )


def test_validate_schema_clean_requires_test_url_for_bigquery(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(SchemaValidationError, match="BigQuery scratch requires --test-url"):
        validate_schema_clean_target(
            target_name="default",
            dbmate_binary=Path("/tmp/dbmate"),
            paths=paths,
            real_url="bigquery://my-project/us/my_dataset",
            test_url=None,
            keep_scratch=False,
            no_repo_check=False,
        )


def test_validate_schema_clean_rejects_spanner_scratch(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(SchemaValidationError, match="Spanner scratch is not supported"):
        validate_schema_clean_target(
            target_name="default",
            dbmate_binary=Path("/tmp/dbmate"),
            paths=paths,
            real_url="spanner-postgres://127.0.0.1:5432/app",
            test_url=None,
            keep_scratch=False,
            no_repo_check=False,
        )


def test_validate_schema_clean_drop_failure_sets_failure(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    paths.schema_file.parent.mkdir(parents=True, exist_ok=True)
    paths.schema_file.write_text("CREATE TABLE t (id INT);\n", encoding="utf-8")

    monkeypatch.setattr(
        "matey.workflows.schema.plan_scratch_target",
        lambda **_: ScratchPlan(
            target=ScratchTarget(
                engine="postgres",
                scratch_name="scratch",
                scratch_url="postgres://u:p@localhost/scratch",
                cleanup_required=True,
                auto_provisioned=False,
            ),
            cleanup=lambda: None,
        ),
    )

    def _fake_run_dbmate(**kwargs):
        if kwargs["verb"] == "drop":
            return 1
        return 0

    monkeypatch.setattr("matey.workflows.schema.run_dbmate", _fake_run_dbmate)
    monkeypatch.setattr(
        "matey.workflows.schema.run_dbmate_capture",
        lambda **_: subprocess.CompletedProcess(
            args=[],
            returncode=0,
            stdout="CREATE TABLE t (id INT);\n",
            stderr="",
        ),
    )

    result = validate_schema_clean_target(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url="postgres://u:p@localhost/app",
        test_url=None,
        keep_scratch=False,
        no_repo_check=False,
    )
    assert result.success is False
    assert result.error == "dbmate drop failed while cleaning scratch target."


def test_validate_schema_rejects_conflicting_modes(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    with pytest.raises(SchemaValidationError, match="Cannot enable both schema-only and path-only"):
        validate_schema_clean_target(
            target_name="default",
            dbmate_binary=Path("/tmp/dbmate"),
            paths=paths,
            real_url="postgres://u:p@localhost/app",
            test_url=None,
            keep_scratch=False,
            no_repo_check=True,
            schema_only=True,
            path_only=True,
        )


def test_validate_schema_reports_upgrade_diff_when_both_checks_enabled(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    paths.schema_file.parent.mkdir(parents=True, exist_ok=True)
    paths.schema_file.write_text("CREATE TABLE t (id INT);\n", encoding="utf-8")

    monkeypatch.setattr(
        "matey.workflows.schema._run_clean_check",
        lambda **_: SimpleNamespace(
            scratch_url="clean://scratch",
            schema_sql="CREATE TABLE t (id INT);\n",
            error=None,
        ),
    )
    monkeypatch.setattr(
        "matey.workflows.schema._run_upgrade_check",
        lambda **_: SimpleNamespace(
            scratch_url="upgrade://scratch",
            schema_sql="CREATE TABLE t (id INT PRIMARY KEY);\n",
            error=None,
        ),
    )

    result = validate_schema_clean_target(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url="postgres://u:p@localhost/app",
        test_url=None,
        keep_scratch=False,
        no_repo_check=False,
        schema_only=False,
        path_only=False,
        no_upgrade_diff=False,
    )
    assert result.success is False
    assert result.diff_text is None
    assert result.upgrade_diff_text is not None
    assert result.scratch_urls == ("clean://scratch", "upgrade://scratch")


def test_validate_schema_path_only_skips_clean_check(tmp_path: Path, monkeypatch) -> None:
    paths = _paths(tmp_path)
    calls = {"clean": 0, "upgrade": 0}

    def _clean(**_kwargs):
        calls["clean"] += 1
        return SimpleNamespace(scratch_url="clean://scratch", schema_sql="ignored", error=None)

    def _upgrade(**_kwargs):
        calls["upgrade"] += 1
        return SimpleNamespace(
            scratch_url="upgrade://scratch",
            schema_sql="CREATE TABLE t (id INT);\n",
            error=None,
        )

    monkeypatch.setattr("matey.workflows.schema._run_clean_check", _clean)
    monkeypatch.setattr("matey.workflows.schema._run_upgrade_check", _upgrade)

    result = validate_schema_clean_target(
        target_name="default",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url="postgres://u:p@localhost/app",
        test_url=None,
        keep_scratch=False,
        no_repo_check=True,
        schema_only=False,
        path_only=True,
        no_upgrade_diff=False,
    )
    assert result.success is True
    assert calls == {"clean": 0, "upgrade": 1}


def test_validate_schema_bigquery_ignores_scratch_dataset_name_in_upgrade_diff(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)

    monkeypatch.setattr(
        "matey.workflows.schema._run_clean_check",
        lambda **_: SimpleNamespace(
            scratch_url="bigquery://proj/us/matey_clean",
            schema_sql="CREATE TABLE `proj.matey_clean.widgets` (id INT64);\n",
            error=None,
        ),
    )
    monkeypatch.setattr(
        "matey.workflows.schema._run_upgrade_check",
        lambda **_: SimpleNamespace(
            scratch_url="bigquery://proj/us/matey_upgrade",
            schema_sql="CREATE TABLE `proj.matey_upgrade.widgets` (id INT64);\n",
            error=None,
        ),
    )

    result = validate_schema_clean_target(
        target_name="bigquery",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url=None,
        test_url="bigquery://proj/us",
        keep_scratch=False,
        no_repo_check=True,
        schema_only=False,
        path_only=False,
        no_upgrade_diff=False,
    )
    assert result.success is True
    assert result.upgrade_diff_text is None


def test_validate_schema_bigquery_upgrade_diff_still_detects_real_schema_changes(
    tmp_path: Path,
    monkeypatch,
) -> None:
    paths = _paths(tmp_path)

    monkeypatch.setattr(
        "matey.workflows.schema._run_clean_check",
        lambda **_: SimpleNamespace(
            scratch_url="bigquery://proj/us/matey_clean",
            schema_sql="CREATE TABLE `proj.matey_clean.widgets` (id INT64);\n",
            error=None,
        ),
    )
    monkeypatch.setattr(
        "matey.workflows.schema._run_upgrade_check",
        lambda **_: SimpleNamespace(
            scratch_url="bigquery://proj/us/matey_upgrade",
            schema_sql="CREATE TABLE `proj.matey_upgrade.widgets` (id INT64, payload STRING);\n",
            error=None,
        ),
    )

    result = validate_schema_clean_target(
        target_name="bigquery",
        dbmate_binary=Path("/tmp/dbmate"),
        paths=paths,
        real_url=None,
        test_url="bigquery://proj/us",
        keep_scratch=False,
        no_repo_check=True,
        schema_only=False,
        path_only=False,
        no_upgrade_diff=False,
    )
    assert result.success is False
    assert result.upgrade_diff_text is not None
