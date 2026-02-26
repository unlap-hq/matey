from __future__ import annotations

from pathlib import Path

from matey.domain import DefaultsConfig, MateyConfig, SelectedTarget, TargetConfig
from matey.settings.resolve import derive_paths


def test_derive_paths_in_implicit_single_target_mode(tmp_path: Path) -> None:
    config = MateyConfig(defaults=DefaultsConfig(dir="db"), targets={})
    target = SelectedTarget(name="default", config=None, implicit=True)

    resolved = derive_paths(config, target, dir_override=None, cwd=tmp_path)

    assert resolved.db_dir == tmp_path / "db"
    assert resolved.migrations_dir == tmp_path / "db" / "migrations"
    assert resolved.schema_file == tmp_path / "db" / "schema.sql"


def test_derive_paths_in_named_target_mode(tmp_path: Path) -> None:
    config = MateyConfig(
        defaults=DefaultsConfig(dir="db"),
        targets={"core": TargetConfig(name="core", url_env="CORE_URL")},
    )
    target = SelectedTarget(name="core", config=config.targets["core"], implicit=False)

    resolved = derive_paths(config, target, dir_override=None, cwd=tmp_path)

    assert resolved.db_dir == tmp_path / "db" / "core"
    assert resolved.migrations_dir == tmp_path / "db" / "core" / "migrations"
    assert resolved.schema_file == tmp_path / "db" / "core" / "schema.sql"


def test_derive_paths_honors_target_dir_then_cli_dir_override(tmp_path: Path) -> None:
    config = MateyConfig(
        defaults=DefaultsConfig(dir="db"),
        targets={"core": TargetConfig(name="core", url_env="CORE_URL", dir="services/core/db")},
    )
    target = SelectedTarget(name="core", config=config.targets["core"], implicit=False)

    from_target = derive_paths(config, target, dir_override=None, cwd=tmp_path)
    assert from_target.db_dir == tmp_path / "services" / "core" / "db" / "core"

    from_cli = derive_paths(config, target, dir_override=Path("db_override"), cwd=tmp_path)
    assert from_cli.db_dir == tmp_path / "db_override" / "core"
