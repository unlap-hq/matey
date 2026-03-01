from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class DefaultsConfig:
    dir: str = "db"
    url_env: str = "MATEY_URL"
    test_url_env: str = "MATEY_TEST_URL"


@dataclass(frozen=True)
class TargetConfig:
    name: str
    url_env: str | None = None
    dir: str | None = None
    test_url_env: str | None = None


@dataclass(frozen=True)
class MateyConfig:
    defaults: DefaultsConfig = field(default_factory=DefaultsConfig)
    targets: dict[str, TargetConfig] = field(default_factory=dict)
    source_path: Path | None = None


@dataclass(frozen=True)
class SelectedTarget:
    name: str
    config: TargetConfig | None
    implicit: bool


@dataclass(frozen=True)
class ResolvedPaths:
    db_dir: Path
    migrations_dir: Path
    schema_file: Path


@dataclass(frozen=True)
class ScratchTarget:
    engine: str
    scratch_name: str
    scratch_url: str
    cleanup_required: bool
    auto_provisioned: bool
