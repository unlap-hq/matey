from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


class MateyError(Exception):
    """Base exception for matey domain errors."""


class ConfigError(MateyError):
    """Invalid or missing configuration."""


class TargetSelectionError(MateyError):
    """Invalid target selection for the current config."""


class PathResolutionError(MateyError):
    """Failed to derive migrations/schema paths."""


class URLResolutionError(MateyError):
    """Failed to resolve the database URL for a target."""


class ScratchProvisionError(MateyError):
    """Failed to provision scratch infrastructure."""


class SchemaValidationError(MateyError):
    """Schema validation failed before diff evaluation."""


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
