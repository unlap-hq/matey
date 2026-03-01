from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ConfigDefaults:
    dir: str = "db"
    url_env: str = "MATEY_URL"
    test_url_env: str = "MATEY_TEST_URL"


@dataclass(frozen=True)
class ConfigTarget:
    dir: str | None = None
    url_env: str | None = None
    test_url_env: str | None = None


@dataclass(frozen=True)
class MateyConfig:
    defaults: ConfigDefaults = ConfigDefaults()
    targets: dict[str, ConfigTarget] | None = None


@dataclass(frozen=True)
class ResolvedTargetConfig:
    name: str
    db_dir: Path
    url_env: str
    test_url_env: str
