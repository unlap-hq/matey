from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from . import cli, db, dbmate, lockfile, repo, schema, scratch, sql, tx
from .config import Config, ConfigError, TargetConfig

try:
    __version__ = version("matey")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__ = [
    "Config",
    "ConfigError",
    "TargetConfig",
    "__version__",
    "cli",
    "db",
    "dbmate",
    "lockfile",
    "repo",
    "schema",
    "scratch",
    "sql",
    "tx",
]
