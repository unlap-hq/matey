from __future__ import annotations

import warnings
from enum import StrEnum
from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from types import ModuleType

from .project import ConfigError, TargetConfig, Workspace

warnings.filterwarnings(
    "ignore",
    message=r"urllib3 .* doesn't match a supported version!",
    category=Warning,
    module=r"requests(\..*)?",
)
warnings.filterwarnings(
    "ignore",
    message=r"The @wait_container_is_ready decorator is deprecated.*",
    category=DeprecationWarning,
    module=r"testcontainers\.clickhouse(\..*)?",
)


class Engine(StrEnum):
    POSTGRES = "postgres"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    CLICKHOUSE = "clickhouse"
    BIGQUERY = "bigquery"
    BIGQUERY_EMULATOR = "bigquery-emulator"

_LAZY_MODULES = {
    "cli",
    "db",
    "dbmate",
    "lockfile",
    "repo",
    "data",
    "schema",
    "scratch",
    "sql",
    "tx",
}

try:
    __version__ = version("matey")
except PackageNotFoundError:
    __version__ = "0.0.0"


def __getattr__(name: str) -> ModuleType:
    if name not in _LAZY_MODULES:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(f".{name}", __name__)
    globals()[name] = module
    return module


__all__ = [
    "ConfigError",
    "Engine",
    "TargetConfig",
    "Workspace",
    "__version__",
]
