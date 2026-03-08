from __future__ import annotations

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from types import ModuleType

from .project import ConfigError, TargetConfig, Workspace

_LAZY_MODULES = {
    "cli",
    "db",
    "dbmate",
    "lockfile",
    "repo",
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
    "TargetConfig",
    "Workspace",
    "__version__",
]
