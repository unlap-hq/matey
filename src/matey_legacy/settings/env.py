from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from environs import Env

DEFAULT_DBMATE_WAIT_TIMEOUT = "45s"
DEFAULT_DBMATE_MODULE = "github.com/amacneil/dbmate/v2"
DEFAULT_DBMATE_VERSION = "v2.31.0"
DEFAULT_DBMATE_SOURCE = "go-install"
DEFAULT_DBMATE_CGO_ENABLED = "1"
DEFAULT_GO_LICENSES_MODULE = "github.com/google/go-licenses/v2"
DEFAULT_GO_LICENSES_VERSION = "v2.0.1"
DEFAULT_GO_LICENSES_DISALLOWED_TYPES = "forbidden,restricted,unknown"
DEFAULT_GO_LICENSES_ENFORCE = False


class _MappedEnv(Env):
    def __init__(self, environ: Mapping[str, str]) -> None:
        super().__init__()
        self._mapping = environ

    def _get_value(self, env_key: str, default: Any) -> Any:
        return self._mapping.get(env_key, default)


def _optional(value: str) -> str | None:
    stripped = value.strip()
    return stripped or None


def _parse_bool(raw: str | None, *, name: str, default: bool) -> bool:
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "t", "yes", "y", "on"}:
        return True
    if normalized in {"0", "false", "f", "no", "n", "off"}:
        return False
    raise ValueError(f"Invalid boolean value for {name}: {raw!r}")


@dataclass(frozen=True)
class RuntimeEnv:
    values: Mapping[str, str]
    dbmate_bin: str | None
    dbmate_wait_timeout: str
    github_base_ref: str | None
    gitlab_base_ref: str | None
    buildkite_base_ref: str | None

    def get(self, name: str) -> str | None:
        return self.values.get(name)


@dataclass(frozen=True)
class BuildEnv:
    values: Mapping[str, str]
    dbmate_source: str
    dbmate_module: str
    dbmate_version: str
    dbmate_cgo_enabled: str
    go_licenses_module: str
    go_licenses_version: str
    go_licenses_disallowed_types: str
    go_licenses_enforce: bool

    def get(self, name: str) -> str | None:
        return self.values.get(name)


def _values_from_environ(environ: Mapping[str, str] | None = None) -> dict[str, str]:
    return dict(os.environ) if environ is None else dict(environ)


def load_runtime_env(environ: Mapping[str, str] | None = None) -> RuntimeEnv:
    values = _values_from_environ(environ)
    env = _MappedEnv(values)

    dbmate_bin = _optional(env.str("MATEY_DBMATE_BIN", ""))
    wait_timeout = (
        _optional(env.str("MATEY_DBMATE_WAIT_TIMEOUT", "")) or DEFAULT_DBMATE_WAIT_TIMEOUT
    )
    github_base_ref = _optional(env.str("GITHUB_BASE_REF", ""))
    gitlab_base_ref = _optional(env.str("CI_MERGE_REQUEST_TARGET_BRANCH_NAME", ""))
    buildkite_base_ref = _optional(env.str("BUILDKITE_PULL_REQUEST_BASE_BRANCH", ""))

    return RuntimeEnv(
        values=values,
        dbmate_bin=dbmate_bin,
        dbmate_wait_timeout=wait_timeout,
        github_base_ref=github_base_ref,
        gitlab_base_ref=gitlab_base_ref,
        buildkite_base_ref=buildkite_base_ref,
    )


def load_build_env(environ: Mapping[str, str] | None = None) -> BuildEnv:
    values = _values_from_environ(environ)
    env = _MappedEnv(values)

    source = _optional(env.str("MATEY_DBMATE_SOURCE", "")) or DEFAULT_DBMATE_SOURCE
    module = _optional(env.str("MATEY_DBMATE_MODULE", "")) or DEFAULT_DBMATE_MODULE
    version = _optional(env.str("MATEY_DBMATE_VERSION", "")) or DEFAULT_DBMATE_VERSION
    cgo_enabled = _optional(env.str("MATEY_DBMATE_CGO_ENABLED", "")) or DEFAULT_DBMATE_CGO_ENABLED
    go_licenses_module = (
        _optional(env.str("MATEY_GO_LICENSES_MODULE", "")) or DEFAULT_GO_LICENSES_MODULE
    )
    go_licenses_version = (
        _optional(env.str("MATEY_GO_LICENSES_VERSION", "")) or DEFAULT_GO_LICENSES_VERSION
    )
    go_licenses_disallowed_types = (
        _optional(env.str("MATEY_GO_LICENSES_DISALLOWED_TYPES", ""))
        or DEFAULT_GO_LICENSES_DISALLOWED_TYPES
    )
    go_licenses_enforce = _parse_bool(
        _optional(env.str("MATEY_GO_LICENSES_ENFORCE", "")),
        name="MATEY_GO_LICENSES_ENFORCE",
        default=DEFAULT_GO_LICENSES_ENFORCE,
    )

    return BuildEnv(
        values=values,
        dbmate_source=source,
        dbmate_module=module,
        dbmate_version=version,
        dbmate_cgo_enabled=cgo_enabled,
        go_licenses_module=go_licenses_module,
        go_licenses_version=go_licenses_version,
        go_licenses_disallowed_types=go_licenses_disallowed_types,
        go_licenses_enforce=go_licenses_enforce,
    )
