from __future__ import annotations

from dataclasses import dataclass

import typed_settings

from matey.app.protocols import IEnvProvider


@dataclass(frozen=True)
class RuntimeSettings:
    matey_dbmate_bin: str = ""
    matey_dbmate_wait_timeout: int = 60
    github_base_ref: str = ""
    ci_merge_request_target_branch_name: str = ""
    buildkite_pull_request_base_branch: str = ""


class TypedSettingsEnvProvider(IEnvProvider):
    def __init__(self) -> None:
        self._settings = typed_settings.load(
            RuntimeSettings,
            "matey",
            config_files=(),
            env_prefix=None,
        )

    @property
    def settings(self) -> RuntimeSettings:
        return self._settings

    def get(self, key: str, default: str | None = None) -> str | None:
        import os

        return os.environ.get(key, default)

    def require(self, key: str) -> str:
        value = self.get(key)
        if value is None or not value.strip():
            raise KeyError(f"Required environment variable is missing: {key}")
        return value.strip()


def normalized_optional(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None
