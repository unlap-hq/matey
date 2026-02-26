from __future__ import annotations

import pytest

from matey.domain import (
    DefaultsConfig,
    MateyConfig,
    SelectedTarget,
    TargetConfig,
    URLResolutionError,
)
from matey.settings.resolve import resolve_real_url, resolve_test_url


def test_resolve_real_url_uses_cli_override_first() -> None:
    config = MateyConfig(defaults=DefaultsConfig(url_env="MATEY_URL"), targets={})
    target = SelectedTarget(name="default", config=None, implicit=True)

    resolved = resolve_real_url(
        config,
        target,
        cli_url="postgres://cli-url/db",
        environ={"MATEY_URL": "postgres://env-url/db"},
    )
    assert resolved == "postgres://cli-url/db"


def test_resolve_real_url_uses_target_env_first() -> None:
    config = MateyConfig(
        defaults=DefaultsConfig(url_env="MATEY_URL"),
        targets={"core": TargetConfig(name="core", url_env="CORE_URL")},
    )
    target = SelectedTarget(name="core", config=config.targets["core"], implicit=False)

    resolved = resolve_real_url(
        config,
        target,
        cli_url=None,
        environ={"MATEY_URL": "postgres://default/db", "CORE_URL": "postgres://core/db"},
    )
    assert resolved == "postgres://core/db"


def test_resolve_real_url_raises_when_env_missing() -> None:
    config = MateyConfig(defaults=DefaultsConfig(url_env="MATEY_URL"), targets={})
    target = SelectedTarget(name="default", config=None, implicit=True)

    with pytest.raises(URLResolutionError, match="Missing database URL"):
        resolve_real_url(config, target, cli_url=None, environ={})


def test_resolve_test_url_returns_none_when_not_set() -> None:
    config = MateyConfig(defaults=DefaultsConfig(test_url_env="MATEY_TEST_URL"), targets={})
    target = SelectedTarget(name="default", config=None, implicit=True)
    assert resolve_test_url(config, target, cli_test_url=None, environ={}) is None
