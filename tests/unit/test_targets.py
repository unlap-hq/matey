from __future__ import annotations

import pytest

from matey.domain import DefaultsConfig, MateyConfig, TargetConfig, TargetSelectionError
from matey.settings.resolve import select_targets


def test_select_targets_implicit_single_target_when_no_targets_configured() -> None:
    config = MateyConfig(defaults=DefaultsConfig(), targets={})
    selected = select_targets(config, target_name=None, all_targets=False)

    assert len(selected) == 1
    assert selected[0].name == "default"
    assert selected[0].implicit is True


def test_select_targets_requires_target_when_multiple_targets_exist() -> None:
    config = MateyConfig(
        defaults=DefaultsConfig(),
        targets={
            "core": TargetConfig(name="core", url_env="CORE_URL"),
            "analytics": TargetConfig(name="analytics", url_env="ANALYTICS_URL"),
        },
    )

    with pytest.raises(TargetSelectionError, match="Multiple targets configured"):
        select_targets(config, target_name=None, all_targets=False)


def test_select_targets_supports_all_flag() -> None:
    config = MateyConfig(
        defaults=DefaultsConfig(),
        targets={
            "core": TargetConfig(name="core", url_env="CORE_URL"),
            "analytics": TargetConfig(name="analytics", url_env="ANALYTICS_URL"),
        },
    )

    selected = select_targets(config, target_name=None, all_targets=True)
    assert [target.name for target in selected] == ["core", "analytics"]


def test_select_targets_rejects_target_and_all_combination() -> None:
    config = MateyConfig(
        defaults=DefaultsConfig(),
        targets={"core": TargetConfig(name="core", url_env="CORE_URL")},
    )
    with pytest.raises(TargetSelectionError, match="Cannot use --target and --all together"):
        select_targets(config, target_name="core", all_targets=True)
