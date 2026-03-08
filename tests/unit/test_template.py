from __future__ import annotations

from pathlib import Path

import pytest

from matey.cli.template import (
    TemplateProvider,
    render_ci_template,
    render_config_template,
    write_text_file,
)


def test_render_config_template_default() -> None:
    rendered = render_config_template(())
    assert rendered == (
        'dir = "db"\n'
        'url_env = "DATABASE_URL"\n'
        'test_url_env = "TEST_DATABASE_URL"\n'
    )


def test_render_config_template_targets() -> None:
    rendered = render_config_template(("core", "analytics-v2"))
    assert "[core]" in rendered
    assert 'dir = "core"' not in rendered
    assert 'url_env = "CORE_DATABASE_URL"' in rendered
    assert "[analytics-v2]" in rendered
    assert 'dir = "analytics-v2"' not in rendered
    assert 'url_env = "ANALYTICS_V2_DATABASE_URL"' in rendered


def test_render_config_template_rejects_invalid_target() -> None:
    with pytest.raises(ValueError, match="Invalid target name"):
        render_config_template(("bad.name",))


def test_render_config_template_prefixes_digit_leading_env_stem() -> None:
    rendered = render_config_template(("1foo",))

    assert 'url_env = "_1FOO_DATABASE_URL"' in rendered
    assert 'test_url_env = "_1FOO_TEST_DATABASE_URL"' in rendered


def test_render_config_template_rejects_colliding_env_stems() -> None:
    with pytest.raises(ValueError, match="normalize to the same env stem"):
        render_config_template(("alpha-beta", "alpha_beta"))


@pytest.mark.parametrize(
    ("provider", "expected_base_var"),
    [
        ("github", "${{ github.base_ref }}"),
        ("gitlab", "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME"),
        ("buildkite", "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"),
    ],
)
def test_render_ci_template(
    provider: TemplateProvider,
    expected_base_var: str,
) -> None:
    content = render_ci_template(provider)
    assert "pixi run matey schema status --all" in content
    assert expected_base_var in content


def test_write_text_file_overwrite_rules(tmp_path: Path) -> None:
    path = tmp_path / "out.txt"
    write_text_file(path, "first\n", overwrite=False)
    assert path.read_text(encoding="utf-8") == "first\n"

    with pytest.raises(FileExistsError):
        write_text_file(path, "second\n", overwrite=False)

    write_text_file(path, "second\n", overwrite=True)
    assert path.read_text(encoding="utf-8") == "second\n"
