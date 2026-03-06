from __future__ import annotations

from pathlib import Path

import pytest

from matey.cli.template import (
    TemplateProvider,
    default_ci_template_path,
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
    assert 'url_env = "CORE_DATABASE_URL"' in rendered
    assert "[analytics-v2]" in rendered
    assert 'url_env = "ANALYTICS_V2_DATABASE_URL"' in rendered


def test_render_config_template_rejects_invalid_target() -> None:
    with pytest.raises(ValueError, match="Invalid target name"):
        render_config_template(("bad.name",))


@pytest.mark.parametrize(
    ("provider", "expected_path", "expected_base_var"),
    [
        ("github", Path(".github/workflows/matey-schema.yml"), "${{ github.base_ref }}"),
        ("gitlab", Path(".gitlab-ci.matey.yml"), "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME"),
        ("buildkite", Path(".buildkite/matey-schema.yml"), "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"),
    ],
)
def test_render_ci_template(
    provider: TemplateProvider,
    expected_path: Path,
    expected_base_var: str,
) -> None:
    path = default_ci_template_path(provider)
    content = render_ci_template(provider)
    assert path == expected_path
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
