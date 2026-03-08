from __future__ import annotations

from pathlib import Path

import pytest

from matey.cli.ci import (
    TemplateProvider,
    default_ci_template_path,
    render_ci_template,
    write_text_file,
)
from matey.project import (
    CodegenConfig,
    ConfigEditor,
    default_target_config_values,
)


def test_render_workspace_config_default() -> None:
    rendered = ConfigEditor("workspace").render_workspace(target_paths=())
    assert rendered == "targets = []\n"


def test_render_workspace_config_targets() -> None:
    rendered = ConfigEditor("workspace").render_workspace(target_paths=("db/core", "services/analytics/db"))
    assert '"db/core"' in rendered
    assert '"services/analytics/db"' in rendered


def test_update_workspace_text_adds_target() -> None:
    rendered = ConfigEditor("workspace").update_workspace(
        existing_text="targets = [\"db/core\"]\n",
        target_path="db/analytics",
    )
    assert '"db/core"' in rendered
    assert '"db/analytics"' in rendered


def test_render_target_config_default() -> None:
    rendered = ConfigEditor("workspace").render_target(
        engine="postgres",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
    )
    assert 'engine = "postgres"' in rendered
    assert 'url_env = "DATABASE_URL"' in rendered
    assert '[codegen]' not in rendered


def test_render_target_config_with_codegen() -> None:
    rendered = ConfigEditor("workspace").render_target(
        engine="postgres",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
        codegen=CodegenConfig(enabled=True, generator="tables", options=None),
    )
    assert '[codegen]' in rendered
    assert 'enabled = true' in rendered
    assert 'out = "models.py"' not in rendered
    assert '#  options = "..."' in rendered


def test_update_target_config_preserves_unknown_keys() -> None:
    rendered = ConfigEditor("workspace").update_target(
        existing_text='engine = "sqlite"\nurl_env = "OLD_URL"\ntest_url_env = "OLD_TEST_URL"\ncustom = "keep"\n',
        engine="postgres",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
    )
    assert 'engine = "postgres"' in rendered
    assert 'url_env = "DATABASE_URL"' in rendered
    assert 'custom = "keep"' in rendered


def test_default_target_config_values() -> None:
    assert default_target_config_values("db/core") == ("DB_CORE_DATABASE_URL", "DB_CORE_TEST_DATABASE_URL")
    assert default_target_config_values(".") == ("DATABASE_URL", "TEST_DATABASE_URL")


@pytest.mark.parametrize(
    ("provider", "expected_base_var"),
    [
        ("github", "${{ github.base_ref }}"),
        ("gitlab", "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME"),
        ("buildkite", "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"),
    ],
)
def test_render_ci_template(provider: TemplateProvider, expected_base_var: str) -> None:
    content = render_ci_template(provider)
    assert "pixi run matey lint --all" in content
    assert expected_base_var in content


def test_default_ci_template_path() -> None:
    assert default_ci_template_path("github") == Path(".github/workflows/matey-schema.yml")


def test_write_text_file_overwrite_rules(tmp_path: Path) -> None:
    path = tmp_path / "out.txt"
    write_text_file(path, "first\n", overwrite=False)
    assert path.read_text(encoding="utf-8") == "first\n"

    with pytest.raises(FileExistsError):
        write_text_file(path, "second\n", overwrite=False)

    write_text_file(path, "second\n", overwrite=True)
    assert path.read_text(encoding="utf-8") == "second\n"
