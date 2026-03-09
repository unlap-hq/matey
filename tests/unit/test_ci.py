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
    TargetConfig,
    Workspace,
    default_target_config_values,
)


def test_render_workspace_config_default() -> None:
    workspace = Workspace(
        root=Path(), repo_root=None, config_path=Path("missing.toml"), config_kind="workspace", targets=()
    )
    rendered = workspace.render_config(target_paths=())
    assert rendered == "targets = []\n"


def test_render_workspace_config_targets() -> None:
    workspace = Workspace(
        root=Path(), repo_root=None, config_path=Path("missing.toml"), config_kind="workspace", targets=()
    )
    rendered = workspace.update_config(
        existing_text='targets = ["services/analytics/db"]\n',
        target_path="db/core",
    )
    assert '"db/core"' in rendered
    assert '"services/analytics/db"' in rendered


def test_update_workspace_text_adds_target() -> None:
    workspace = Workspace(
        root=Path(), repo_root=None, config_path=Path("missing.toml"), config_kind="workspace", targets=()
    )
    rendered = workspace.update_config(
        existing_text='targets = ["db/core"]\n',
        target_path="db/analytics",
    )
    assert '"db/core"' in rendered
    assert '"db/analytics"' in rendered


def test_render_target_config_default() -> None:
    rendered = TargetConfig(
        name=".",
        root=Path(),
        engine="postgres",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
        codegen=None,
    ).render_config()
    assert 'engine = "postgres"' in rendered
    assert 'url_env = "DATABASE_URL"' in rendered
    assert "[codegen]" not in rendered


def test_render_target_config_with_codegen() -> None:
    rendered = TargetConfig(
        name=".",
        root=Path(),
        engine="postgres",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
        codegen=CodegenConfig(enabled=True, generator="tables", options=None),
    ).render_config()
    assert "[codegen]" in rendered
    assert "enabled = true" in rendered
    assert 'out = "models.py"' not in rendered
    assert '#  options = "..."' in rendered


def test_update_target_config_preserves_unknown_keys() -> None:
    rendered = TargetConfig(
        name=".",
        root=Path(),
        engine="postgres",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
        codegen=None,
    ).render_config(
        existing_text='engine = "sqlite"\nurl_env = "OLD_URL"\ntest_url_env = "OLD_TEST_URL"\ncustom = "keep"\n'
    )
    assert 'engine = "postgres"' in rendered
    assert 'url_env = "DATABASE_URL"' in rendered
    assert 'custom = "keep"' in rendered


def test_default_target_config_values() -> None:
    assert default_target_config_values("db/core") == (
        "DB_CORE_DATABASE_URL",
        "DB_CORE_TEST_DATABASE_URL",
    )
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
    content = render_ci_template(provider, workspace_ref=".")
    assert "pixi run matey lint --all" in content
    assert "pixi run matey schema apply --all --base" in content
    assert "git status --porcelain" in content
    assert expected_base_var in content


def test_render_ci_template_monorepo_workspace() -> None:
    content = render_ci_template("github", workspace_ref="services/db")
    assert "cd services/db" in content
    assert 'pixi run matey schema apply --all --base "${{ github.base_ref }}"' in content


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
