from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Literal

import tomlkit
from tomlkit.items import Table

from matey.config import (
    TARGET_CONFIG_FILE,
    WORKSPACE_CONFIG_FILE,
    CodegenConfig,
    normalize_target_path_ref,
    target_env_stem,
)
from matey.paths import RelativePathError

TemplateProvider = Literal["github", "gitlab", "buildkite"]


def _github_template() -> str:
    return (
        "name: matey-schema\n"
        "on:\n"
        "  pull_request:\n"
        "jobs:\n"
        "  schema:\n"
        "    runs-on: ubuntu-latest\n"
        "    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "      - uses: prefix-dev/setup-pixi@v0\n"
        "      - run: pixi install\n"
        "      - run: pixi run matey lint --all\n"
        "      - run: pixi run matey schema status --all\n"
        '      - run: pixi run matey schema plan --all --base "${{ github.base_ref }}"\n'
    )


def _gitlab_template() -> str:
    return (
        "stages:\n"
        "  - validate\n"
        "\n"
        "matey_schema:\n"
        "  stage: validate\n"
        "  image: ghcr.io/prefix-dev/pixi:latest\n"
        "  script:\n"
        "    - pixi install\n"
        "    - pixi run matey lint --all\n"
        "    - pixi run matey schema status --all\n"
        '    - pixi run matey schema plan --all --base "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME"\n'
    )


def _buildkite_template() -> str:
    return (
        "steps:\n"
        '  - label: ":matey: schema"\n'
        "    command:\n"
        "      - pixi install\n"
        "      - pixi run matey lint --all\n"
        "      - pixi run matey schema status --all\n"
        '      - pixi run matey schema plan --all --base "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"\n'
    )


_CI_TEMPLATES: dict[TemplateProvider, Callable[[], str]] = {
    "github": _github_template,
    "gitlab": _gitlab_template,
    "buildkite": _buildkite_template,
}
_CI_TEMPLATE_PATHS: dict[TemplateProvider, Path] = {
    "github": Path(".github/workflows/matey-schema.yml"),
    "gitlab": Path("matey.gitlab-ci.yml"),
    "buildkite": Path(".buildkite/matey-schema.yml"),
}


def render_workspace_config(target_paths: tuple[str, ...], *, pyproject: bool = False) -> str:
    doc = tomlkit.document()
    section = _workspace_section(doc, pyproject=pyproject, create=True)
    section["targets"] = sorted(_normalized_target_paths(target_paths))
    return tomlkit.dumps(doc)


def update_workspace_text(*, existing_text: str, target_path: str | None, pyproject: bool = False) -> str:
    parsed = tomlkit.parse(existing_text)
    section = _workspace_section(parsed, pyproject=pyproject, create=True)
    targets = section.get("targets")
    if not isinstance(targets, list):
        targets = []
    normalized = _normalize_target_path(target_path) if target_path is not None else None
    values = [str(item) for item in targets]
    if normalized is not None and normalized not in values:
        values.append(normalized)
    section["targets"] = sorted(values)
    return tomlkit.dumps(parsed)


def render_target_config(
    *,
    engine: str,
    url_env: str,
    test_url_env: str,
    codegen: CodegenConfig | None = None,
) -> str:
    doc = tomlkit.document()
    doc["engine"] = engine
    doc["url_env"] = url_env
    doc["test_url_env"] = test_url_env
    _set_codegen(doc, codegen)
    return tomlkit.dumps(doc)


def update_target_config_text(
    *,
    existing_text: str,
    engine: str,
    url_env: str,
    test_url_env: str,
    codegen: CodegenConfig | None = None,
) -> str:
    parsed = tomlkit.parse(existing_text)
    parsed["engine"] = engine
    parsed["url_env"] = url_env
    parsed["test_url_env"] = test_url_env
    _set_codegen(parsed, codegen)
    return tomlkit.dumps(parsed)


def default_ci_template_path(provider: TemplateProvider) -> Path:
    try:
        return _CI_TEMPLATE_PATHS[provider]
    except KeyError as error:
        raise ValueError(f"Unsupported CI provider: {provider!r}") from error


def render_ci_template(provider: TemplateProvider) -> str:
    try:
        render = _CI_TEMPLATES[provider]
    except KeyError as error:
        raise ValueError(f"Unsupported CI provider: {provider!r}") from error
    return render()


def write_text_file(path: Path, content: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def default_target_config_values(path: str) -> tuple[str, str]:
    stem = target_env_stem(path)
    if stem == "DEFAULT":
        return "DATABASE_URL", "TEST_DATABASE_URL"
    return f"{stem}_DATABASE_URL", f"{stem}_TEST_DATABASE_URL"


def workspace_default_path(repo_root: Path) -> Path:
    return repo_root / WORKSPACE_CONFIG_FILE


def target_config_path(target_root: Path) -> Path:
    return target_root / TARGET_CONFIG_FILE


def _set_codegen(doc: Table, codegen: CodegenConfig | None) -> None:
    if codegen is None:
        return
    table = doc.get("codegen")
    if not isinstance(table, Table):
        table = tomlkit.table()
        doc["codegen"] = table
    table["enabled"] = codegen.enabled
    table["generator"] = codegen.generator
    if codegen.options is None:
        if "options" in table:
            del table["options"]
    else:
        table["options"] = codegen.options


def _workspace_section(doc: Table, *, pyproject: bool, create: bool) -> Table:
    if not pyproject:
        return doc
    tool = doc.get("tool")
    if not isinstance(tool, Table):
        if not create:
            raise ValueError("pyproject.toml is missing [tool] table.")
        tool = tomlkit.table()
        doc["tool"] = tool
    matey = tool.get("matey")
    if not isinstance(matey, Table):
        if not create:
            raise ValueError("pyproject.toml is missing [tool.matey] table.")
        matey = tomlkit.table()
        tool["matey"] = matey
    return matey


def _normalize_target_path(path: str | None) -> str | None:
    if path is None:
        return None
    try:
        return normalize_target_path_ref(path, label="target path")
    except RelativePathError as error:
        raise ValueError(str(error)) from error


def _normalized_target_paths(target_paths: tuple[str, ...]) -> tuple[str, ...]:
    return tuple(sorted(filter(None, (_normalize_target_path(path) for path in target_paths))))


__all__ = [
    "TemplateProvider",
    "default_ci_template_path",
    "default_target_config_values",
    "render_ci_template",
    "render_target_config",
    "render_workspace_config",
    "target_config_path",
    "update_target_config_text",
    "update_workspace_text",
    "workspace_default_path",
    "write_text_file",
]
