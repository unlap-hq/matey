from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Literal

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


def default_ci_template_path(provider: TemplateProvider) -> Path:
    path = _CI_TEMPLATE_PATHS.get(provider)
    if path is None:
        raise ValueError(f"Unsupported CI provider: {provider!r}")
    return path


def render_ci_template(provider: TemplateProvider) -> str:
    render = _CI_TEMPLATES.get(provider)
    if render is None:
        raise ValueError(f"Unsupported CI provider: {provider!r}")
    return render()


def write_text_file(path: Path, content: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


__all__ = [
    "TemplateProvider",
    "default_ci_template_path",
    "render_ci_template",
    "write_text_file",
]
