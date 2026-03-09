from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Literal

TemplateProvider = Literal["github", "gitlab", "buildkite"]


def _prefix_workspace_commands(workspace_ref: str, body: str) -> str:
    if workspace_ref == ".":
        return body
    return f"cd {workspace_ref}\n{body}"


def _github_template(*, workspace_ref: str) -> str:
    commands = _prefix_workspace_commands(
        workspace_ref,
        "pixi install\n"
        "pixi run matey lint --all\n"
        "pixi run matey schema status --all\n"
        'pixi run matey schema apply --all --base "${{ github.base_ref }}"\n'
        'if [ -n "$(git status --porcelain)" ]; then\n'
        "  git status --short\n"
        "  git diff\n"
        "  exit 1\n"
        "fi\n",
    )
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
        "      - run: |\n"
        + "".join(f"          {line}\n" for line in commands.rstrip().splitlines())
    )


def _gitlab_template(*, workspace_ref: str) -> str:
    commands = _prefix_workspace_commands(
        workspace_ref,
        "pixi install\n"
        "pixi run matey lint --all\n"
        "pixi run matey schema status --all\n"
        'pixi run matey schema apply --all --base "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME"\n'
        'if [ -n "$(git status --porcelain)" ]; then\n'
        "  git status --short\n"
        "  git diff\n"
        "  exit 1\n"
        "fi\n",
    )
    return (
        "stages:\n"
        "  - validate\n"
        "\n"
        "matey_schema:\n"
        "  stage: validate\n"
        "  image: ghcr.io/prefix-dev/pixi:latest\n"
        "  script:\n" + "".join(f"    - {line}\n" for line in commands.rstrip().splitlines())
    )


def _buildkite_template(*, workspace_ref: str) -> str:
    commands = _prefix_workspace_commands(
        workspace_ref,
        "pixi install\n"
        "pixi run matey lint --all\n"
        "pixi run matey schema status --all\n"
        'pixi run matey schema apply --all --base "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"\n'
        'if [ -n "$(git status --porcelain)" ]; then\n'
        "  git status --short\n"
        "  git diff\n"
        "  exit 1\n"
        "fi\n",
    )
    return 'steps:\n  - label: ":matey: schema"\n    command:\n' + "".join(
        f"      - {line}\n" for line in commands.rstrip().splitlines()
    )


_CI_TEMPLATES: dict[TemplateProvider, Callable[..., str]] = {
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


def render_ci_template(provider: TemplateProvider, *, workspace_ref: str) -> str:
    render = _CI_TEMPLATES.get(provider)
    if render is None:
        raise ValueError(f"Unsupported CI provider: {provider!r}")
    return render(workspace_ref=workspace_ref)


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
