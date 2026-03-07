from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Literal

from matey.config import (
    DEFAULT_CONFIG_VALUES,
    ConfigError,
    normalize_target_names,
    target_env_stem,
)

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
        "    - pixi run matey schema status --all\n"
        '    - pixi run matey schema plan --all --base "$CI_MERGE_REQUEST_TARGET_BRANCH_NAME"\n'
    )


def _buildkite_template() -> str:
    return (
        "steps:\n"
        '  - label: ":matey: schema"\n'
        "    command:\n"
        "      - pixi install\n"
        "      - pixi run matey schema status --all\n"
        '      - pixi run matey schema plan --all --base "$BUILDKITE_PULL_REQUEST_BASE_BRANCH"\n'
    )


_CI_TEMPLATES: dict[TemplateProvider, Callable[[], str]] = {
    "github": _github_template,
    "gitlab": _gitlab_template,
    "buildkite": _buildkite_template,
}


def render_config_template(targets: tuple[str, ...]) -> str:
    try:
        normalized_targets = normalize_target_names(targets)
    except ConfigError as error:
        raise ValueError(str(error)) from error
    _require_unique_env_stems(normalized_targets)
    lines = [
        f'dir = "{DEFAULT_CONFIG_VALUES["dir"]}"',
        f'url_env = "{DEFAULT_CONFIG_VALUES["url_env"]}"',
        f'test_url_env = "{DEFAULT_CONFIG_VALUES["test_url_env"]}"',
    ]
    for target in normalized_targets:
        env_stem = target_env_stem(target)
        lines.extend(
            [
                "",
                f"[{target}]",
                f'url_env = "{env_stem}_DATABASE_URL"',
                f'test_url_env = "{env_stem}_TEST_DATABASE_URL"',
            ]
        )
    return "\n".join(lines) + "\n"


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


def _require_unique_env_stems(targets: tuple[str, ...]) -> None:
    seen: dict[str, str] = {}
    for target in targets:
        stem = target_env_stem(target)
        previous = seen.get(stem)
        if previous is not None:
            raise ValueError(
                f"Targets {previous!r} and {target!r} normalize to the same env stem {stem!r}."
            )
        seen[stem] = target


__all__ = [
    "TemplateProvider",
    "render_ci_template",
    "render_config_template",
    "write_text_file",
]
