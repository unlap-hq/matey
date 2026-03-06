from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

TemplateProvider = Literal["github", "gitlab", "buildkite"]

_TARGET_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_-]+$")


def render_config_template(targets: tuple[str, ...]) -> str:
    normalized_targets = _normalize_targets(targets)
    lines = [
        'dir = "db"',
        'url_env = "DATABASE_URL"',
        'test_url_env = "TEST_DATABASE_URL"',
    ]
    for target in normalized_targets:
        env_stem = _target_env_stem(target)
        lines.extend(
            [
                "",
                f"[{target}]",
                f'url_env = "{env_stem}_DATABASE_URL"',
                f'test_url_env = "{env_stem}_TEST_DATABASE_URL"',
            ]
        )
    return "\n".join(lines) + "\n"


def default_ci_template_path(provider: TemplateProvider) -> Path:
    if provider == "github":
        return Path(".github/workflows/matey-schema.yml")
    if provider == "gitlab":
        return Path(".gitlab-ci.matey.yml")
    if provider == "buildkite":
        return Path(".buildkite/matey-schema.yml")
    raise ValueError(f"Unsupported CI provider: {provider!r}")


def render_ci_template(provider: TemplateProvider) -> str:
    if provider == "github":
        return _github_template()
    if provider == "gitlab":
        return _gitlab_template()
    if provider == "buildkite":
        return _buildkite_template()
    raise ValueError(f"Unsupported CI provider: {provider!r}")


def write_text_file(path: Path, content: str, *, overwrite: bool) -> None:
    if path.exists() and not overwrite:
        raise FileExistsError(f"Refusing to overwrite existing file: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _normalize_targets(targets: tuple[str, ...]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for raw in targets:
        value = raw.strip()
        if not value:
            raise ValueError("Target names cannot be empty.")
        if not _TARGET_NAME_PATTERN.fullmatch(value):
            raise ValueError(f"Invalid target name: {value!r}")
        if value in seen:
            raise ValueError(f"Duplicate target name: {value!r}")
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _target_env_stem(target: str) -> str:
    return target.replace("-", "_").upper()


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


__all__ = [
    "TemplateProvider",
    "default_ci_template_path",
    "render_ci_template",
    "render_config_template",
    "write_text_file",
]
