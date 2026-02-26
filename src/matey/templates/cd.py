from __future__ import annotations

from pathlib import Path

from matey.templates.ci import CIProvider
from matey.templates.types import TemplateFile


def _github_content(targets: list[str]) -> str:
    if not targets:
        return (
            "name: matey-schema-cd\n"
            "on:\n"
            "  workflow_dispatch:\n"
            "jobs:\n"
            "  schema-cd:\n"
            "    runs-on: ubuntu-latest\n"
            "    steps:\n"
            "      - uses: actions/checkout@v4\n"
            "      - uses: prefix-dev/setup-pixi@v0\n"
            "      - run: pixi install\n"
            "      - run: pixi run matey up\n"
            "      - run: pixi run matey schema diff --live --expected repo\n"
        )

    matrix_targets = ", ".join(f'"{target}"' for target in targets)
    return (
        "name: matey-schema-cd\n"
        "on:\n"
        "  workflow_dispatch:\n"
        "jobs:\n"
        "  schema-cd:\n"
        "    runs-on: ubuntu-latest\n"
        "    strategy:\n"
        "      fail-fast: false\n"
        "      matrix:\n"
        f"        target: [{matrix_targets}]\n"
        "    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "      - uses: prefix-dev/setup-pixi@v0\n"
        "      - run: pixi install\n"
        "      - run: pixi run matey --target \"${{ matrix.target }}\" up\n"
        "      - run: pixi run matey --target \"${{ matrix.target }}\" schema diff --live --expected repo\n"
    )


def _gitlab_content(targets: list[str]) -> str:
    if not targets:
        return (
            "stages:\n"
            "  - deploy\n"
            "\n"
            "matey_schema_cd:\n"
            "  stage: deploy\n"
            "  image: ghcr.io/prefix-dev/pixi:latest\n"
            "  script:\n"
            "    - pixi install\n"
            "    - pixi run matey up\n"
            "    - pixi run matey schema diff --live --expected repo\n"
        )

    lines: list[str] = []
    for target in targets:
        lines.append(f"      pixi run matey --target {target} up")
        lines.append(f"      pixi run matey --target {target} schema diff --live --expected repo")
    joined = "\n".join(lines)
    return (
        "stages:\n"
        "  - deploy\n"
        "\n"
        "matey_schema_cd:\n"
        "  stage: deploy\n"
        "  image: ghcr.io/prefix-dev/pixi:latest\n"
        "  script:\n"
        "    - pixi install\n"
        "    - |\n"
        f"{joined}\n"
    )


def _buildkite_content(targets: list[str]) -> str:
    if not targets:
        return (
            "steps:\n"
            "  - label: \":matey: schema cd\"\n"
            "    command:\n"
            "      - pixi install\n"
            "      - pixi run matey up\n"
            "      - pixi run matey schema diff --live --expected repo\n"
        )

    steps = [
        "\n".join(
            [
                f'  - label: ":matey: schema cd ({target})"',
                "    command:",
                "      - pixi install",
                f"      - pixi run matey --target {target} up",
                f"      - pixi run matey --target {target} schema diff --live --expected repo",
            ]
        )
        for target in targets
    ]
    return "steps:\n" + "\n".join(steps) + "\n"


def render_cd_template(provider: CIProvider, *, targets: list[str]) -> TemplateFile:
    if provider == "github":
        return TemplateFile(path=Path(".github/workflows/matey-schema-cd.yml"), content=_github_content(targets))
    if provider == "gitlab":
        return TemplateFile(path=Path(".gitlab-ci.matey-cd.yml"), content=_gitlab_content(targets))
    if provider == "buildkite":
        return TemplateFile(path=Path(".buildkite/matey-schema-cd.yml"), content=_buildkite_content(targets))
    raise ValueError(f"Unsupported provider: {provider}")
