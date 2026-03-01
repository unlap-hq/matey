from __future__ import annotations

from pathlib import Path
from typing import Literal

from matey.templates.types import TemplateFile

CIProvider = Literal["github", "gitlab", "buildkite"]


def _github_content(targets: list[str]) -> str:
    if not targets:
        return (
            "name: matey-schema-validate\n"
            "on:\n"
            "  pull_request:\n"
            "  push:\n"
            "    branches: [main]\n"
            "jobs:\n"
            "  schema-validate:\n"
            "    runs-on: ubuntu-latest\n"
            "    steps:\n"
            "      - uses: actions/checkout@v4\n"
            "      - uses: prefix-dev/setup-pixi@v0\n"
            "      - run: pixi install\n"
            "      - run: pixi run matey lock doctor\n"
            "      - run: pixi run matey schema validate\n"
        )

    matrix_targets = ", ".join(f'"{target}"' for target in targets)
    return (
        "name: matey-schema-validate\n"
        "on:\n"
        "  pull_request:\n"
        "  push:\n"
        "    branches: [main]\n"
        "jobs:\n"
        "  schema-validate:\n"
        "    runs-on: ubuntu-latest\n"
        "    strategy:\n"
        "      fail-fast: false\n"
        "      matrix:\n"
        f"        target: [{matrix_targets}]\n"
        "    steps:\n"
        "      - uses: actions/checkout@v4\n"
        "      - uses: prefix-dev/setup-pixi@v0\n"
        "      - run: pixi install\n"
        "      - run: pixi run matey --target \"${{ matrix.target }}\" lock doctor\n"
        "      - run: pixi run matey --target \"${{ matrix.target }}\" schema validate\n"
    )


def _gitlab_content(targets: list[str]) -> str:
    if not targets:
        return (
            "stages:\n"
            "  - validate\n"
            "\n"
            "matey_schema_validate:\n"
            "  stage: validate\n"
            "  image: ghcr.io/prefix-dev/pixi:latest\n"
            "  script:\n"
            "    - pixi install\n"
            "    - pixi run matey lock doctor\n"
            "    - pixi run matey schema validate\n"
        )

    lines = "\n".join(
        "\n".join(
            [
                f"      pixi run matey --target {target} lock doctor",
                f"      pixi run matey --target {target} schema validate",
            ]
        )
        for target in targets
    )
    return (
        "stages:\n"
        "  - validate\n"
        "\n"
        "matey_schema_validate:\n"
        "  stage: validate\n"
        "  image: ghcr.io/prefix-dev/pixi:latest\n"
        "  script:\n"
        "    - pixi install\n"
        "    - |\n"
        f"{lines}\n"
    )


def _buildkite_content(targets: list[str]) -> str:
    if not targets:
        return (
            "steps:\n"
            "  - label: \":matey: schema validate\"\n"
            "    command:\n"
            "      - pixi install\n"
            "      - pixi run matey lock doctor\n"
            "      - pixi run matey schema validate\n"
        )

    steps = [
        "\n".join(
            [
                f'  - label: ":matey: schema validate ({target})"',
                "    command:",
                "      - pixi install",
                f"      - pixi run matey --target {target} lock doctor",
                f"      - pixi run matey --target {target} schema validate",
            ]
        )
        for target in targets
    ]
    return "steps:\n" + "\n".join(steps) + "\n"


def render_ci_template(provider: CIProvider, *, targets: list[str]) -> TemplateFile:
    if provider == "github":
        return TemplateFile(path=Path(".github/workflows/matey-schema-validate.yml"), content=_github_content(targets))
    if provider == "gitlab":
        return TemplateFile(path=Path(".gitlab-ci.matey.yml"), content=_gitlab_content(targets))
    if provider == "buildkite":
        return TemplateFile(path=Path(".buildkite/matey-schema-validate.yml"), content=_buildkite_content(targets))
    raise ValueError(f"Unsupported provider: {provider}")
