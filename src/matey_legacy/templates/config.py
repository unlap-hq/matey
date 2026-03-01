from __future__ import annotations

import re


def _target_env_name(target: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9]+", "_", target).strip("_").upper()
    if not normalized:
        normalized = "TARGET"
    return f"{normalized}_DATABASE_URL"


def render_config_template(*, targets: list[str]) -> str:
    if not targets:
        raise ValueError("Cannot render target skeleton config without targets.")

    lines = [
        "[defaults]",
        'dir = "db"',
        'url_env = "MATEY_URL"',
        'test_url_env = "MATEY_TEST_URL"',
    ]
    for target in targets:
        lines.extend(
            [
                "",
                f"[targets.{target}]",
                f'url_env = "{_target_env_name(target)}"',
            ]
        )
    return "\n".join(lines) + "\n"
