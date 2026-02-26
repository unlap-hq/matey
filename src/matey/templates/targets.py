from __future__ import annotations

import re

TARGET_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


def parse_target_list(raw_targets: str | None) -> list[str]:
    if raw_targets is None:
        return []

    parts = [part.strip() for part in raw_targets.split(",")]
    targets: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part:
            continue
        if not TARGET_NAME_RE.fullmatch(part):
            raise ValueError(
                f"Invalid target name {part!r}. Allowed characters: letters, numbers, '-' and '_'."
            )
        if part not in seen:
            seen.add(part)
            targets.append(part)
    return targets
