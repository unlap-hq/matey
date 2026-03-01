from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class RootOptions:
    target: str | None
    all_targets: bool
    config_path: Path | None
    dir_override: Path | None
    base_ref: str | None
    url: str | None
    test_url: str | None
    keep_scratch: bool
    verbose: bool
    quiet: bool
