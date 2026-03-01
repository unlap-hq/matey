from __future__ import annotations

from pathlib import Path

from matey.domain.errors import ConfigError

_GITHUB_ACTIONS_TEMPLATE = """name: matey

on:
  pull_request:
  push:
    branches: [main]

jobs:
  schema:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: prefix-dev/setup-pixi@v0
      - run: pixi install
      - run: pixi run matey schema status
      - run: pixi run matey schema plan
"""


class CiTemplateEngine:
    def render(self) -> str:
        return _GITHUB_ACTIONS_TEMPLATE

    def write(self, *, path: Path, overwrite: bool) -> None:
        if path.exists() and not overwrite:
            raise ConfigError(f"CI template already exists: {path}")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(self.render(), encoding="utf-8")
