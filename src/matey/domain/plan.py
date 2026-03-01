from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from matey.domain.engine import Engine
from matey.domain.target import TargetId, TargetKey, TargetPaths


@dataclass(frozen=True)
class SchemaOpContext:
    target_id: TargetId
    target_key: TargetKey
    target_paths: TargetPaths
    repo_root: Path
    base_ref: str | None
    replay_engine: Engine
    test_url: str | None
    clean: bool
    keep_scratch: bool
    run_nonce: str


@dataclass(frozen=True)
class DbOpContext:
    target_id: TargetId
    target_key: TargetKey
    target_paths: TargetPaths
    live_engine: Engine
    live_url: str
    test_url: str | None
    keep_scratch: bool
    run_nonce: str
