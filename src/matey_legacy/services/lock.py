from __future__ import annotations

from matey.core.lock import SchemaLock, doctor_schema_lock, sync_schema_lock
from matey.core.types import ResolvedPaths


def doctor(*, paths: ResolvedPaths) -> SchemaLock:
    return doctor_schema_lock(paths=paths)


def sync(*, paths: ResolvedPaths, engine: str, target: str) -> SchemaLock:
    return sync_schema_lock(paths=paths, engine=engine, target=target)
