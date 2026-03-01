from __future__ import annotations

import hashlib

from matey.domain.constants import CHAIN_PREFIX
from matey.domain.engine import Engine
from matey.domain.target import TargetKey


def digest_bytes_blake2b256(payload: bytes) -> str:
    return hashlib.blake2b(payload, digest_size=32).hexdigest()


def digest_text_blake2b256(text: str) -> str:
    return digest_bytes_blake2b256(text.encode("utf-8"))


def lock_chain_seed(engine: Engine, target_key: TargetKey) -> str:
    seed = f"{CHAIN_PREFIX}|{engine.value}|{target_key.value}".encode()
    return digest_bytes_blake2b256(seed)


def lock_chain_step(prev: str, version: str, migration_file: str, migration_digest: str) -> str:
    payload = f"{prev}|{version}|{migration_file}|{migration_digest}".encode()
    return digest_bytes_blake2b256(payload)
