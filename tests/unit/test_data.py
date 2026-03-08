from __future__ import annotations

import json
from pathlib import Path

import pytest

from matey.data.apply import _apply_rows
from matey.data.io import load_data_sets, read_jsonl, select_data_set, write_jsonl
from matey.data.model import DataError, DataFile
from matey.db_urls import IbisTarget
from matey.project import TargetConfig


def _target(tmp_path: Path) -> TargetConfig:
    return TargetConfig(
        name="db/core",
        root=(tmp_path / "db" / "core").resolve(),
        engine="sqlite",
        url_env="DATABASE_URL",
        test_url_env="TEST_DATABASE_URL",
    )


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_load_data_sets_parses_manifest_and_upsert_key(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.data_manifest,
        """
[core]
files = [
  { name = "roles", table = "roles", mode = "replace" },
  { name = "permissions", table = "permissions", mode = "upsert", on = "id" },
]
""".strip()
        + "\n",
    )

    sets = load_data_sets(target)

    assert [data_set.name for data_set in sets] == ["core"]
    assert sets[0].files[0].path == target.data_dir / "roles.jsonl"
    assert sets[0].files[1].on == "id"


def test_select_data_set_uses_only_set_when_single(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.data_manifest,
        """
[core]
files = [
  { name = "roles", table = "roles", mode = "replace" },
]
""".strip()
        + "\n",
    )

    data_set = select_data_set(load_data_sets(target), set_name=None)

    assert data_set.name == "core"


def test_select_data_set_requires_set_when_multiple(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.data_manifest,
        """
[core]
files = [
  { name = "roles", table = "roles", mode = "replace" },
]

[demo]
files = [
  { name = "demo_users", table = "users", mode = "insert" },
]
""".strip()
        + "\n",
    )

    with pytest.raises(DataError, match="Multiple data sets are defined; pass --set"):
        select_data_set(load_data_sets(target), set_name=None)


def test_load_data_sets_requires_upsert_key(tmp_path: Path) -> None:
    target = _target(tmp_path)
    _write(
        target.data_manifest,
        """
[core]
files = [
  { name = "permissions", table = "permissions", mode = "upsert" },
]
""".strip()
        + "\n",
    )

    with pytest.raises(DataError, match="requires on"):
        load_data_sets(target)


def test_jsonl_roundtrip_and_deterministic_order(tmp_path: Path) -> None:
    path = tmp_path / "rows.jsonl"
    rows = [
        {"id": 2, "name": "viewer"},
        {"id": 1, "name": "admin"},
    ]

    count = write_jsonl(path, rows)

    assert count == 2
    assert read_jsonl(path) == [
        {"id": 1, "name": "admin"},
        {"id": 2, "name": "viewer"},
    ]
    lines = path.read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[0])["id"] == 1


class _Backend:
    def __init__(self) -> None:
        self.calls: list[tuple[str, object]] = []

    def insert(self, name, obj, *, database=None, overwrite=False):
        self.calls.append(("insert", (name, obj, database, overwrite)))

    def upsert(self, name, obj, *, on, database=None):
        self.calls.append(("upsert", (name, obj, on, database)))

    def truncate_table(self, name, *, database=None):
        self.calls.append(("truncate", (name, database)))


def test_apply_rows_dispatches_all_modes(tmp_path: Path) -> None:
    backend = _Backend()
    handle = IbisTarget(kind="ibis", backend=backend, database=None)
    replace_file = DataFile(
        name="roles", table="roles", mode="replace", path=tmp_path / "roles.jsonl"
    )
    insert_file = DataFile(
        name="users", table="users", mode="insert", path=tmp_path / "users.jsonl"
    )
    upsert_file = DataFile(
        name="permissions",
        table="permissions",
        mode="upsert",
        path=tmp_path / "permissions.jsonl",
        on="id",
    )

    _apply_rows(handle=handle, data_file=replace_file, rows=[{"id": 1}])
    _apply_rows(handle=handle, data_file=insert_file, rows=[{"id": 2}])
    _apply_rows(handle=handle, data_file=upsert_file, rows=[{"id": 3}])
    _apply_rows(handle=handle, data_file=replace_file, rows=[])

    assert backend.calls == [
        ("insert", ("roles", [{"id": 1}], None, True)),
        ("insert", ("users", [{"id": 2}], None, False)),
        ("upsert", ("permissions", [{"id": 3}], "id", None)),
        ("truncate", ("roles", None)),
    ]
