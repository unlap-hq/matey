from __future__ import annotations

import json
import tomllib
from collections.abc import Iterable
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any

from matey.paths import normalize_relative_posix_path, safe_descendant
from matey.project import TargetConfig

from .model import DataError, DataFile, DataSet

_DATA_KEYS = frozenset({"files"})
_DATA_FILE_KEYS = frozenset({"name", "table", "mode", "on"})
_DATA_MODES = frozenset({"replace", "upsert", "insert"})


def load_data_sets(target: TargetConfig) -> tuple[DataSet, ...]:
    manifest_path = target.data_manifest
    if not manifest_path.exists():
        raise DataError(f"Data manifest not found: {manifest_path}")
    try:
        parsed = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
    except OSError as error:
        raise DataError(
            f"Unable to read data manifest {manifest_path}: {error.strerror or error}"
        ) from error
    except UnicodeDecodeError as error:
        raise DataError(f"Unable to decode data manifest {manifest_path} as UTF-8.") from error
    except tomllib.TOMLDecodeError as error:
        raise DataError(f"Unable to parse data manifest {manifest_path}: {error}") from error
    if not isinstance(parsed, dict):
        raise DataError(f"Data manifest {manifest_path} must be a top-level table.")

    sets: list[DataSet] = []
    for set_name, value in parsed.items():
        if not isinstance(set_name, str) or not isinstance(value, dict):
            raise DataError(f"Data manifest {manifest_path} contains an invalid set entry.")
        unsupported = set(value) - _DATA_KEYS
        if unsupported:
            rendered = ", ".join(sorted(repr(key) for key in unsupported))
            raise DataError(f"{manifest_path}: unsupported keys in data set {set_name!r}: {rendered}.")
        raw_files = value.get("files")
        if not isinstance(raw_files, list):
            raise DataError(f"{manifest_path}: data set {set_name!r} must define a files array.")
        files: list[DataFile] = []
        for raw_file in raw_files:
            if not isinstance(raw_file, dict):
                raise DataError(
                    f"{manifest_path}: data set {set_name!r} contains a non-table file entry."
                )
            files.append(_parse_data_file(target=target, manifest_path=manifest_path, raw=raw_file))
        sets.append(DataSet(name=set_name, files=tuple(files)))

    return tuple(sets)


def select_data_set(sets: tuple[DataSet, ...], *, set_name: str | None) -> DataSet:
    if not sets:
        raise DataError("No data sets are defined.")
    if set_name is None:
        if len(sets) == 1:
            return sets[0]
        available = ", ".join(data_set.name for data_set in sets)
        raise DataError(
            f"Multiple data sets are defined; pass --set. Available sets: {available}"
        )
    for data_set in sets:
        if data_set.name == set_name:
            return data_set
    available = ", ".join(data_set.name for data_set in sets)
    raise DataError(f"Unknown data set {set_name!r}. Available sets: {available}")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        content = path.read_text(encoding="utf-8")
    except OSError as error:
        raise DataError(f"Unable to read data file {path}: {error.strerror or error}") from error
    except UnicodeDecodeError as error:
        raise DataError(f"Unable to decode data file {path} as UTF-8.") from error
    rows: list[dict[str, Any]] = []
    for index, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise DataError(f"Invalid JSON in {path}:{index}: {error}") from error
        if not isinstance(row, dict):
            raise DataError(f"Data file {path}:{index} must contain JSON objects only.")
        rows.append(row)
    return rows


def write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    normalized_rows = sorted(
        rows,
        key=lambda row: json.dumps(row, sort_keys=True, default=_json_default),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in normalized_rows:
            handle.write(json.dumps(row, sort_keys=True, default=_json_default))
            handle.write("\n")
    return len(normalized_rows)


def _parse_data_file(
    *,
    target: TargetConfig,
    manifest_path: Path,
    raw: dict[str, Any],
) -> DataFile:
    unsupported = set(raw) - _DATA_FILE_KEYS
    if unsupported:
        rendered = ", ".join(sorted(repr(key) for key in unsupported))
        raise DataError(f"{manifest_path}: unsupported data file keys: {rendered}.")
    name = raw.get("name")
    table = raw.get("table")
    mode = raw.get("mode")
    on = raw.get("on")
    if not isinstance(name, str) or not isinstance(table, str) or not isinstance(mode, str):
        raise DataError(f"{manifest_path}: data file entries require string name/table/mode.")
    if mode not in _DATA_MODES:
        raise DataError(f"{manifest_path}: unsupported data mode {mode!r}.")
    if on is not None and not isinstance(on, str):
        raise DataError(f"{manifest_path}: data file {name!r} has non-string on key.")
    if mode == "upsert" and not on:
        raise DataError(f"{manifest_path}: data file {name!r} with mode='upsert' requires on.")

    normalized = normalize_relative_posix_path(name, label="data file")
    candidate = target.data_dir / Path(
        normalized if normalized.endswith(".jsonl") else f"{normalized}.jsonl"
    )
    path = safe_descendant(
        root=target.data_dir,
        candidate=candidate,
        label=f"data file {name!r}",
        allow_missing_leaf=True,
        expected_kind="file",
    )
    return DataFile(
        name=Path(normalized).stem,
        table=table,
        mode=mode,  # type: ignore[arg-type]
        path=path,
        on=on,
    )


def _json_default(value: Any) -> str:
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    return str(value)
