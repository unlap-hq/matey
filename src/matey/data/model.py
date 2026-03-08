from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal


class DataError(RuntimeError):
    pass


DataMode = Literal["replace", "upsert", "insert"]


@dataclass(frozen=True, slots=True)
class DataFile:
    name: str
    table: str
    mode: DataMode
    path: Path
    on: str | None = None


@dataclass(frozen=True, slots=True)
class DataSet:
    name: str
    files: tuple[DataFile, ...]


@dataclass(frozen=True, slots=True)
class DataFileResult:
    name: str
    table: str
    mode: DataMode
    rows: int


@dataclass(frozen=True, slots=True)
class DataApplyResult:
    target_name: str
    set_name: str
    files: tuple[DataFileResult, ...]


@dataclass(frozen=True, slots=True)
class DataExportResult:
    target_name: str
    set_name: str
    files: tuple[DataFileResult, ...]
