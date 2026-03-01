from __future__ import annotations

import os
import tempfile
from pathlib import Path

from matey.app.protocols import IFileSystem


class LocalFileSystem(IFileSystem):
    def read_bytes(self, path: Path) -> bytes:
        return path.read_bytes()

    def read_text(self, path: Path) -> str:
        return path.read_text(encoding="utf-8")

    def write_bytes_atomic(self, path: Path, data: bytes) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(path)
        finally:
            if tmp_path.exists():
                tmp_path.unlink(missing_ok=True)

    def write_text_atomic(self, path: Path, data: str) -> None:
        self.write_bytes_atomic(path, data.encode("utf-8"))

    def exists(self, path: Path) -> bool:
        return path.exists()

    def mkdir(self, path: Path, parents: bool = False) -> None:
        path.mkdir(parents=parents, exist_ok=True)

    def list_files(self, path: Path) -> tuple[Path, ...]:
        if not path.exists():
            return ()
        return tuple(sorted((p for p in path.iterdir() if p.is_file()), key=lambda p: p.name))


def resolve_inside(root: Path, rel: str) -> Path:
    candidate = (root / rel).resolve()
    root_resolved = root.resolve()
    try:
        candidate.relative_to(root_resolved)
    except ValueError as error:
        raise ValueError(f"Path escapes root: {rel!r}") from error
    return candidate
