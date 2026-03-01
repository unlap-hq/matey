from __future__ import annotations

import tempfile
from pathlib import Path

from matey.app.protocols import ScratchHandle
from matey.domain.engine import Engine


class SqliteScratchManager:
    def __init__(self) -> None:
        self._files: dict[str, Path] = {}

    def prepare(
        self,
        *,
        scratch_name: str,
        purpose: str,
        test_base_url: str | None,
        build_scratch_url,
    ) -> ScratchHandle:
        if test_base_url and test_base_url.strip():
            url = build_scratch_url(test_base_url.strip(), scratch_name)
            return ScratchHandle(
                engine=Engine.SQLITE,
                url=url,
                scratch_name=scratch_name,
                purpose=purpose,
                auto_provisioned=False,
                cleanup_required=False,
            )

        file_path = Path(tempfile.gettempdir()) / f"{scratch_name}.sqlite3"
        file_path.parent.mkdir(parents=True, exist_ok=True)
        self._files[scratch_name] = file_path
        return ScratchHandle(
            engine=Engine.SQLITE,
            url=f"sqlite3:{file_path.as_posix()}",
            scratch_name=scratch_name,
            purpose=purpose,
            auto_provisioned=True,
            cleanup_required=False,
        )

    def cleanup(self, handle: ScratchHandle) -> None:
        file_path = self._files.pop(handle.scratch_name, None)
        if file_path is not None and file_path.exists():
            file_path.unlink(missing_ok=True)
