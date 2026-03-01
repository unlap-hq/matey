from __future__ import annotations

import subprocess
from pathlib import Path

from matey.app.protocols import CmdResult, IProcessRunner


class SubprocessRunner(IProcessRunner):
    def run(self, argv: tuple[str, ...], cwd: Path | None = None) -> CmdResult:
        completed = subprocess.run(
            list(argv),
            cwd=str(cwd) if cwd is not None else None,
            check=False,
            capture_output=True,
            text=True,
        )
        return CmdResult(
            argv=argv,
            exit_code=int(completed.returncode),
            stdout=completed.stdout or "",
            stderr=completed.stderr or "",
        )
