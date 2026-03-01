from __future__ import annotations

import shlex
from dataclasses import dataclass

from rich.console import Console
from rich.text import Text

from matey.adapters.dbmate import DbmateExecutionResult


@dataclass(frozen=True)
class OutputOptions:
    verbose: bool = False
    quiet: bool = False
    failure_tail_lines: int = 40


class RichDbmateRenderer:
    def __init__(
        self,
        *,
        options: OutputOptions,
        stdout_console: Console | None = None,
        stderr_console: Console | None = None,
    ) -> None:
        self.options = options
        self._stdout = stdout_console or Console()
        self._stderr = stderr_console or Console(stderr=True)

    def handle(self, result: DbmateExecutionResult) -> None:
        if result.ok:
            self._render_success(result)
            return
        self._render_failure(result)

    def _render_success(self, result: DbmateExecutionResult) -> None:
        if self.options.quiet:
            return

        self._stdout.print(
            self._line(
                symbol="OK",
                symbol_style="green",
                message=f"{self._context_label(result)} ok {result.duration_seconds:.2f}s",
            )
        )
        if not self.options.verbose:
            return
        self._render_command(result, to_stderr=False)
        if result.stdout.strip():
            self._render_block("OUT", "stdout:", result.stdout.strip(), to_stderr=False)
        if result.stderr.strip():
            self._render_block("STDERR", "stderr:", result.stderr.strip(), to_stderr=True)

    def _render_failure(self, result: DbmateExecutionResult) -> None:
        self._stderr.print(
            self._line(
                symbol="ERR",
                symbol_style="red",
                message=(
                    f"{self._context_label(result)} failed exit={result.returncode} "
                    f"{result.duration_seconds:.2f}s"
                ),
            )
        )
        self._render_command(result, to_stderr=True)

        if self.options.verbose:
            if result.stdout.strip():
                self._render_block("OUT", "stdout:", result.stdout.strip(), to_stderr=True)
            if result.stderr.strip():
                self._render_block("STDERR", "stderr:", result.stderr.strip(), to_stderr=True)
            return

        stdout_tail = self._tail(result.stdout)
        stderr_tail = self._tail(result.stderr)
        if stdout_tail:
            self._render_block("OUT", "stdout tail:", stdout_tail, to_stderr=True)
        if stderr_tail:
            self._render_block("STDERR", "stderr tail:", stderr_tail, to_stderr=True)

    def _context_label(self, result: DbmateExecutionResult) -> str:
        context = result.context
        parts: list[str] = []
        if context is not None:
            if context.target:
                parts.append(f"target={context.target}")
            if context.phase:
                parts.append(f"phase={context.phase}")
            if context.step:
                parts.append(f"step={context.step}")
        if not parts:
            parts.append(f"step={result.verb}")
        return " ".join(parts)

    def _render_command(self, result: DbmateExecutionResult, *, to_stderr: bool) -> None:
        command_text = shlex.join(result.display_command)
        line = self._line(symbol="CMD", symbol_style="cyan", message=command_text)
        (self._stderr if to_stderr else self._stdout).print(line)

    def _render_block(self, symbol: str, label: str, payload: str, *, to_stderr: bool) -> None:
        console = self._stderr if to_stderr else self._stdout
        console.print(self._line(symbol=symbol, symbol_style=None, message=label))
        console.print(payload)

    @staticmethod
    def _line(*, symbol: str, symbol_style: str | None, message: str) -> Text:
        line = Text("[matey] ")
        line.append(f"[{symbol}]", style=symbol_style)
        line.append(f" {message}")
        return line

    def _tail(self, text: str) -> str:
        stripped = text.strip()
        if not stripped:
            return ""
        lines = stripped.splitlines()
        limit = max(1, int(self.options.failure_tail_lines))
        if len(lines) <= limit:
            return "\n".join(lines)
        return f"(last {limit} lines)\n" + "\n".join(lines[-limit:])
