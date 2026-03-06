from __future__ import annotations

from dataclasses import dataclass

from rich.console import Console
from rich.table import Table

from matey.db import DriftResult, MutationResult
from matey.db import PlanResult as DbPlanResult
from matey.lockfile import LockState
from matey.schema import ApplyResult
from matey.schema import PlanResult as SchemaPlanResult


@dataclass(slots=True)
class CliRenderer:
    console: Console
    error_console: Console

    @classmethod
    def create(cls) -> CliRenderer:
        return cls(console=Console(), error_console=Console(stderr=True))

    def target_header(self, target_name: str) -> None:
        self.console.print(f"[bold]{target_name}[/bold]")

    def stdout_blob(self, text: str) -> None:
        if text:
            self.console.print(text, markup=False, end="")

    def stderr_blob(self, text: str) -> None:
        if text:
            self.error_console.print(text, markup=False, end="")

    def db_status_stdout(self, text: str) -> None:
        self.stdout_blob(text)

    def db_mutation(self, command: str, result: MutationResult) -> None:
        table = Table(show_header=True, header_style="bold", box=None)
        table.add_column("Command")
        table.add_column("Before", justify="right")
        table.add_column("After", justify="right")
        table.add_row(command, str(result.before_index), str(result.after_index))
        self.console.print(table)

    def db_drift(self, result: DriftResult) -> None:
        status = "drifted" if result.drifted else "clean"
        color = "yellow" if result.drifted else "green"
        self.console.print(
            f"Applied index: {result.applied_index}  Status: [{color}]{status}[/{color}]"
        )

    def db_plan(self, result: DbPlanResult) -> None:
        status = "match" if result.matches else "mismatch"
        color = "green" if result.matches else "yellow"
        table = Table(show_header=True, header_style="bold", box=None)
        table.add_column("Applied", justify="right")
        table.add_column("Target", justify="right")
        table.add_column("Status")
        table.add_row(
            str(result.applied_index),
            str(result.target_index),
            f"[{color}]{status}[/{color}]",
        )
        self.console.print(table)

    def schema_status(self, state: LockState) -> None:
        status = "clean" if state.is_clean else "stale"
        color = "green" if state.is_clean else "yellow"
        summary = Table(show_header=True, header_style="bold", box=None)
        summary.add_column("Status")
        summary.add_column("Steps", justify="right")
        summary.add_column("Diagnostics", justify="right")
        summary.add_row(
            f"[{color}]{status}[/{color}]",
            str(len(state.worktree_steps)),
            str(len(state.diagnostics)),
        )
        self.console.print(summary)
        if state.diagnostics:
            diag = Table(show_header=True, header_style="bold")
            diag.add_column("Code")
            diag.add_column("Path")
            diag.add_column("Detail")
            for item in state.diagnostics:
                diag.add_row(item.code.value, item.path, item.detail)
            self.console.print(diag)

    def schema_plan(self, result: SchemaPlanResult) -> None:
        status = "match" if result.matches else "mismatch"
        color = "green" if result.matches else "yellow"
        table = Table(show_header=True, header_style="bold", box=None)
        table.add_column("Divergence")
        table.add_column("Anchor", justify="right")
        table.add_column("Tail", justify="right")
        table.add_column("Status")
        table.add_row(
            "-" if result.divergence_index is None else str(result.divergence_index),
            str(result.anchor_index),
            str(result.tail_count),
            f"[{color}]{status}[/{color}]",
        )
        self.console.print(table)
        self.console.print(f"Replay scratch: {result.replay_scratch_url}")
        if result.down_scratch_url:
            self.console.print(f"Down-check scratch: {result.down_scratch_url}")
        self.console.print(
            f"Down checked: {len(result.down_checked)}  Down skipped: {len(result.down_skipped)}"
        )

    def schema_apply(self, result: ApplyResult) -> None:
        status = "changed" if result.wrote else "no-op"
        color = "green" if result.wrote else "cyan"
        self.console.print(f"Apply: [{color}]{status}[/{color}]")
        self.console.print(f"Replay scratch: {result.replay_scratch_url}")
        if result.down_scratch_url:
            self.console.print(f"Down-check scratch: {result.down_scratch_url}")
        if result.changed_files:
            files_table = Table(show_header=True, header_style="bold", box=None)
            files_table.add_column("Changed Files")
            for path in result.changed_files:
                files_table.add_row(path)
            self.console.print(files_table)

    def template_content(self, content: str) -> None:
        self.console.print(content, markup=False, end="")

    def template_written(self, path: str) -> None:
        self.console.print(f"Wrote {path}")

    def sql_blob(self, sql: str) -> None:
        self.console.print(sql, markup=False, end="")

    def diff_blob(self, diff: str) -> None:
        self.console.print(diff, markup=False, end="")

    def error(self, message: str) -> None:
        self.error_console.print(f"[red]error:[/red] {message}")
