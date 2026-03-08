from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from rich.console import Console
from rich.table import Table

from matey.data import DataApplyResult, DataExportResult
from matey.db import DriftResult, MutationResult
from matey.db import PlanResult as DbPlanResult
from matey.lockfile import LockState
from matey.schema import ApplyResult, InitResult
from matey.schema import PlanResult as SchemaPlanResult


@dataclass(slots=True)
class Renderer:
    console: Console
    error_console: Console

    @classmethod
    def create(cls) -> Renderer:
        return cls(console=Console(), error_console=Console(stderr=True))

    def target_header(self, target_name: str) -> None:
        self.console.print(f"[bold]{target_name}[/bold]")

    def db_mutation(self, command: str, result: MutationResult) -> None:
        self.console.print(
            self._table(
                columns=(
                    ("Command", {}),
                    ("Before", {"justify": "right"}),
                    ("After", {"justify": "right"}),
                ),
                rows=((command, str(result.before_index), str(result.after_index)),),
                box=None,
            )
        )

    def db_drift(self, result: DriftResult) -> None:
        status = "drifted" if result.drifted else "clean"
        color = "yellow" if result.drifted else "green"
        self.console.print(
            f"Applied index: {result.applied_index}  Status: [{color}]{status}[/{color}]"
        )

    def db_plan(self, result: DbPlanResult) -> None:
        status = "match" if result.matches else "mismatch"
        color = "green" if result.matches else "yellow"
        self.console.print(
            self._table(
                columns=(
                    ("Applied", {"justify": "right"}),
                    ("Target", {"justify": "right"}),
                    ("Status", {}),
                ),
                rows=(
                    (
                        str(result.applied_index),
                        str(result.target_index),
                        f"[{color}]{status}[/{color}]",
                    ),
                ),
                box=None,
            )
        )

    def schema_status(self, state: LockState) -> None:
        status = "clean" if state.is_clean else "stale"
        color = "green" if state.is_clean else "yellow"
        self.console.print(
            self._table(
                columns=(
                    ("Status", {}),
                    ("Steps", {"justify": "right"}),
                    ("Diagnostics", {"justify": "right"}),
                ),
                rows=(
                    (
                        f"[{color}]{status}[/{color}]",
                        str(len(state.worktree_steps)),
                        str(len(state.diagnostics)),
                    ),
                ),
                box=None,
            )
        )
        if state.diagnostics:
            self.console.print(
                self._table(
                    columns=(("Code", {}), ("Path", {}), ("Detail", {})),
                    rows=tuple(
                        (item.code.value, item.path, item.detail) for item in state.diagnostics
                    ),
                )
            )

    def schema_plan(self, result: SchemaPlanResult) -> None:
        status = "match" if result.matches else "mismatch"
        color = "green" if result.matches else "yellow"
        self.console.print(
            self._table(
                columns=(
                    ("Divergence", {}),
                    ("Anchor", {"justify": "right"}),
                    ("Tail", {"justify": "right"}),
                    ("Status", {}),
                ),
                rows=(
                    (
                        "-" if result.divergence_index is None else str(result.divergence_index),
                        str(result.anchor_index),
                        str(result.tail_count),
                        f"[{color}]{status}[/{color}]",
                    ),
                ),
                box=None,
            )
        )
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
        if result.codegen_path:
            self.console.print(f"Codegen: {result.codegen_path}")
        if result.changed_files:
            self.console.print(
                self._table(
                    columns=(("Changed Files", {}),),
                    rows=tuple((path,) for path in result.changed_files),
                    box=None,
                )
            )

    def init_target(self, result: InitResult) -> None:
        status = "changed" if result.wrote else "no-op"
        color = "green" if result.wrote else "cyan"
        self.console.print(f"Init: [{color}]{status}[/{color}]  Engine: {result.engine}")
        if result.changed_files:
            self.console.print(
                self._table(
                    columns=(("Changed Files", {}),),
                    rows=tuple((path,) for path in result.changed_files),
                    box=None,
                )
            )

    def data_apply(self, result: DataApplyResult) -> None:
        self.console.print(f"Data set: {result.set_name}")
        self.console.print(
            self._table(
                columns=(
                    ("File", {}),
                    ("Table", {}),
                    ("Mode", {}),
                    ("Rows", {"justify": "right"}),
                ),
                rows=tuple(
                    (item.name, item.table, item.mode, str(item.rows)) for item in result.files
                ),
                box=None,
            )
        )

    def data_export(self, result: DataExportResult) -> None:
        self.console.print(f"Data export set: {result.set_name}")
        self.console.print(
            self._table(
                columns=(
                    ("File", {}),
                    ("Table", {}),
                    ("Mode", {}),
                    ("Rows", {"justify": "right"}),
                ),
                rows=tuple(
                    (item.name, item.table, item.mode, str(item.rows)) for item in result.files
                ),
                box=None,
            )
        )

    def template_content(self, content: str) -> None:
        self._print_blob(self.console, content)

    def template_written(self, path: str) -> None:
        self.console.print(f"Wrote {path}")

    def sql_blob(self, sql: str) -> None:
        self._print_blob(self.console, sql)

    def diff_blob(self, diff: str) -> None:
        self._print_blob(self.console, diff)

    def stdout_blob(self, text: str) -> None:
        self._print_blob(self.console, text)

    def stderr_blob(self, text: str) -> None:
        self._print_blob(self.error_console, text)

    def error(self, message: str) -> None:
        self.error_console.print(f"[red]error:[/red] {message}")

    def _print_blob(self, console: Console, text: str) -> None:
        if text:
            console.print(text, markup=False, end="")

    def _table(
        self,
        *,
        columns: tuple[tuple[str, dict[str, Any]], ...],
        rows: tuple[tuple[str, ...], ...],
        box: Any | None = None,
    ) -> Table:
        table = Table(show_header=True, header_style="bold", box=box)
        for name, kwargs in columns:
            table.add_column(name, **kwargs)
        for row in rows:
            table.add_row(*row)
        return table


__all__ = ["Renderer"]
