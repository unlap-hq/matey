from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from enum import Enum

from rich.console import Console
from rich.table import Table

from matey.domain.model import DbPlanResult, SchemaPlanResult, SchemaStatusResult


class OutputKind(Enum):
    PASSTHROUGH = "passthrough"
    REPORT = "report"
    ARTIFACT = "artifact"


class OutputFormat(Enum):
    HUMAN = "human"
    JSON = "json"


COMMAND_OUTPUT_POLICY: dict[str, OutputKind] = {
    # db passthrough
    "db.new": OutputKind.PASSTHROUGH,
    "db.create": OutputKind.PASSTHROUGH,
    "db.wait": OutputKind.PASSTHROUGH,
    "db.up": OutputKind.PASSTHROUGH,
    "db.migrate": OutputKind.PASSTHROUGH,
    "db.status": OutputKind.PASSTHROUGH,
    "db.load": OutputKind.PASSTHROUGH,
    "db.dump": OutputKind.PASSTHROUGH,
    "db.down": OutputKind.PASSTHROUGH,
    "db.drop": OutputKind.PASSTHROUGH,
    "db.dbmate": OutputKind.PASSTHROUGH,
    # db report/artifact
    "db.drift": OutputKind.REPORT,
    "db.plan": OutputKind.REPORT,
    "db.plan.diff": OutputKind.ARTIFACT,
    "db.plan.sql": OutputKind.ARTIFACT,
    # schema report/artifact
    "schema.status": OutputKind.REPORT,
    "schema.plan": OutputKind.REPORT,
    "schema.plan.diff": OutputKind.ARTIFACT,
    "schema.plan.sql": OutputKind.ARTIFACT,
    "schema.apply": OutputKind.REPORT,
}


@dataclass(frozen=True)
class TargetText:
    target: str
    text: str


@dataclass(frozen=True)
class SchemaStatusRecord:
    target: str
    result: SchemaStatusResult


@dataclass(frozen=True)
class SchemaPlanRecord:
    target: str
    result: SchemaPlanResult


@dataclass(frozen=True)
class DbPlanRecord:
    target: str
    result: DbPlanResult


@dataclass(frozen=True)
class OperationRecord:
    target: str
    status: str
    detail: str | None = None


class CliPresenter:
    def __init__(self, *, console: Console | None = None) -> None:
        self._console = console or Console()

    @staticmethod
    def output_kind(command_id: str) -> OutputKind:
        try:
            return COMMAND_OUTPUT_POLICY[command_id]
        except KeyError as error:
            raise ValueError(f"Unknown command output policy key: {command_id}") from error

    @staticmethod
    def parse_output_format(value: str) -> OutputFormat:
        normalized = value.strip().lower()
        if normalized == "human":
            return OutputFormat.HUMAN
        if normalized == "json":
            return OutputFormat.JSON
        raise ValueError("Output must be one of: human, json.")

    def emit_operations(self, *, command_id: str, records: tuple[OperationRecord, ...]) -> int:
        if self.output_kind(command_id) is not OutputKind.REPORT:
            raise ValueError(f"Command {command_id} is not a report command.")

        table = Table(show_header=True, header_style="bold")
        table.add_column("Target", style="cyan", no_wrap=True)
        table.add_column("Status", style="green", no_wrap=True)
        table.add_column("Detail", style="white")
        for record in records:
            table.add_row(record.target, record.status, record.detail or "")
        self._console.print(table)
        return 0

    def emit_target_text(self, *, command_id: str, blocks: tuple[TargetText, ...]) -> None:
        kind = self.output_kind(command_id)
        if kind not in {OutputKind.PASSTHROUGH, OutputKind.ARTIFACT}:
            raise ValueError(f"Command {command_id} does not use text output.")

        if not blocks:
            self._console.print("", end="")
            return
        if len(blocks) == 1:
            self._console.print(blocks[0].text, end="")
            return

        for index, block in enumerate(blocks):
            self._console.print(f"[{block.target}]")
            self._console.print(block.text.rstrip())
            if index < len(blocks) - 1:
                self._console.print()

    def emit_schema_status(
        self,
        *,
        command_id: str,
        records: tuple[SchemaStatusRecord, ...],
        output: OutputFormat,
    ) -> int:
        if self.output_kind(command_id) is not OutputKind.REPORT:
            raise ValueError(f"Command {command_id} is not a report command.")

        exit_code = int(any(record.result.stale for record in records))
        if output is OutputFormat.JSON:
            payload = [
                {
                    "target": record.target,
                    "up_to_date": record.result.up_to_date,
                    "stale": record.result.stale,
                    "summary": list(record.result.summary),
                    "rows": [asdict(row) for row in record.result.rows],
                }
                for record in records
            ]
            body: object = payload[0] if len(payload) == 1 else payload
            self._console.print_json(json.dumps(body, sort_keys=True))
            return exit_code

        summary_table = Table(show_header=True, header_style="bold")
        summary_table.add_column("Target", style="cyan", no_wrap=True)
        summary_table.add_column("State", style="white", no_wrap=True)
        summary_table.add_column("Issues", style="yellow", no_wrap=True)
        summary_table.add_column("Summary", style="white")
        for record in records:
            state = "up-to-date" if record.result.up_to_date else "stale"
            summary_table.add_row(
                record.target,
                state,
                str(len(record.result.rows)),
                ", ".join(record.result.summary),
            )
        self._console.print(summary_table)

        for record in records:
            if not record.result.rows:
                continue
            self._console.print(f"[bold]{record.target}[/bold] [dim]details[/dim]")
            details = Table(show_header=True, header_style="bold")
            details.add_column("Marker", style="cyan", no_wrap=True)
            details.add_column("Migration", style="magenta")
            details.add_column("Status", style="yellow")
            details.add_column("Detail", style="white")
            for row in record.result.rows:
                marker = {"ok": "OK", "warn": "WARN", "error": "ERR"}.get(row.marker, "UNK")
                details.add_row(marker, row.migration_file, row.status, row.detail)
            self._console.print(details)

        return exit_code

    def emit_schema_plan(
        self,
        *,
        command_id: str,
        records: tuple[SchemaPlanRecord, ...],
        output: OutputFormat,
    ) -> int:
        if self.output_kind(command_id) is not OutputKind.REPORT:
            raise ValueError(f"Command {command_id} is not a report command.")

        exit_code = int(any(not record.result.comparison.equal for record in records))
        if output is OutputFormat.JSON:
            payload = [
                {
                    "target": record.target,
                    "comparison_equal": record.result.comparison.equal,
                    "comparison_diff": record.result.comparison.diff,
                    "replay_scratch_url": record.result.replay_scratch_url,
                    "down_checked": record.result.down_checked,
                    "orphan_checkpoints": list(record.result.orphan_checkpoints),
                }
                for record in records
            ]
            body: object = payload[0] if len(payload) == 1 else payload
            self._console.print_json(json.dumps(body, sort_keys=True))
            return exit_code

        table = Table(show_header=True, header_style="bold")
        table.add_column("Target", style="cyan", no_wrap=True)
        table.add_column("Comparison", style="white", no_wrap=True)
        table.add_column("Down Checks", style="white", no_wrap=True)
        table.add_column("Orphans", style="yellow", no_wrap=True)
        table.add_column("Scratch", style="white")
        for record in records:
            comparison = "match" if record.result.comparison.equal else "diff"
            table.add_row(
                record.target,
                comparison,
                "enabled" if record.result.down_checked else "disabled",
                str(len(record.result.orphan_checkpoints)),
                record.result.replay_scratch_url,
            )
        self._console.print(table)
        return exit_code

    def emit_db_plan(
        self,
        *,
        command_id: str,
        mode: str,
        records: tuple[DbPlanRecord, ...],
        output: OutputFormat,
    ) -> int:
        if self.output_kind(command_id) is not OutputKind.REPORT:
            raise ValueError(f"Command {command_id} is not a report command.")

        exit_code = int(any(not record.result.comparison.equal for record in records))
        if output is OutputFormat.JSON:
            payload = [
                {
                    "target": record.target,
                    "mode": mode,
                    "comparison_equal": record.result.comparison.equal,
                    "comparison_diff": record.result.comparison.diff,
                    "live_applied_index": record.result.live_applied_index,
                }
                for record in records
            ]
            body: object = payload[0] if len(payload) == 1 else payload
            self._console.print_json(json.dumps(body, sort_keys=True))
            return exit_code

        table = Table(show_header=True, header_style="bold")
        table.add_column("Target", style="cyan", no_wrap=True)
        table.add_column("Mode", style="white", no_wrap=True)
        table.add_column("Comparison", style="white", no_wrap=True)
        table.add_column("Live Index", style="yellow", no_wrap=True)
        for record in records:
            comparison = "match" if record.result.comparison.equal else "diff"
            table.add_row(record.target, mode, comparison, str(record.result.live_applied_index))
        self._console.print(table)
        return exit_code
