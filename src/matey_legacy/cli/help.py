from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CommandSpec:
    name: str
    help: str


@dataclass(frozen=True)
class GroupSpec:
    name: str
    panel: str
    summary: str
    commands: tuple[CommandSpec, ...]


_GROUPS: dict[str, GroupSpec] = {
    "schema": GroupSpec(
        name="schema",
        panel="Schema Workflows",
        summary="Schema workflows.",
        commands=(
            CommandSpec("validate", "Validate schema.sql against lockfile replay."),
            CommandSpec("regen", "Regenerate schema.sql and schema.lock.toml from replay."),
            CommandSpec("diff", "Show schema.sql vs lockfile replay differences."),
        ),
    ),
    "db": GroupSpec(
        name="db",
        panel="Database Workflows",
        summary="Live database workflows.",
        commands=(
            CommandSpec("new", "Create a new migration file."),
            CommandSpec("create", "Create database/dataset where supported."),
            CommandSpec("wait", "Wait for database to become available."),
            CommandSpec("up", "Create if needed, then apply pending migrations."),
            CommandSpec("migrate", "Apply pending migrations (no implicit create)."),
            CommandSpec("status", "Show migration status."),
            CommandSpec("diff", "Compare live DB schema against lockfile-expected schema."),
            CommandSpec("load", "Load schema from schema file."),
            CommandSpec("dump", "Dump schema to stdout."),
            CommandSpec("down", "Roll back N steps (default 1)."),
            CommandSpec("drop", "Drop database/dataset where supported."),
            CommandSpec("dbmate", "Run bundled dbmate directly."),
        ),
    ),
    "lock": GroupSpec(
        name="lock",
        panel="Lock Workflows",
        summary="Lockfile workflows.",
        commands=(
            CommandSpec("doctor", "Check schema.lock.toml and checkpoint integrity."),
            CommandSpec("sync", "Regenerate schema.lock.toml from repo artifacts."),
        ),
    ),
    "ci": GroupSpec(
        name="ci",
        panel="Project Setup",
        summary="CI helper workflows.",
        commands=(
            CommandSpec("init", "Write provider CI template."),
            CommandSpec("print", "Print provider CI template to stdout."),
        ),
    ),
    "config": GroupSpec(
        name="config",
        panel="Project Setup",
        summary="Config helper workflows.",
        commands=(
            CommandSpec("init", "Write matey.toml skeleton."),
            CommandSpec("print", "Print matey.toml skeleton to stdout."),
        ),
    ),
}


def group_spec(group: str) -> GroupSpec:
    try:
        return _GROUPS[group]
    except KeyError as error:  # pragma: no cover - internal programming error guard
        raise ValueError(f"Unknown command group: {group}") from error


def group_panel(group: str) -> str:
    return group_spec(group).panel


def subgroup_help(group: str) -> str:
    spec = group_spec(group)
    return f"{spec.summary} Run `matey {group} <command> --help` for command usage."


def root_group_help(group: str) -> str:
    spec = group_spec(group)
    commands = ", ".join(command.name for command in spec.commands)
    return f"{spec.summary} Commands: {commands}."


def command_help(group: str, command: str) -> str:
    spec = group_spec(group)
    for command_spec in spec.commands:
        if command_spec.name == command:
            return command_spec.help
    raise ValueError(f"Unknown command '{command}' for group '{group}'")
