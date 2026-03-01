from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CommandMeta:
    name: str
    help: str


@dataclass(frozen=True)
class GroupMeta:
    name: str
    help: str
    commands: tuple[CommandMeta, ...]
    subgroups: tuple[GroupMeta, ...] = ()


DB_PLAN = GroupMeta(
    name="plan",
    help="Compare live database against expected schema.",
    commands=(
        CommandMeta(name="__summary__", help="Show plan summary."),
        CommandMeta(name="diff", help="Print live-vs-head schema diff."),
        CommandMeta(name="sql", help="Print expected head schema SQL from artifacts (offline)."),
    ),
)

SCHEMA_PLAN = GroupMeta(
    name="plan",
    help="Read-only replay plan and outputs.",
    commands=(
        CommandMeta(name="__summary__", help="Show plan summary."),
        CommandMeta(name="diff", help="Print A vs B replay diff."),
        CommandMeta(name="sql", help="Print planned normalized schema SQL (B)."),
    ),
)

DB_GROUP = GroupMeta(
    name="db",
    help="Live database commands.",
    commands=(
        CommandMeta(name="new", help="Create a new migration file in the target migrations directory."),
        CommandMeta(name="create", help="Create the target database/dataset if missing."),
        CommandMeta(name="wait", help="Wait until the target database is reachable."),
        CommandMeta(name="up", help="Run guarded dbmate up with pre/post schema checks."),
        CommandMeta(name="migrate", help="Run guarded dbmate migrate with pre/post schema checks."),
        CommandMeta(name="status", help="Show live migration status from dbmate."),
        CommandMeta(name="drift", help="Fail if live schema differs from expected schema at current index."),
        CommandMeta(name="plan", help=DB_PLAN.help),
        CommandMeta(name="load", help="Load schema SQL into live database via dbmate."),
        CommandMeta(name="dump", help="Dump live schema SQL via dbmate."),
        CommandMeta(name="down", help="Run guarded rollback of last migration(s) with pre/post checks."),
        CommandMeta(name="drop", help="Drop the target database/dataset."),
        CommandMeta(name="dbmate", help="Pass through raw arguments to dbmate under command scope."),
    ),
    subgroups=(DB_PLAN,),
)

SCHEMA_GROUP = GroupMeta(
    name="schema",
    help="Schema artifact workflows.",
    commands=(
        CommandMeta(name="status", help="Report schema artifact health and staleness."),
        CommandMeta(name="plan", help=SCHEMA_PLAN.help),
        CommandMeta(name="apply", help="Apply schema plan and rewrite artifacts atomically."),
    ),
    subgroups=(SCHEMA_PLAN,),
)

TEMPLATE_GROUP = GroupMeta(
    name="template",
    help="Template helpers.",
    commands=(
        CommandMeta(name="config", help="Print or write starter matey.toml template."),
        CommandMeta(name="ci", help="Print or write starter CI workflow template."),
    ),
)

ROOT_GROUPS: tuple[GroupMeta, ...] = (
    DB_GROUP,
    SCHEMA_GROUP,
    TEMPLATE_GROUP,
)


def group_meta(group_name: str) -> GroupMeta:
    for group in ROOT_GROUPS:
        if group.name == group_name:
            return group
    raise KeyError(f"Unknown help group: {group_name}")


def subgroup_meta(group_name: str, subgroup_name: str) -> GroupMeta:
    group = group_meta(group_name)
    for subgroup in group.subgroups:
        if subgroup.name == subgroup_name:
            return subgroup
    raise KeyError(f"Unknown subgroup: {group_name}.{subgroup_name}")


def command_help(*, group_name: str, command_name: str, subgroup_name: str | None = None) -> str:
    group = subgroup_meta(group_name, subgroup_name) if subgroup_name else group_meta(group_name)
    for command in group.commands:
        if command.name == command_name:
            return command.help
    raise KeyError(f"Unknown command in registry: {group_name}.{command_name}")


def root_help_text() -> str:
    lines = ["matey: opinionated dbmate wrapper for repeatable migrations + schema safety.", ""]
    lines.append("Command Groups:")
    for group in ROOT_GROUPS:
        command_names = ", ".join(command.name for command in group.commands)
        lines.append(f"- {group.name}: {command_names}")
        for subgroup in group.subgroups:
            subgroup_commands = ", ".join(
                command.name for command in subgroup.commands if command.name != "__summary__"
            )
            lines.append(f"  - {group.name}.{subgroup.name}: {subgroup_commands}")
    return "\n".join(lines)
