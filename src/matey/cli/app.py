from __future__ import annotations

import typer

from matey.cli.commands import cd, ci, config, db, dbmate, lock, schema
from matey.cli.common import main_callback

_SCHEMA_PANEL_HELP = (
    "Schema workflows.\n"
    "validate: verify schema.sql vs lock/checkpoint replay.\n"
    "regen: rebuild schema.sql and lock metadata from replay.\n"
    "diff: show repo schema vs replay diff.\n"
    "Run `matey schema <command> --help` for command flags."
)

_DB_PANEL_HELP = (
    "Live database workflows.\n"
    "up: create-if-needed and apply pending migrations.\n"
    "migrate: apply pending migrations without implicit create.\n"
    "down: roll back N steps (default 1).\n"
    "diff: compare live schema against lockfile-expected schema.\n"
    "Run `matey db <command> --help` for command flags."
)

_LOCK_PANEL_HELP = (
    "Lockfile workflows.\n"
    "doctor: verify lock/checkpoint/schema integrity.\n"
    "sync: deterministically regenerate schema.lock.toml.\n"
    "Run `matey lock <command> --help` for command flags."
)

_CI_PANEL_HELP = (
    "CI helper workflows.\n"
    "init: write provider CI templates.\n"
    "print: print provider CI templates to stdout.\n"
    "Run `matey ci <command> --help` for command flags."
)

_CD_PANEL_HELP = (
    "CD helper workflows.\n"
    "init: write provider CD templates.\n"
    "print: print provider CD templates to stdout.\n"
    "Run `matey cd <command> --help` for command flags."
)

_CONFIG_PANEL_HELP = (
    "Config helpers.\n"
    "init: write a matey.toml skeleton.\n"
    "print: print a matey.toml skeleton to stdout.\n"
    "Run `matey config <command> --help` for command flags."
)

_PROJECT_SETUP_PANEL = "Project Setup"

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="matey: opinionated dbmate wrapper for repeatable migrations + schema safety.",
)
schema_app = typer.Typer(no_args_is_help=True, help=_SCHEMA_PANEL_HELP)
app.add_typer(schema_app, name="schema", rich_help_panel="Schema Workflows")
db_app = typer.Typer(no_args_is_help=True, help=_DB_PANEL_HELP)
app.add_typer(db_app, name="db", rich_help_panel="Database Workflows")
lock_app = typer.Typer(no_args_is_help=True, help=_LOCK_PANEL_HELP)
app.add_typer(lock_app, name="lock", rich_help_panel="Lock Workflows")
ci_app = typer.Typer(no_args_is_help=True, help=_CI_PANEL_HELP)
app.add_typer(ci_app, name="ci", rich_help_panel=_PROJECT_SETUP_PANEL)
cd_app = typer.Typer(no_args_is_help=True, help=_CD_PANEL_HELP)
app.add_typer(cd_app, name="cd", rich_help_panel=_PROJECT_SETUP_PANEL)
config_app = typer.Typer(no_args_is_help=True, help=_CONFIG_PANEL_HELP)
app.add_typer(config_app, name="config", rich_help_panel=_PROJECT_SETUP_PANEL)

app.callback()(main_callback)

db.register(db_app)
schema.register(schema_app)
ci.register(ci_app)
cd.register(cd_app)
config.register(config_app)
dbmate.register(db_app)
lock.register(lock_app)
