from __future__ import annotations

import typer

from matey.cli import db, lock, schema, template
from matey.cli.help import group_panel, root_group_help, subgroup_help
from matey.cli.options import main_callback

app = typer.Typer(
    add_completion=False,
    no_args_is_help=True,
    help="matey: opinionated dbmate wrapper for repeatable migrations + schema safety.",
)

schema_app = typer.Typer(no_args_is_help=True, help=subgroup_help("schema"))
app.add_typer(
    schema_app,
    name="schema",
    help=root_group_help("schema"),
    rich_help_panel=group_panel("schema"),
)

db_app = typer.Typer(no_args_is_help=True, help=subgroup_help("db"))
app.add_typer(
    db_app,
    name="db",
    help=root_group_help("db"),
    rich_help_panel=group_panel("db"),
)

lock_app = typer.Typer(no_args_is_help=True, help=subgroup_help("lock"))
app.add_typer(
    lock_app,
    name="lock",
    help=root_group_help("lock"),
    rich_help_panel=group_panel("lock"),
)

ci_app = typer.Typer(no_args_is_help=True, help=subgroup_help("ci"))
app.add_typer(
    ci_app,
    name="ci",
    help=root_group_help("ci"),
    rich_help_panel=group_panel("ci"),
)

config_app = typer.Typer(no_args_is_help=True, help=subgroup_help("config"))
app.add_typer(
    config_app,
    name="config",
    help=root_group_help("config"),
    rich_help_panel=group_panel("config"),
)

app.callback()(main_callback)

db.register(db_app)
schema.register(schema_app)
template.register_ci(ci_app)
template.register_config(config_app)
lock.register(lock_app)
