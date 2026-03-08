from __future__ import annotations

from cyclopts import App

import matey.db as db_api
from matey.project import TargetConfig

from ..render import Renderer
from . import common
from .common import (
    AllOpt,
    CliUsageError,
    DiffOpt,
    PathOpt,
    SqlOpt,
    StepsOpt,
    UrlOpt,
    WorkspaceOpt,
    plan_mode,
    render_cmd_blob,
    run_targets,
)


def register_db_commands(*, db_app: App, root_app: App, renderer: Renderer) -> None:
    @db_app.command(name="status", sort_key=10)
    def status_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        all_targets: AllOpt = False,
        url: UrlOpt = None,
    ) -> None:
        """Show live migration status."""

        def render_target(item: TargetConfig) -> None:
            render_cmd_blob(
                renderer=renderer,
                result=db_api.status_raw(item, url=url, dbmate_bin=None),
                context="db status",
            )

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

    @db_app.command(name="bootstrap", sort_key=15)
    def bootstrap_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Load schema.sql into an empty DB and verify dbmate head state."""

        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation(
                "bootstrap",
                db_api.bootstrap(item, url=url, dbmate_bin=None),
            )

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="up", sort_key=20)
    def up_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Create DB if missing, then apply pending migrations."""

        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation("up", db_api.up(item, url=url, dbmate_bin=None))

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="migrate", sort_key=30)
    def migrate_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        url: UrlOpt = None,
    ) -> None:
        """Apply pending migrations (no create-if-needed)."""

        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation(
                "migrate",
                db_api.migrate(item, url=url, dbmate_bin=None),
            )

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="down", sort_key=40)
    def down_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        url: UrlOpt = None,
        steps: StepsOpt = 1,
    ) -> None:
        """Rollback migration(s)."""

        def render_target(item: TargetConfig) -> None:
            renderer.db_mutation(
                "down",
                db_api.down(item, steps=steps, url=url, dbmate_bin=None),
            )

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @db_app.command(name="drift", sort_key=50)
    def drift_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        all_targets: AllOpt = False,
        url: UrlOpt = None,
    ) -> None:
        """Check live schema drift."""

        def render_target(item: TargetConfig) -> None:
            renderer.db_drift(db_api.drift(item, url=url, dbmate_bin=None))

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

    @db_app.command(name="plan", sort_key=60)
    def db_plan_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        all_targets: AllOpt = False,
        url: UrlOpt = None,
        sql: SqlOpt = False,
        diff: DiffOpt = False,
    ) -> None:
        """Compare live schema to expected worktree target schema."""
        mode = plan_mode(sql=sql, diff=diff)

        def render_target(item: TargetConfig) -> None:
            match mode:
                case "summary":
                    renderer.db_plan(db_api.plan(item, url=url, dbmate_bin=None))
                case "sql":
                    renderer.sql_blob(db_api.plan_sql(item, url=url, dbmate_bin=None))
                case "diff":
                    renderer.diff_blob(db_api.plan_diff(item, url=url, dbmate_bin=None))
                case _:
                    raise AssertionError("invalid plan mode")

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

    @db_app.command(name="new", sort_key=70)
    def new_command(
        name: str,
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
    ) -> None:
        """Create a new migration file."""

        def render_target(item: TargetConfig) -> None:
            render_cmd_blob(
                renderer=renderer,
                result=db_api.new(item, name=name, dbmate_bin=None),
                context="db new",
            )

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )

    @root_app.command(name="dbmate", sort_key=50, help_flags=[])
    def dbmate_passthrough_command(*args: str) -> None:
        """Run dbmate directly with verbatim arguments."""
        # Keep a registered command so the root help surface advertises dbmate,
        # while the actual implementation stays shared with the top-level argv intercept.
        raise SystemExit(
            common.handle_dbmate_passthrough(
                argv=args,
                renderer=renderer,
                dbmate_bin=None,
            )
        )


__all__ = ["CliUsageError", "db_api", "register_db_commands"]
