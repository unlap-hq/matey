from __future__ import annotations

from cyclopts import App

import matey.schema as schema_api
from matey.config import TargetConfig

from ..render import Renderer
from .common import (
    AllOpt,
    BaseOpt,
    CleanOpt,
    DiffOpt,
    KeepScratchOpt,
    PathOpt,
    SqlOpt,
    TestUrlOpt,
    WorkspaceOpt,
    plan_mode,
    run_targets,
)


def register_schema_commands(*, schema_app: App, renderer: Renderer) -> None:
    @schema_app.command(name="status", sort_key=10)
    def schema_status_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        all_targets: AllOpt = False,
    ) -> None:
        """Show schema artifact health."""

        def render_target(item: TargetConfig) -> None:
            renderer.schema_status(schema_api.status(item))

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=all_targets,
            renderer=renderer,
            require_single=False,
            body=render_target,
        )

    @schema_app.command(name="plan", sort_key=20)
    def schema_plan_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        all_targets: AllOpt = False,
        base: BaseOpt = None,
        test_url: TestUrlOpt = None,
        clean: CleanOpt = False,
        keep_scratch: KeepScratchOpt = False,
        sql: SqlOpt = False,
        diff: DiffOpt = False,
    ) -> None:
        """Run validated schema replay in scratch and inspect the resulting schema."""
        mode = plan_mode(sql=sql, diff=diff)

        def render_target(item: TargetConfig) -> None:
            kwargs = {
                "base_ref": base,
                "clean": clean,
                "test_base_url": test_url,
                "keep_scratch": keep_scratch,
                "dbmate_bin": None,
            }
            match mode:
                case "summary":
                    renderer.schema_plan(schema_api.plan(item, **kwargs))
                case "sql":
                    renderer.sql_blob(schema_api.plan_sql(item, **kwargs))
                case "diff":
                    renderer.diff_blob(schema_api.plan_diff(item, **kwargs))
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

    @schema_app.command(name="apply", sort_key=30)
    def schema_apply_command(
        workspace: WorkspaceOpt = None,
        path: PathOpt = None,
        base: BaseOpt = None,
        test_url: TestUrlOpt = None,
        clean: CleanOpt = False,
        keep_scratch: KeepScratchOpt = False,
    ) -> None:
        """Run validated schema replay in scratch, then write schema artifacts."""

        def render_target(item: TargetConfig) -> None:
            renderer.schema_apply(
                schema_api.apply(
                    item,
                    base_ref=base,
                    clean=clean,
                    test_base_url=test_url,
                    keep_scratch=keep_scratch,
                    dbmate_bin=None,
                )
            )

        run_targets(
            workspace_path=workspace,
            path=path,
            all_targets=False,
            renderer=renderer,
            require_single=True,
            body=render_target,
        )


__all__ = ["register_schema_commands", "schema_api"]
