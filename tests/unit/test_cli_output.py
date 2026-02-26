from __future__ import annotations

from io import StringIO

from rich.console import Console

from matey.cli.output import OutputOptions, RichDbmateRenderer
from matey.drivers.dbmate import DbmateExecutionResult, DbmateLogContext


def _renderer(*, verbose: bool = False, quiet: bool = False, tail_lines: int = 40):
    stdout = StringIO()
    stderr = StringIO()
    renderer = RichDbmateRenderer(
        options=OutputOptions(
            verbose=verbose,
            quiet=quiet,
            failure_tail_lines=tail_lines,
        ),
        stdout_console=Console(file=stdout, force_terminal=False, color_system=None),
        stderr_console=Console(file=stderr, force_terminal=False, color_system=None),
    )
    return renderer, stdout, stderr


def _result(
    *,
    returncode: int,
    stdout: str = "",
    stderr: str = "",
    context: DbmateLogContext | None = None,
) -> DbmateExecutionResult:
    return DbmateExecutionResult(
        command=("/bin/dbmate", "--url", "postgres://db", "up"),
        display_command=("/bin/dbmate", "--url", "postgres://db", "up"),
        returncode=returncode,
        duration_seconds=1.23,
        stdout=stdout,
        stderr=stderr,
        verb="up",
        context=context,
        captured=False,
    )


def test_renderer_success_default() -> None:
    renderer, stdout, stderr = _renderer()
    renderer.handle(
        _result(
            returncode=0,
            context=DbmateLogContext(target="core", phase="clean", step="create"),
        )
    )
    assert "[OK] target=core phase=clean step=create ok 1.23s" in stdout.getvalue()
    assert stderr.getvalue() == ""


def test_renderer_quiet_suppresses_success() -> None:
    renderer, stdout, stderr = _renderer(quiet=True)
    renderer.handle(_result(returncode=0))
    assert stdout.getvalue() == ""
    assert stderr.getvalue() == ""


def test_renderer_failure_non_verbose_uses_tails() -> None:
    renderer, stdout, stderr = _renderer(tail_lines=2)
    renderer.handle(
        _result(
            returncode=2,
            stdout="a\nb\nc\n",
            stderr="d\ne\nf\n",
            context=DbmateLogContext(target="core", phase="upgrade", step="up(head)"),
        )
    )
    assert stdout.getvalue() == ""
    err_text = stderr.getvalue()
    assert "[ERR] target=core phase=upgrade step=up(head) failed exit=2 1.23s" in err_text
    assert "[CMD] /bin/dbmate --url postgres://db up" in err_text
    assert "[OUT] stdout tail:" in err_text
    assert "[STDERR] stderr tail:" in err_text
    assert "(last 2 lines)\nb\nc" in err_text
    assert "(last 2 lines)\ne\nf" in err_text


def test_renderer_failure_verbose_prints_full_streams() -> None:
    renderer, stdout, stderr = _renderer(verbose=True)
    renderer.handle(
        _result(
            returncode=1,
            stdout="full-out",
            stderr="full-err",
        )
    )
    err_text = stderr.getvalue()
    assert "[ERR] step=up failed exit=1 1.23s" in err_text
    assert "[OUT] stdout:" in err_text
    assert "full-out" in err_text
    assert "[STDERR] stderr:" in err_text
    assert "full-err" in err_text
    assert stdout.getvalue() == ""
