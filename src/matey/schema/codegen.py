from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from plumbum import local

from matey import Engine
from matey.db_urls import sqlalchemy_target
from matey.project import TargetConfig


@dataclass(frozen=True, slots=True)
class CodegenResult:
    path: Path
    content: bytes


@dataclass(frozen=True, slots=True)
class ToolResult:
    stdout: str
    stderr: str
    exit_code: int


class CodegenError(RuntimeError):
    pass


def generate_sqlalchemy_models(
    *,
    target: TargetConfig,
    engine: Engine,
    url: str,
) -> CodegenResult:
    target_info = sqlalchemy_target(engine=engine, url=url)
    names = reflect_object_names(
        sqlalchemy_url=target_info.url,
        env_updates=target_info.env,
        engine_kwargs=target_info.engine_kwargs,
    )
    if not names:
        return CodegenResult(
            path=target.models,
            content=b"# No user tables or views reflected.\n",
        )
    return CodegenResult(
        path=target.models,
        content=run_sqlacodegen(
            target=target,
            sqlalchemy_url=target_info.url,
            env_updates=target_info.env,
            engine_kwargs=target_info.engine_kwargs,
            names=names,
        ),
    )


def reflect_object_names(
    *,
    sqlalchemy_url: str,
    env_updates: dict[str, str],
    engine_kwargs: dict[str, Any],
) -> tuple[str, ...]:
    result = run_tool(
        local[sys.executable][
            "-m",
            "matey.schema.codegen_probe",
            sqlalchemy_url,
            json.dumps(engine_kwargs),
        ],
        env=env_updates,
        failure_prefix="SQLAlchemy reflection failed",
    )
    try:
        names = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise CodegenError(f"Invalid reflection probe output: {result.stdout!r}") from error
    if not isinstance(names, list) or not all(isinstance(name, str) for name in names):
        raise CodegenError(f"Invalid reflection probe output: {result.stdout!r}")
    return tuple(names)


def run_sqlacodegen(
    *,
    target: TargetConfig,
    sqlalchemy_url: str,
    env_updates: dict[str, str],
    engine_kwargs: dict[str, Any],
    names: tuple[str, ...],
) -> bytes:
    try:
        binary = local["sqlacodegen"]
    except Exception as error:
        raise CodegenError("sqlacodegen is not installed or not on PATH.") from error

    generator = target.codegen.generator if target.codegen is not None else "tables"
    options = target.codegen.options if target.codegen is not None else None
    argv = [
        "--generator",
        generator,
        "--tables",
        ",".join(names),
    ]
    if options:
        argv += ["--options", options]
    for key, value in engine_kwargs.items():
        argv += ["--engine-arg", f"{key}={value!r}"]
    argv.append(sqlalchemy_url)

    result = run_tool(
        binary[tuple(argv)],
        env=env_updates,
        failure_prefix="sqlacodegen failed",
    )
    return result.stdout.encode("utf-8")


def run_tool(
    cmd: Any,
    *,
    env: dict[str, str],
    failure_prefix: str,
) -> ToolResult:
    try:
        exit_code, stdout, stderr = cmd.run(retcode=None, env=dict(env))
    except OSError as error:
        raise CodegenError(f"{failure_prefix}: {error}") from error
    if exit_code != 0:
        raise CodegenError(
            f"{failure_prefix} (exit_code={exit_code}): stderr={stderr.strip()!r}; stdout={stdout.strip()!r}"
        )
    return ToolResult(stdout=stdout, stderr=stderr, exit_code=exit_code)
