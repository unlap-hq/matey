from __future__ import annotations

from dataclasses import dataclass
from importlib.metadata import entry_points
from pathlib import Path

from sqlalchemy import MetaData, create_engine, inspect

from matey import Engine
from matey.db_urls import SqlAlchemyTarget, sqlalchemy_target
from matey.project import TargetConfig


@dataclass(frozen=True, slots=True)
class CodegenResult:
    path: Path
    content: bytes


class CodegenError(RuntimeError):
    pass


def generate_sqlalchemy_models(
    *,
    target: TargetConfig,
    engine: Engine,
    url: str,
) -> CodegenResult:
    sqlalchemy = sqlalchemy_target(engine=engine, url=url)
    return CodegenResult(
        path=target.models,
        content=_generate_models(target=target, sqlalchemy=sqlalchemy),
    )


def _generate_models(*, target: TargetConfig, sqlalchemy: SqlAlchemyTarget) -> bytes:
    engine = create_engine(
        sqlalchemy.url,
        connect_args=sqlalchemy.connect_args,
        **sqlalchemy.engine_kwargs,
    )
    try:
        names = _reflect_object_names(engine)
        if not names:
            return b"# No user tables or views reflected.\n"

        generators = {ep.name: ep for ep in entry_points(group="sqlacodegen.generators")}
        generator_name = target.codegen.generator if target.codegen is not None else "tables"
        generator_class = generators[generator_name].load()
        options = (
            set(target.codegen.options.split(","))
            if target.codegen is not None and target.codegen.options
            else set()
        )
        metadata = MetaData()
        generator = generator_class(metadata, engine, options)
        metadata.reflect(engine, None, generator.views_supported, names)
        return generator.generate().encode("utf-8")
    except Exception as error:
        raise CodegenError(f"sqlacodegen failed: {error}") from error
    finally:
        engine.dispose()


def _reflect_object_names(engine) -> tuple[str, ...]:
    inspector = inspect(engine)
    table_names = tuple(inspector.get_table_names())
    try:
        view_names = tuple(inspector.get_view_names())
    except NotImplementedError:
        view_names = ()
    return tuple(
        sorted(name for name in {*table_names, *view_names} if name != "schema_migrations")
    )
