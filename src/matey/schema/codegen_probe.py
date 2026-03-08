from __future__ import annotations

import json
import sys

from sqlalchemy import create_engine, inspect


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2:
        raise SystemExit(
            "usage: python -m matey.schema.codegen_probe <sqlalchemy-url> <engine-kwargs-json>"
        )

    sqlalchemy_url, engine_kwargs_json = args
    engine_kwargs = json.loads(engine_kwargs_json)

    engine = create_engine(sqlalchemy_url, **engine_kwargs)
    try:
        inspector = inspect(engine)
        table_names = tuple(inspector.get_table_names())
        try:
            view_names = tuple(inspector.get_view_names())
        except NotImplementedError:
            view_names = ()
    finally:
        engine.dispose()

    names = sorted(name for name in {*table_names, *view_names} if name != "schema_migrations")
    print(json.dumps(names))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
