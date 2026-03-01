from matey.parsing import parse_down_section_state, parse_migration_files


def test_parse_migration_files_orders_sql_and_ignores_non_sql() -> None:
    files = [
        "migrations/20260102_b.sql",
        "migrations/readme.md",
        "migrations/20260101_a.sql",
    ]
    parsed = parse_migration_files(files)
    assert [item.filename for item in parsed] == ["20260101_a.sql", "20260102_b.sql"]


def test_parse_down_section_state_detects_executable_sql() -> None:
    sql = """
-- migrate:up
create table x(id int);

-- migrate:down
-- comment only
/* block */
;
DROP TABLE x;
"""
    state = parse_down_section_state(sql)
    assert state.marker_present is True
    assert state.has_executable_sql is True


def test_parse_down_section_state_treats_empty_down_as_irreversible() -> None:
    sql = """
-- migrate:up
create table x(id int);

-- migrate:down
-- comment only
/* block */
;
"""
    state = parse_down_section_state(sql)
    assert state.marker_present is True
    assert state.has_executable_sql is False
