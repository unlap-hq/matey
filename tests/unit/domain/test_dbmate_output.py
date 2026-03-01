import pytest

from matey.domain.dbmate_output import DbmateOutput, extract_dump_sql, parse_status_output
from matey.domain.errors import ExternalCommandError


def test_parse_status_output_extracts_applied_rows() -> None:
    text = """
[X] migrations/1.sql
[ ] migrations/2.sql
applied: 1
"""
    snapshot = parse_status_output(text)
    assert snapshot.applied_files == ("migrations/1.sql",)
    assert snapshot.applied_count == 1


def test_parse_status_output_rejects_summary_mismatch() -> None:
    text = """
[X] migrations/1.sql
applied: 2
"""
    with pytest.raises(ExternalCommandError):
        parse_status_output(text)


def test_extract_dump_sql_requires_output() -> None:
    with pytest.raises(ExternalCommandError):
        extract_dump_sql(DbmateOutput(exit_code=0, stdout="", stderr="oops"))
