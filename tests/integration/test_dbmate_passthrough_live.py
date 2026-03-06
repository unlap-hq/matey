from __future__ import annotations

import os

import pytest

from matey.dbmate import default_dbmate_binary, passthrough

pytestmark = pytest.mark.integration


def test_dbmate_passthrough_help_smoke() -> None:
    dbmate_bin = default_dbmate_binary()
    if not dbmate_bin.exists() or not dbmate_bin.is_file():
        pytest.skip(f"Bundled dbmate binary not available: {dbmate_bin}")
    if not os.access(dbmate_bin, os.X_OK):
        pytest.skip(f"Bundled dbmate binary is not executable: {dbmate_bin}")

    result = passthrough("--help")

    assert result.exit_code == 0
    assert "dbmate" in (result.stdout + result.stderr).lower()
