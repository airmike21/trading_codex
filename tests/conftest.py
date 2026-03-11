from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _isolate_run_archive_root(monkeypatch: pytest.MonkeyPatch, tmp_path):
    monkeypatch.setenv("TRADING_CODEX_ARCHIVE_ROOT", str(tmp_path / "run_archive"))
