from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _mk_completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    return SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode)


def test_alert_emits_once_then_suppresses(monkeypatch, tmp_path, capsys):
    # Import after monkeypatching path if needed
    from scripts import next_action_alert

    state = tmp_path / "state.txt"

    # First call returns JSON with event_id=E1; second call (text) returns one line
    payload = {
        "schema_name": "next_action",
        "schema_version": 1,
        "schema_minor": 0,
        "event_id": "E1",
        "date": "2005-05-02",
        "strategy": "dual_mom",
        "action": "HOLD",
        "symbol": "TLT",
        "target_shares": 160,
        "resize_new_shares": 160,
        "next_rebalance": "2005-05-31",
    }
    json_line = json.dumps(payload, separators=(",", ":"))
    text_line = "2005-05-02 dual_mom HOLD TLT"

    calls = []

    def fake_run(argv, capture_output, text):
        calls.append(argv)
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        if "--next-action" in argv:
            return _mk_completed(stdout=text_line + "\n")
        return _mk_completed(stderr="bad argv", returncode=1)

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)

    # First run should emit (json by default) and write state
    rc = next_action_alert.main(["--state-file", str(state), "--", "--strategy", "dual_mom"])
    assert rc == 0
    out = capsys.readouterr().out.splitlines()
    assert len(out) == 1
    assert json.loads(out[0])["event_id"] == "E1"
    assert state.read_text().strip() == "E1"

    # Second run with same event_id should emit nothing
    rc = next_action_alert.main(["--state-file", str(state), "--", "--strategy", "dual_mom"])
    assert rc == 0
    out = capsys.readouterr().out
    assert out == ""


def test_alert_emit_text(monkeypatch, tmp_path, capsys):
    from scripts import next_action_alert

    state = tmp_path / "state.txt"

    payload = {"event_id": "E2"}
    json_line = json.dumps(payload, separators=(",", ":"))
    text_line = "SINGLE LINE TEXT"

    def fake_run(argv, capture_output, text):
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        if "--next-action" in argv:
            return _mk_completed(stdout=text_line + "\n")
        return _mk_completed(stderr="bad argv", returncode=1)

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)

    rc = next_action_alert.main(["--emit", "text", "--state-file", str(state), "--", "--strategy", "dual_mom"])
    assert rc == 0
    out_lines = capsys.readouterr().out.splitlines()
    assert out_lines == [text_line]
    assert state.read_text().strip() == "E2"
