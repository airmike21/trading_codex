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


def test_explicit_state_file_ignores_key(tmp_path):
    from scripts import next_action_alert

    explicit = tmp_path / "legacy_state.txt"
    resolved = next_action_alert.resolve_state_path(
        state_file=str(explicit),
        state_dir=tmp_path / "ignored",
        state_key="my-key",
        derived_key_inputs={"strategy": "dual_mom"},
    )

    assert resolved == explicit


def test_state_key_changes_filename(tmp_path):
    from scripts import next_action_alert

    state_dir = tmp_path / "state"
    derived_inputs = {
        "strategy": "dual_mom",
        "symbol": "TLT",
        "symbols": ["SPY", "QQQ", "IWM", "EFA"],
        "defensive": "TLT",
        "args_fingerprint": "--strategy dual_mom --symbols SPY QQQ IWM EFA --defensive TLT",
    }

    path_a = next_action_alert.resolve_state_path(
        state_file=None,
        state_dir=state_dir,
        state_key="monitor-a",
        derived_key_inputs=derived_inputs,
    )
    path_b = next_action_alert.resolve_state_path(
        state_file=None,
        state_dir=state_dir,
        state_key="monitor-b",
        derived_key_inputs=derived_inputs,
    )

    assert path_a != path_b
    assert path_a.parent == state_dir
    assert path_b.parent == state_dir
    assert path_a.name.startswith("next_action_alert.")
    assert path_a.suffix == ".json"


def test_auto_derived_key_differs_for_diff_monitor_inputs(tmp_path):
    from scripts import next_action_alert

    state_dir = tmp_path / "state"
    inputs_a = {
        "strategy": "dual_mom",
        "symbol": "TLT",
        "symbols": ["SPY", "QQQ", "IWM", "EFA"],
        "defensive": "TLT",
        "args_fingerprint": "--strategy dual_mom --symbols SPY QQQ IWM EFA --defensive TLT",
    }
    inputs_b = {
        "strategy": "sma200",
        "symbol": "SPY",
        "symbols": ["SPY"],
        "defensive": None,
        "args_fingerprint": "--strategy sma200 --symbols SPY",
    }

    path_a = next_action_alert.resolve_state_path(
        state_file=None,
        state_dir=state_dir,
        state_key=None,
        derived_key_inputs=inputs_a,
    )
    path_b = next_action_alert.resolve_state_path(
        state_file=None,
        state_dir=state_dir,
        state_key=None,
        derived_key_inputs=inputs_b,
    )

    assert path_a != path_b
    assert path_a.parent == state_dir
    assert path_b.parent == state_dir
