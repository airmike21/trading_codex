from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from types import SimpleNamespace

import pytest


def _mk_completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    return SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode)


def test_mode_cli_defaults_and_parses():
    from scripts import next_action_alert

    parser = next_action_alert.build_parser()
    args_default = parser.parse_args([])
    args_due = parser.parse_args(["--mode", "change_or_rebalance_due"])
    args_lock = parser.parse_args([])
    args_lock_custom = parser.parse_args(
        ["--no-lock", "--lock-timeout-seconds", "1.5", "--lock-stale-seconds", "2.5"]
    )

    assert args_default.mode == "change_only"
    assert args_due.mode == "change_or_rebalance_due"
    assert args_lock.no_lock is False
    assert args_lock.lock_timeout_seconds == 0.0
    assert args_lock.lock_stale_seconds == 3600.0
    assert args_lock_custom.no_lock is True
    assert args_lock_custom.lock_timeout_seconds == 1.5
    assert args_lock_custom.lock_stale_seconds == 2.5


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


def test_due_mode_emits_once_for_same_event_id(monkeypatch, tmp_path, capsys):
    from scripts import next_action_alert

    state = tmp_path / "state.txt"
    state.write_text("E1\n", encoding="utf-8")

    payload = {
        "event_id": "E1",
        "strategy": "dual_mom",
        "symbol": "TLT",
        "next_rebalance": "2025-01-01",
    }
    json_line = json.dumps(payload, separators=(",", ":"))

    def fake_run(argv, capture_output, text):
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        if "--next-action" in argv:
            return _mk_completed(stdout="UNUSED TEXT\n")
        return _mk_completed(stderr="bad argv", returncode=1)

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)
    monkeypatch.setattr(next_action_alert, "_today_chicago", lambda: date(2025, 1, 2))

    rc = next_action_alert.main(
        [
            "--mode",
            "change_or_rebalance_due",
            "--state-file",
            str(state),
            "--",
            "--strategy",
            "dual_mom",
        ]
    )
    assert rc == 0
    out_first = capsys.readouterr().out.splitlines()
    assert len(out_first) == 1
    assert json.loads(out_first[0])["event_id"] == "E1"

    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["last_event_id"] == "E1"
    assert saved["last_due_fingerprint"] == "dual_mom:TLT:2025-01-01"

    rc = next_action_alert.main(
        [
            "--mode",
            "change_or_rebalance_due",
            "--state-file",
            str(state),
            "--",
            "--strategy",
            "dual_mom",
        ]
    )
    assert rc == 0
    assert capsys.readouterr().out == ""


def test_due_mode_future_next_rebalance_suppresses_when_unchanged(monkeypatch, tmp_path, capsys):
    from scripts import next_action_alert

    state = tmp_path / "state.txt"
    state.write_text("E1\n", encoding="utf-8")

    payload = {
        "event_id": "E1",
        "strategy": "dual_mom",
        "symbol": "TLT",
        "next_rebalance": "2025-01-03",
    }
    json_line = json.dumps(payload, separators=(",", ":"))

    def fake_run(argv, capture_output, text):
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        if "--next-action" in argv:
            return _mk_completed(stdout="UNUSED TEXT\n")
        return _mk_completed(stderr="bad argv", returncode=1)

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)
    monkeypatch.setattr(next_action_alert, "_today_chicago", lambda: date(2025, 1, 2))

    rc = next_action_alert.main(
        [
            "--mode",
            "change_or_rebalance_due",
            "--state-file",
            str(state),
            "--",
            "--strategy",
            "dual_mom",
        ]
    )
    assert rc == 0
    assert capsys.readouterr().out == ""
    assert state.read_text(encoding="utf-8").strip() == "E1"


def test_due_mode_emit_text_single_line(monkeypatch, tmp_path, capsys):
    from scripts import next_action_alert

    state = tmp_path / "state.txt"
    state.write_text("E3\n", encoding="utf-8")

    payload = {
        "event_id": "E3",
        "strategy": "dual_mom",
        "symbol": "TLT",
        "next_rebalance": "2025-01-01",
    }
    json_line = json.dumps(payload, separators=(",", ":"))
    text_line = "SINGLE LINE TEXT"

    def fake_run(argv, capture_output, text):
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        if "--next-action" in argv:
            return _mk_completed(stdout=text_line + "\n")
        return _mk_completed(stderr="bad argv", returncode=1)

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)
    monkeypatch.setattr(next_action_alert, "_today_chicago", lambda: date(2025, 1, 2))

    rc = next_action_alert.main(
        [
            "--mode",
            "change_or_rebalance_due",
            "--emit",
            "text",
            "--state-file",
            str(state),
            "--",
            "--strategy",
            "dual_mom",
        ]
    )
    assert rc == 0
    assert capsys.readouterr().out.splitlines() == [text_line]


def test_dict_state_preserves_unknown_fields_on_write(monkeypatch, tmp_path, capsys):
    from scripts import next_action_alert

    state = tmp_path / "state.json"
    state.write_text(
        json.dumps(
            {
                "last_event_id": "E0",
                "last_due_fingerprint": "old:fp",
                "keep_me": {"a": 1},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    payload = {"event_id": "E9", "strategy": "dual_mom", "symbol": "TLT", "next_rebalance": "2025-01-01"}
    json_line = json.dumps(payload, separators=(",", ":"))

    def fake_run(argv, capture_output, text):
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        if "--next-action" in argv:
            return _mk_completed(stdout="UNUSED TEXT\n")
        return _mk_completed(stderr="bad argv", returncode=1)

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)
    monkeypatch.setattr(next_action_alert, "_today_chicago", lambda: date(2025, 1, 2))

    rc = next_action_alert.main(["--state-file", str(state), "--", "--strategy", "dual_mom"])
    assert rc == 0
    assert len(capsys.readouterr().out.splitlines()) == 1

    saved = json.loads(state.read_text(encoding="utf-8"))
    assert saved["last_event_id"] == "E9"
    assert saved["keep_me"] == {"a": 1}


def test_lock_unavailable_exits_silently(monkeypatch, tmp_path, capsys):
    from scripts import next_action_alert

    state = tmp_path / "state.txt"

    payload = {"event_id": "E10", "strategy": "dual_mom", "symbol": "TLT", "next_rebalance": "2025-01-01"}
    json_line = json.dumps(payload, separators=(",", ":"))

    def fake_run(argv, capture_output, text):
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        if "--next-action" in argv:
            return _mk_completed(stdout="SHOULD NOT PRINT\n")
        return _mk_completed(stderr="bad argv", returncode=1)

    @contextmanager
    def fake_lock(_path, _timeout_seconds, _stale_seconds):
        yield False

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)
    monkeypatch.setattr(next_action_alert, "_state_lock", fake_lock)

    rc = next_action_alert.main(["--state-file", str(state), "--", "--strategy", "dual_mom"])
    assert rc == 0
    out = capsys.readouterr().out
    assert out == ""
    assert not state.exists()
