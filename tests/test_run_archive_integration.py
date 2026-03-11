from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

from trading_codex.run_archive import recent_runs, resolve_manifest_path


def _mk_completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    return SimpleNamespace(stdout=stdout, stderr=stderr, returncode=returncode)


def _signal_payload() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "schema_version": 1,
        "schema_minor": 0,
        "date": "2026-03-09",
        "strategy": "dual_mom",
        "action": "RESIZE",
        "symbol": "EFA",
        "price": 99.16,
        "target_shares": 100,
        "resize_prev_shares": 82,
        "resize_new_shares": 100,
        "next_rebalance": "2026-03-31",
        "event_id": "2026-03-09:dual_mom:RESIZE:EFA:100:100:2026-03-31",
        "vol_target": 0.12,
        "leverage": 0.94,
    }
    return payload


def test_next_action_alert_keeps_one_line_stdout_while_archiving(
    monkeypatch,
    tmp_path: Path,
    capsys,
) -> None:
    from scripts import next_action_alert

    state_path = tmp_path / "state.txt"
    payload = _signal_payload()
    json_line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)

    def fake_run(argv, capture_output, text):
        if "--next-action-json" in argv:
            return _mk_completed(stdout=json_line + "\n")
        raise AssertionError(f"unexpected command: {argv}")

    monkeypatch.setattr(next_action_alert.subprocess, "run", fake_run)

    exit_code = next_action_alert.main(
        [
            "--emit",
            "json",
            "--state-file",
            str(state_path),
            "--",
            "--strategy",
            "dual_mom",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0
    assert captured.out == json_line + "\n"
    assert captured.err == ""
    assert state_path.read_text(encoding="utf-8").strip() == str(payload["event_id"])

    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])
    entries = recent_runs(root_dir=archive_root, limit=5)
    assert len(entries) == 1
    assert entries[0]["run_kind"] == "next_action_alert"
    manifest_path = resolve_manifest_path(entries[0], root_dir=archive_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["event_id"] == payload["event_id"]
    assert manifest["artifact_paths"]["emitted_line"] == "artifacts/emitted_line.txt"
    emitted_line = (manifest_path.parent / manifest["artifact_paths"]["emitted_line"]).read_text(encoding="utf-8")
    assert emitted_line == json_line + "\n"


def test_plan_execution_archives_review_artifacts(tmp_path: Path, capsys) -> None:
    from scripts import plan_execution

    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    positions_path = tmp_path / "positions.json"
    positions_payload = {
        "broker_name": "mock",
        "account_id": "paper-1",
        "buying_power": 10_000.0,
        "positions": [{"symbol": "EFA", "shares": 82, "price": 99.16}],
    }
    positions_path.write_text(json.dumps(positions_payload), encoding="utf-8")

    base_dir = tmp_path / "execution_plans"
    exit_code = plan_execution.main(
        [
            "--signal-json-file",
            str(signal_path),
            "--positions-file",
            str(positions_path),
            "--base-dir",
            str(base_dir),
            "--timestamp",
            "2026-03-09T10:45:00-05:00",
            "--emit",
            "json",
        ]
    )

    captured = capsys.readouterr()
    assert exit_code == 0, captured.err
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["plan_sha256"]

    archive_root = Path(os.environ["TRADING_CODEX_ARCHIVE_ROOT"])
    entries = recent_runs(root_dir=archive_root, limit=5)
    assert len(entries) == 1
    assert entries[0]["run_kind"] == "execution_plan"
    manifest_path = resolve_manifest_path(entries[0], root_dir=archive_root)
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["plan_sha256"] == payload["plan_sha256"]
    assert manifest["strategy"] == "dual_mom"
    assert manifest["artifact_paths"]["execution_plan_json"].startswith("artifacts/")
    assert manifest["artifact_paths"]["execution_plan_markdown"].startswith("artifacts/")
    assert manifest["artifact_paths"]["signal_payload"] == "artifacts/signal_payload.json"
    assert (manifest_path.parent / manifest["artifact_paths"]["execution_plan_json"]).exists()
    assert (manifest_path.parent / manifest["artifact_paths"]["execution_plan_markdown"]).exists()
