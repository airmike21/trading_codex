from __future__ import annotations

import csv
import json
import subprocess
import zipfile
from pathlib import Path

from scripts import paper_lane_daily_ops
from trading_codex.execution.paper_lane import DEFAULT_PAPER_STATE_KEY
from trading_codex.run_archive import recent_runs


def _write_presets(path: Path, *, data_dir: Path) -> None:
    payload = {
        "presets": {
            "dual_mom_vol10_cash_core": {
                "description": "test preset",
                "run_backtest_args": [
                    "--strategy",
                    "dual_mom_vol10_cash",
                    "--symbols",
                    "SPY",
                    "QQQ",
                    "IWM",
                    "EFA",
                    "--dmv-defensive-symbol",
                    "BIL",
                    "--data-dir",
                    str(data_dir),
                    "--no-plot",
                ],
            }
        }
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _status_payload(*, event_id: str, signal_date: str, archive_manifest_path: str) -> dict[str, object]:
    return {
        "archive_manifest_path": archive_manifest_path,
        "drift_present": False,
        "event_already_applied": False,
        "paths": {
            "state_path": "/tmp/paper/state.json",
            "ledger_path": "/tmp/paper/ledger.jsonl",
        },
        "signal": {
            "action": "HOLD",
            "date": signal_date,
            "event_id": event_id,
            "next_rebalance": "2026-04-24",
            "symbol": "BIL",
            "target_shares": 200,
        },
    }


def _apply_payload(*, event_id: str, archive_manifest_path: str) -> dict[str, object]:
    return {
        "archive_manifest_path": archive_manifest_path,
        "duplicate_event_blocked": False,
        "event_receipt_path": f"/tmp/paper/{event_id}.json",
        "result": "applied",
    }


def test_build_paper_lane_cmd_places_emit_before_subcommand(tmp_path: Path) -> None:
    presets_path = tmp_path / "presets.json"
    _write_presets(presets_path, data_dir=tmp_path / "data")

    cmd = paper_lane_daily_ops.build_paper_lane_cmd(
        repo_root=Path("/repo"),
        command="status",
        preset_name="dual_mom_vol10_cash_core",
        presets_path=presets_path,
        state_key=DEFAULT_PAPER_STATE_KEY,
        data_dir=tmp_path / "data",
        paper_base_dir=None,
        timestamp="2026-03-26T16:10:00-05:00",
    )

    assert cmd[:6] == [
        paper_lane_daily_ops.sys.executable,
        "/repo/scripts/paper_lane.py",
        "--emit",
        "json",
        "--state-key",
        DEFAULT_PAPER_STATE_KEY,
    ]
    assert cmd.index("--emit") < cmd.index("status")
    assert cmd.index("--timestamp") < cmd.index("status")
    assert cmd[cmd.index("status") + 1 : cmd.index("status") + 5] == [
        "--preset",
        "dual_mom_vol10_cash_core",
        "--presets-file",
        str(presets_path),
    ]


def test_main_appends_history_and_generates_xlsx(tmp_path: Path, monkeypatch) -> None:
    presets_path = tmp_path / "presets.json"
    archive_root = tmp_path / "archive"
    data_dir = tmp_path / "data"
    _write_presets(presets_path, data_dir=data_dir)

    runs = [
        (
            "2026-03-26T16:10:00-05:00",
            _status_payload(
                event_id="evt-1",
                signal_date="2026-03-26",
                archive_manifest_path="/tmp/status-1.json",
            ),
            _apply_payload(event_id="evt-1", archive_manifest_path="/tmp/apply-1.json"),
        ),
        (
            "2026-03-27T16:10:00-05:00",
            _status_payload(
                event_id="evt-2",
                signal_date="2026-03-27",
                archive_manifest_path="/tmp/status-2.json",
            ),
            _apply_payload(event_id="evt-2", archive_manifest_path="/tmp/apply-2.json"),
        ),
    ]
    seen_commands: list[list[str]] = []
    current = {"value": 0}

    def fake_run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
        seen_commands.append(cmd)
        _, status_payload, apply_payload = runs[current["value"]]
        script_name = Path(cmd[1]).name
        if script_name == "update_data_eod.py":
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="",
                stderr="[update_data_eod] SPY: wrote rows=100\n[update_data_eod] updated_symbols=5\n",
            )
        if script_name == "paper_lane.py" and "status" in cmd:
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(status_payload), stderr="")
        if script_name == "paper_lane.py" and "apply" in cmd:
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(apply_payload), stderr="")
        raise AssertionError(f"Unexpected command: {cmd}")

    monkeypatch.setattr(paper_lane_daily_ops, "_run_process", fake_run_process)

    for index, (timestamp, _, _) in enumerate(runs):
        current["value"] = index
        rc = paper_lane_daily_ops.main(
            [
                "--presets-file",
                str(presets_path),
                "--archive-root",
                str(archive_root),
                "--timestamp",
                timestamp,
            ]
        )
        assert rc == 0

    ops_paths = paper_lane_daily_ops.resolve_ops_paths(
        state_key=DEFAULT_PAPER_STATE_KEY,
        archive_root=archive_root,
        create=False,
    )
    rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
    assert [row["status_event_id"] for row in rows] == ["evt-1", "evt-2"]
    assert [row["successful_signal_days_recorded"] for row in rows] == [1, 2]

    with ops_paths["csv_path"].open("r", encoding="utf-8", newline="") as fh:
        csv_rows = list(csv.DictReader(fh))
    assert [row["status_event_id"] for row in csv_rows] == ["evt-1", "evt-2"]

    with zipfile.ZipFile(ops_paths["xlsx_path"], "r") as zf:
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
    assert "evt-1" in sheet_xml
    assert "evt-2" in sheet_xml
    assert "paper_lane_daily_ops_runs.csv" in sheet_xml

    archives = recent_runs(root_dir=archive_root, limit=4)
    assert len(archives) >= 2
    assert archives[0]["run_kind"] == "paper_lane_daily_ops"

    status_commands = [cmd for cmd in seen_commands if Path(cmd[1]).name == "paper_lane.py" and "status" in cmd]
    apply_commands = [cmd for cmd in seen_commands if Path(cmd[1]).name == "paper_lane.py" and "apply" in cmd]
    assert len(status_commands) == 2
    assert len(apply_commands) == 2
    assert all(cmd.index("--emit") < cmd.index("status") for cmd in status_commands)
    assert all(cmd.index("--emit") < cmd.index("apply") for cmd in apply_commands)


def test_main_logs_failed_step_and_returns_nonzero(tmp_path: Path, monkeypatch, capsys) -> None:
    presets_path = tmp_path / "presets.json"
    archive_root = tmp_path / "archive"
    data_dir = tmp_path / "data"
    _write_presets(presets_path, data_dir=data_dir)

    def fake_run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
        script_name = Path(cmd[1]).name
        if script_name == "update_data_eod.py":
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="",
                stderr="[update_data_eod] updated_symbols=5\n",
            )
        if script_name == "paper_lane.py" and "status" in cmd:
            return subprocess.CompletedProcess(
                cmd,
                2,
                stdout="",
                stderr="[paper_lane] ERROR: Paper lane state does not exist yet: /tmp/paper/state.json. Run init first.\n",
            )
        raise AssertionError(f"Unexpected command after failure: {cmd}")

    monkeypatch.setattr(paper_lane_daily_ops, "_run_process", fake_run_process)

    rc = paper_lane_daily_ops.main(
        [
            "--presets-file",
            str(presets_path),
            "--archive-root",
            str(archive_root),
            "--timestamp",
            "2026-03-26T16:10:00-05:00",
        ]
    )
    captured = capsys.readouterr()

    assert rc == 2
    assert "step paper_lane_status failed" in captured.err

    ops_paths = paper_lane_daily_ops.resolve_ops_paths(
        state_key=DEFAULT_PAPER_STATE_KEY,
        archive_root=archive_root,
        create=False,
    )
    rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
    assert len(rows) == 1
    assert rows[0]["overall_result"] == "failed"
    assert rows[0]["failed_step"] == "paper_lane_status"
    assert rows[0]["apply_exit_code"] == ""
    assert ops_paths["csv_path"].exists()
    assert ops_paths["xlsx_path"].exists()
