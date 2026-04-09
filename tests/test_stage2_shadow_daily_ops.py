from __future__ import annotations

import csv
import json
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from scripts import paper_lane_daily_ops, stage2_shadow_daily_ops


def _write_config(path: Path, *, active_pair: dict[str, object] | None) -> None:
    payload = {
        "schema_name": stage2_shadow_daily_ops.CONFIG_SCHEMA_NAME,
        "schema_version": stage2_shadow_daily_ops.CONFIG_SCHEMA_VERSION,
        "active_pair": active_pair,
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _active_pair_config(*, replay_enabled: bool) -> dict[str, object]:
    local_replay: dict[str, object] = {
        "enabled": replay_enabled,
    }
    if replay_enabled:
        local_replay.update(
            {
                "state_key": "primary_live_candidate_v1_vol_managed_shadow_replay",
                "starting_cash": 100000.0,
            }
        )
    return {
        "pair_id": stage2_shadow_daily_ops.SUPPORTED_PAIR_ID,
        "primary_strategy_id": stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_ID,
        "shadow_strategy_id": stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
        "local_replay": local_replay,
    }


def _summary_row(*, ops_paths: dict[str, Path]) -> dict[str, object]:
    row = {column: "" for column in stage2_shadow_daily_ops.RUN_LOG_COLUMNS}
    row.update(
        {
            "schema_name": stage2_shadow_daily_ops.SUMMARY_SCHEMA_NAME,
            "schema_version": stage2_shadow_daily_ops.SUMMARY_SCHEMA_VERSION,
            "run_id": "existing-run",
            "timestamp_chicago": "2026-04-08T16:10:00-05:00",
            "ops_date": "2026-04-08",
            "overall_result": "noop",
            "no_op_reason": "no_active_pair_configured",
            "daily_ops_jsonl_path": str(ops_paths["jsonl_path"]),
            "daily_ops_csv_path": str(ops_paths["csv_path"]),
            "daily_ops_xlsx_path": str(ops_paths["xlsx_path"]),
        }
    )
    return row


def _write_compare_artifacts(compare_root: Path) -> dict[str, str]:
    report_dir = compare_root / "2026-04-07"
    candidate_outputs_dir = report_dir / "candidate_outputs"
    candidate_reviews_dir = report_dir / "candidate_reviews"
    candidate_outputs_dir.mkdir(parents=True, exist_ok=True)
    candidate_reviews_dir.mkdir(parents=True, exist_ok=True)

    report_json = report_dir / "comparison_report.json"
    report_markdown = report_dir / "comparison_report.md"
    scoreboard_csv = report_dir / "scoreboard.csv"
    shadow_output_json = candidate_outputs_dir / f"{stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID}.json"
    primary_output_json = candidate_outputs_dir / f"{stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_ID}.json"
    shadow_review_json = candidate_reviews_dir / "shadow_review.json"
    shadow_review_markdown = candidate_reviews_dir / "shadow_review.md"
    primary_review_json = candidate_reviews_dir / "primary_review.json"
    primary_review_markdown = candidate_reviews_dir / "primary_review.md"

    shadow_signal = {
        "action": "ENTER",
        "date": "2026-04-07",
        "event_id": "2026-04-07:primary_live_candidate_v1_vol_managed:ENTER:SPY:150::2026-05-01",
        "next_rebalance": "2026-05-01",
        "price": 500.0,
        "strategy": stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
        "symbol": "SPY",
        "target_shares": 150,
    }
    shadow_output_json.write_text(
        json.dumps(
            {
                "artifact_type": "stage2_shadow_candidate_output",
                "artifact_version": 1,
                "strategy_id": stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
                "template_output": {
                    "signal": shadow_signal,
                    "target_weights": {},
                    "diagnostics": {},
                    "reports": {},
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    primary_output_json.write_text(
        json.dumps(
            {
                "artifact_type": "stage2_shadow_candidate_output",
                "artifact_version": 1,
                "strategy_id": stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_ID,
                "template_output": {
                    "signal": {
                        "action": "HOLD",
                        "date": "2026-04-07",
                        "strategy": stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_ID,
                        "symbol": "BIL",
                        "target_shares": 200,
                    }
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    shadow_review_json.write_text(json.dumps({"artifact_type": "shadow_review"}, indent=2) + "\n", encoding="utf-8")
    shadow_review_markdown.write_text("# Shadow Review\n", encoding="utf-8")
    primary_review_json.write_text(json.dumps({"artifact_type": "shadow_review"}, indent=2) + "\n", encoding="utf-8")
    primary_review_markdown.write_text("# Primary Review\n", encoding="utf-8")
    report_markdown.write_text("# Stage 2 Shadow Compare\n", encoding="utf-8")
    scoreboard_csv.write_text("strategy_id,current_decision\nshadow,remain shadow-only\n", encoding="utf-8")

    report_json.write_text(
        json.dumps(
            {
                "artifact_type": "stage2_shadow_compare",
                "artifact_version": 1,
                "pair_id": stage2_shadow_daily_ops.SUPPORTED_PAIR_ID,
                "as_of_date": "2026-04-07",
                "current_decision": "remain shadow-only",
                "candidates": {
                    stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_ID: {
                        "review_summary": {
                            "shadow_review_state": "clean",
                            "automation_decision": "allow",
                            "automation_status": "automation_ready",
                        },
                        "artifacts": {
                            "review_json": str(primary_review_json),
                            "review_markdown": str(primary_review_markdown),
                        },
                    },
                    stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID: {
                        "review_summary": {
                            "shadow_review_state": "clean",
                            "automation_decision": "allow",
                            "automation_status": "automation_ready",
                        },
                        "artifacts": {
                            "review_json": str(shadow_review_json),
                            "review_markdown": str(shadow_review_markdown),
                        },
                    },
                },
                "comparison": {
                    "action_comparison": {
                        "primary_action": "HOLD",
                        "primary_symbol": "BIL",
                        "shadow_action": "ENTER",
                        "shadow_symbol": "SPY",
                        "shadow_next_rebalance": "2026-05-01",
                    }
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "pair_id": stage2_shadow_daily_ops.SUPPORTED_PAIR_ID,
        "as_of_date": "2026-04-07",
        "current_decision": "remain shadow-only",
        "report_json": str(report_json),
        "report_markdown": str(report_markdown),
        "scoreboard_csv": str(scoreboard_csv),
        "primary_output_json": str(primary_output_json),
        "shadow_output_json": str(shadow_output_json),
        "shadow_review_markdown": str(shadow_review_markdown),
    }


def test_load_shadow_ops_config_defaults_missing_local_replay_enabled_to_false(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "shadow_ops.json"
    active_pair = _active_pair_config(replay_enabled=False)
    local_replay = active_pair["local_replay"]
    assert isinstance(local_replay, dict)
    del local_replay["enabled"]
    _write_config(config_path, active_pair=active_pair)

    config = stage2_shadow_daily_ops.load_shadow_ops_config(config_path)

    assert config.active_pair is not None
    assert config.active_pair.local_replay.enabled is False
    assert config.active_pair.local_replay.state_key is None
    assert config.active_pair.local_replay.starting_cash is None


def test_main_rejects_non_boolean_local_replay_enabled(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    config_path = tmp_path / "shadow_ops.json"
    active_pair = _active_pair_config(replay_enabled=False)
    local_replay = active_pair["local_replay"]
    assert isinstance(local_replay, dict)
    local_replay["enabled"] = "false"
    _write_config(config_path, active_pair=active_pair)

    def fail_run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
        raise AssertionError(f"invalid config should fail before launching subprocesses: {cmd}")

    monkeypatch.setattr(stage2_shadow_daily_ops, "_run_process", fail_run_process)

    rc = stage2_shadow_daily_ops.main(
        [
            "--shadow-ops-config",
            str(config_path),
            "--archive-root",
            str(tmp_path / "archive"),
            "--timestamp",
            "2026-04-08T16:10:00-05:00",
        ]
    )
    captured = capsys.readouterr()

    assert rc == 2
    assert "active_pair.local_replay.enabled must be a boolean." in captured.err


def test_main_noops_when_no_active_pair_is_configured(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    config_path = tmp_path / "shadow_ops.json"
    archive_root = tmp_path / "archive"
    _write_config(config_path, active_pair=None)

    def fail_run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
        raise AssertionError(f"no-op run should not launch subprocesses: {cmd}")

    monkeypatch.setattr(stage2_shadow_daily_ops, "_run_process", fail_run_process)

    rc = stage2_shadow_daily_ops.main(
        [
            "--shadow-ops-config",
            str(config_path),
            "--archive-root",
            str(archive_root),
            "--timestamp",
            "2026-04-08T16:10:00-05:00",
            "--emit",
            "json",
        ]
    )
    captured = capsys.readouterr()

    assert rc == 0, captured.err
    payload = json.loads(captured.out.strip())
    assert payload["summary"]["overall_result"] == "noop"
    assert payload["summary"]["no_op_reason"] == "no_active_pair_configured"

    ops_paths = stage2_shadow_daily_ops.resolve_ops_paths(
        scope_key=stage2_shadow_daily_ops.UNCONFIGURED_SCOPE_KEY,
        archive_root=archive_root,
        create=False,
    )
    rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
    assert len(rows) == 1
    assert rows[0]["overall_result"] == "noop"
    assert rows[0]["update_exit_code"] == ""

    with ops_paths["csv_path"].open("r", encoding="utf-8", newline="") as fh:
        csv_rows = list(csv.DictReader(fh))
    assert len(csv_rows) == 1
    assert csv_rows[0]["no_op_reason"] == "no_active_pair_configured"

    with zipfile.ZipFile(ops_paths["xlsx_path"], "r") as zf:
        sheet_xml = zf.read("xl/worksheets/sheet1.xml").decode("utf-8")
    assert "no_active_pair_configured" in sheet_xml

    manifest_path = Path(payload["archive_manifest_path"])
    assert manifest_path.exists()


def test_main_runs_compare_and_shadow_replay_when_configured(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    config_path = tmp_path / "shadow_ops.json"
    archive_root = tmp_path / "archive"
    compare_root = (
        archive_root
        / "stage2_shadow_compare"
        / stage2_shadow_daily_ops.SUPPORTED_PAIR_ID
    )
    paper_state_key = "primary_live_candidate_v1_vol_managed_shadow_replay"
    paper_base_dir = archive_root / "paper_lane" / paper_state_key
    manifests_dir = tmp_path / "manifests"
    manifests_dir.mkdir(parents=True, exist_ok=True)
    _write_config(config_path, active_pair=_active_pair_config(replay_enabled=True))

    compare_summary = _write_compare_artifacts(compare_root)
    init_manifest = manifests_dir / "shadow-paper-init.json"
    status_manifest = manifests_dir / "shadow-paper-status.json"
    apply_manifest = manifests_dir / "shadow-paper-apply.json"
    apply_receipt = manifests_dir / "shadow-paper-receipt.json"
    for path in (init_manifest, status_manifest, apply_manifest, apply_receipt):
        path.write_text("{}\n", encoding="utf-8")

    signal_payload = {
        "action": "ENTER",
        "date": "2026-04-07",
        "event_id": "evt-shadow-1",
        "next_rebalance": "2026-05-01",
        "price": 500.0,
        "strategy": stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID,
        "symbol": "SPY",
        "target_shares": 150,
    }
    seen_commands: list[list[str]] = []

    def fake_run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
        seen_commands.append(cmd)
        script_name = Path(cmd[1]).name
        if script_name == "update_data_eod.py":
            return subprocess.CompletedProcess(
                cmd,
                0,
                stdout="",
                stderr="[update_data_eod] updated_symbols=5\n",
            )
        if script_name == "stage2_shadow_compare.py":
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(compare_summary), stderr="")
        if script_name == "paper_lane.py" and "init" in cmd:
            payload = {
                "archive_manifest_path": str(init_manifest),
                "paths": {
                    "state_path": str(paper_base_dir / "paper_state.json"),
                    "ledger_path": str(paper_base_dir / "paper_ledger.jsonl"),
                },
            }
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload), stderr="")
        if script_name == "paper_lane.py" and "status" in cmd:
            payload = {
                "archive_manifest_path": str(status_manifest),
                "drift_present": False,
                "event_already_applied": False,
                "paths": {
                    "state_path": str(paper_base_dir / "paper_state.json"),
                    "ledger_path": str(paper_base_dir / "paper_ledger.jsonl"),
                },
                "signal": signal_payload,
            }
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload), stderr="")
        if script_name == "paper_lane.py" and "apply" in cmd:
            payload = {
                "archive_manifest_path": str(apply_manifest),
                "duplicate_event_blocked": False,
                "event_receipt_path": str(apply_receipt),
                "result": "applied",
                "signal": signal_payload,
                "paths": {
                    "state_path": str(paper_base_dir / "paper_state.json"),
                    "ledger_path": str(paper_base_dir / "paper_ledger.jsonl"),
                },
            }
            return subprocess.CompletedProcess(cmd, 0, stdout=json.dumps(payload), stderr="")
        raise AssertionError(f"unexpected command: {cmd}")

    monkeypatch.setattr(stage2_shadow_daily_ops, "_run_process", fake_run_process)

    rc = stage2_shadow_daily_ops.main(
        [
            "--shadow-ops-config",
            str(config_path),
            "--archive-root",
            str(archive_root),
            "--timestamp",
            "2026-04-08T16:10:00-05:00",
            "--emit",
            "json",
        ]
    )
    captured = capsys.readouterr()

    assert rc == 0, captured.err
    payload = json.loads(captured.out.strip())
    summary = payload["summary"]
    assert summary["overall_result"] == "ok"
    assert summary["pair_id"] == stage2_shadow_daily_ops.SUPPORTED_PAIR_ID
    assert summary["compare_current_decision"] == "remain shadow-only"
    assert summary["compare_shadow_automation_decision"] == "allow"
    assert summary["local_replay_enabled"] is True
    assert summary["local_replay_auto_initialized"] is True
    assert summary["replay_apply_result"] == "applied"

    ops_paths = stage2_shadow_daily_ops.resolve_ops_paths(
        scope_key=stage2_shadow_daily_ops.SUPPORTED_PAIR_ID,
        archive_root=archive_root,
        create=False,
    )
    rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
    assert len(rows) == 1
    assert rows[0]["compare_shadow_action"] == "ENTER"
    assert rows[0]["replay_status_event_id"] == "evt-shadow-1"

    signal_json_path = Path(summary["compare_shadow_signal_json"])
    assert signal_json_path.exists()
    signal_json = json.loads(signal_json_path.read_text(encoding="utf-8"))
    assert signal_json["strategy"] == stage2_shadow_daily_ops.PRIMARY_LIVE_CANDIDATE_V1_VOL_MANAGED_ID
    assert signal_json["symbol"] == "SPY"

    command_names = [Path(cmd[1]).name for cmd in seen_commands]
    assert command_names == [
        "update_data_eod.py",
        "stage2_shadow_compare.py",
        "paper_lane.py",
        "paper_lane.py",
        "paper_lane.py",
    ]
    init_cmd = seen_commands[2]
    status_cmd = seen_commands[3]
    apply_cmd = seen_commands[4]
    assert "--starting-cash" in init_cmd
    assert "--base-dir" in init_cmd
    assert "--signal-json-file" in status_cmd
    assert "--signal-json-file" in apply_cmd
    assert str(signal_json_path) in status_cmd
    assert str(signal_json_path) in apply_cmd
    assert str(paper_base_dir) in init_cmd
    assert str(paper_base_dir) in status_cmd
    assert str(paper_base_dir) in apply_cmd


def test_main_refuses_overlapping_run_before_rewriting_logs(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    if stage2_shadow_daily_ops.fcntl is None:
        pytest.skip("stage2_shadow_daily_ops locking requires POSIX fcntl")

    config_path = tmp_path / "shadow_ops.json"
    archive_root = tmp_path / "archive"
    _write_config(config_path, active_pair=None)

    ops_paths = stage2_shadow_daily_ops.resolve_ops_paths(
        scope_key=stage2_shadow_daily_ops.UNCONFIGURED_SCOPE_KEY,
        archive_root=archive_root,
        create=True,
    )
    existing_row = _summary_row(ops_paths=ops_paths)
    paper_lane_daily_ops._append_jsonl_record(ops_paths["jsonl_path"], existing_row)
    stage2_shadow_daily_ops._write_csv(ops_paths["csv_path"], rows=[existing_row])
    stage2_shadow_daily_ops._write_xlsx(
        ops_paths["xlsx_path"],
        rows=[existing_row],
        timestamp=paper_lane_daily_ops._resolve_timestamp("2026-04-08T16:10:00-05:00"),
    )
    csv_before = ops_paths["csv_path"].read_bytes()
    xlsx_before = ops_paths["xlsx_path"].read_bytes()

    lock_holder = subprocess.Popen(
        [
            sys.executable,
            "-c",
            (
                "import fcntl, os, sys\n"
                "from pathlib import Path\n"
                "path = Path(sys.argv[1])\n"
                "path.parent.mkdir(parents=True, exist_ok=True)\n"
                "with path.open('a+', encoding='utf-8') as fh:\n"
                "    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)\n"
                "    fh.seek(0)\n"
                "    fh.truncate(0)\n"
                "    fh.write('pid=999 scope_key=unconfigured acquired_at_chicago=2026-04-08T16:10:00-05:00\\n')\n"
                "    fh.flush()\n"
                "    os.fsync(fh.fileno())\n"
                "    print('locked', flush=True)\n"
                "    sys.stdin.read()\n"
            ),
            str(ops_paths["lock_path"]),
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert lock_holder.stdout is not None
        assert lock_holder.stdin is not None
        locked_line = lock_holder.stdout.readline().strip()
        if locked_line != "locked":
            stderr = "" if lock_holder.stderr is None else lock_holder.stderr.read()
            raise AssertionError(f"lock holder failed to start: stdout={locked_line!r} stderr={stderr!r}")

        def fail_run_process(cmd: list[str], *, repo_root: Path) -> subprocess.CompletedProcess[str]:
            raise AssertionError(f"shadow ops steps should not start while lock is held: {cmd}")

        monkeypatch.setattr(stage2_shadow_daily_ops, "_run_process", fail_run_process)

        rc = stage2_shadow_daily_ops.main(
            [
                "--shadow-ops-config",
                str(config_path),
                "--archive-root",
                str(archive_root),
                "--timestamp",
                "2026-04-08T16:15:00-05:00",
            ]
        )
        captured = capsys.readouterr()

        assert rc == 2
        assert "already active" in captured.err
        assert "lock_path=" in captured.err

        rows = paper_lane_daily_ops._load_jsonl_records(ops_paths["jsonl_path"])
        assert len(rows) == 1
        assert rows[0]["run_id"] == "existing-run"
        assert ops_paths["csv_path"].read_bytes() == csv_before
        assert ops_paths["xlsx_path"].read_bytes() == xlsx_before
    finally:
        if lock_holder.stdin is not None:
            lock_holder.stdin.close()
        lock_holder.wait(timeout=5)
