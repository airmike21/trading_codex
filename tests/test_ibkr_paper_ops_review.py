from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from scripts import ibkr_paper_lane_daily_ops, paper_lane_daily_ops
from trading_codex.execution.ibkr_paper_lane import DEFAULT_IBKR_PAPER_STATE_KEY
from trading_codex.execution.ibkr_paper_ops_review import (
    REVIEW_SCHEMA_NAME,
    build_ibkr_paper_ops_review,
    render_ibkr_paper_ops_review_text,
    resolve_ibkr_paper_ops_review_paths,
)


def _write_text(path: Path, content: str = "ok\n") -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return str(path)


def _summary_row(
    *,
    run_id: str,
    timestamp_chicago: str,
    event_id: str,
    overall_result: str = "ok",
    failed_step: str = "",
    signal_date: str = "2026-04-01",
    action: str = "ENTER",
    symbol: str = "EFA",
    target_shares: int = 100,
    next_rebalance: str = "2026-04-24",
    status_exit_code: int | str = 0,
    apply_exit_code: int | str = 0,
    apply_result: str = "applied",
    apply_duplicate_event_blocked: bool = False,
    status_event_claim_pending: bool = False,
    status_pending_claim_result: str = "",
    apply_event_claim_pending: bool = False,
    apply_event_claim_path: str = "",
    apply_event_receipt_path: str = "",
    apply_submitted_order_count: int = 0,
    daily_ops_manifest_path: str = "",
    status_archive_manifest_path: str = "",
    apply_archive_manifest_path: str = "",
    successful_signal_days_recorded: int = 0,
) -> dict[str, object]:
    row = {column: "" for column in ibkr_paper_lane_daily_ops.RUN_LOG_COLUMNS}
    row.update(
        {
            "schema_name": ibkr_paper_lane_daily_ops.SUMMARY_SCHEMA_NAME,
            "schema_version": ibkr_paper_lane_daily_ops.SUMMARY_SCHEMA_VERSION,
            "run_id": run_id,
            "timestamp_chicago": timestamp_chicago,
            "ops_date": signal_date,
            "overall_result": overall_result,
            "failed_step": failed_step,
            "preset": "dual_mom_vol10_cash_core",
            "state_key": DEFAULT_IBKR_PAPER_STATE_KEY,
            "provider": "stooq",
            "status_exit_code": status_exit_code,
            "status_signal_date": signal_date,
            "status_signal_action": action,
            "status_signal_symbol": symbol,
            "status_target_shares": target_shares,
            "status_next_rebalance": next_rebalance,
            "status_event_id": event_id,
            "status_event_claim_pending": status_event_claim_pending,
            "status_pending_claim_result": status_pending_claim_result,
            "apply_exit_code": apply_exit_code,
            "apply_result": apply_result,
            "apply_duplicate_event_blocked": apply_duplicate_event_blocked,
            "apply_event_claim_pending": apply_event_claim_pending,
            "apply_event_claim_path": apply_event_claim_path,
            "apply_event_receipt_path": apply_event_receipt_path,
            "apply_submitted_order_count": apply_submitted_order_count,
            "daily_ops_manifest_path": daily_ops_manifest_path,
            "status_archive_manifest_path": status_archive_manifest_path,
            "apply_archive_manifest_path": apply_archive_manifest_path,
            "successful_signal_days_recorded": successful_signal_days_recorded,
        }
    )
    return row


def _write_ops_history(
    archive_root: Path,
    rows: list[dict[str, object]],
    *,
    csv_rows: list[dict[str, object]] | None = None,
    create_csv: bool = True,
    create_xlsx: bool = True,
) -> dict[str, Path]:
    ops_paths = resolve_ibkr_paper_ops_review_paths(
        state_key=DEFAULT_IBKR_PAPER_STATE_KEY,
        archive_root=archive_root,
        create=True,
    )
    for row in rows:
        paper_lane_daily_ops._append_jsonl_record(ops_paths["jsonl_path"], row)
    if create_csv:
        ibkr_paper_lane_daily_ops._write_csv(ops_paths["csv_path"], rows=rows if csv_rows is None else csv_rows)
    if create_xlsx:
        timestamp = paper_lane_daily_ops._resolve_timestamp(
            str(rows[-1]["timestamp_chicago"]) if rows else "2026-04-01T16:10:00-05:00"
        )
        ibkr_paper_lane_daily_ops._write_xlsx(ops_paths["xlsx_path"], rows=rows, timestamp=timestamp)
    return ops_paths


def test_build_review_flags_no_runs_and_missing_artifacts(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"

    payload = build_ibkr_paper_ops_review(archive_root=archive_root)

    assert payload["schema_name"] == REVIEW_SCHEMA_NAME
    assert payload["state_key"] == DEFAULT_IBKR_PAPER_STATE_KEY
    assert payload["total_runs_available"] == 0
    assert payload["total_runs_inspected"] == 0
    assert payload["ok_count"] == 0
    assert payload["failed_count"] == 0
    assert payload["latest_run_timestamp"] is None
    assert payload["review_checkpoint"]["reached"] is False
    assert payload["cumulative_artifacts"]["jsonl_exists"] is False
    assert payload["cumulative_artifacts"]["csv_exists"] is False
    assert payload["cumulative_artifacts"]["xlsx_exists"] is False
    assert payload["attention_flags"] == [
        "missing_cumulative_jsonl",
        "missing_cumulative_csv",
        "missing_cumulative_xlsx",
        "zero_runs_found",
    ]
    assert payload["review_status"] == "attention_required"

    text = render_ibkr_paper_ops_review_text(payload)
    assert "Runs: inspected 0 of 0 | ok 0 | failed 0" in text
    assert "Attention flags: missing_cumulative_jsonl, missing_cumulative_csv, missing_cumulative_xlsx, zero_runs_found" in text


def test_build_review_summarizes_mixed_history_and_operator_flags(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    manifests_dir = tmp_path / "manifests"
    receipts_dir = tmp_path / "receipts"
    claims_dir = tmp_path / "claims"

    row1 = _summary_row(
        run_id="run-1",
        timestamp_chicago="2026-03-31T16:10:00-05:00",
        signal_date="2026-03-31",
        event_id="evt-1",
        apply_submitted_order_count=1,
        daily_ops_manifest_path=_write_text(manifests_dir / "run-1-daily.json"),
        status_archive_manifest_path=_write_text(manifests_dir / "run-1-status.json"),
        apply_archive_manifest_path=_write_text(manifests_dir / "run-1-apply.json"),
        apply_event_receipt_path=_write_text(receipts_dir / "evt-1.json"),
        successful_signal_days_recorded=1,
    )
    row2 = _summary_row(
        run_id="run-2",
        timestamp_chicago="2026-04-01T16:10:00-05:00",
        signal_date="2026-04-01",
        event_id="evt-2",
        apply_result="duplicate_event_refused",
        apply_duplicate_event_blocked=True,
        daily_ops_manifest_path=_write_text(manifests_dir / "run-2-daily.json"),
        status_archive_manifest_path=_write_text(manifests_dir / "run-2-status.json"),
        apply_archive_manifest_path=str(manifests_dir / "run-2-apply-missing.json"),
        successful_signal_days_recorded=2,
    )
    row3 = _summary_row(
        run_id="run-3",
        timestamp_chicago="2026-04-02T16:10:00-05:00",
        signal_date="2026-04-02",
        event_id="evt-3",
        overall_result="failed",
        failed_step="ibkr_paper_lane_apply",
        apply_exit_code=2,
        apply_result="claim_pending_manual_clearance_required",
        apply_event_claim_pending=True,
        apply_event_claim_path=_write_text(claims_dir / "evt-3.json"),
        apply_submitted_order_count=1,
        daily_ops_manifest_path=_write_text(manifests_dir / "run-3-daily.json"),
        status_archive_manifest_path=_write_text(manifests_dir / "run-3-status.json"),
        successful_signal_days_recorded=3,
    )
    _write_ops_history(archive_root, [row1, row2, row3])

    payload = build_ibkr_paper_ops_review(archive_root=archive_root, limit=10)

    assert payload["total_runs_available"] == 3
    assert payload["total_runs_inspected"] == 3
    assert payload["ok_count"] == 2
    assert payload["failed_count"] == 1
    assert payload["latest_run_timestamp"] == "2026-04-02T16:10:00-05:00"
    assert payload["latest_overall_result"] == "failed"
    assert payload["latest_failed_step"] == "ibkr_paper_lane_apply"
    assert payload["latest_signal"] == {
        "date": "2026-04-02",
        "action": "ENTER",
        "symbol": "EFA",
        "target_shares": 100,
        "next_rebalance": "2026-04-24",
        "event_id": "evt-3",
    }
    assert payload["claim_pending_count"] == 1
    assert payload["duplicate_blocked_count"] == 1
    assert payload["submitted_order_count_total"] == 2
    assert payload["latest_successful_signal_days_recorded"] == 3
    assert payload["review_checkpoint"]["reached"] is False
    assert payload["cumulative_artifacts"]["jsonl_exists"] is True
    assert payload["cumulative_artifacts"]["csv_exists"] is True
    assert payload["cumulative_artifacts"]["xlsx_exists"] is True
    assert payload["cumulative_artifacts"]["jsonl_csv_row_count_match"] is True
    assert payload["path_checks"]["latest_run"]["daily_ops_manifest"]["exists"] is True
    assert payload["path_checks"]["latest_run"]["status_archive_manifest"]["exists"] is True
    assert payload["path_checks"]["latest_run"]["apply_archive_manifest"]["expected"] is False
    assert payload["path_checks"]["latest_run"]["apply_event_claim"]["exists"] is True
    assert payload["path_checks"]["inspected_runs"]["apply_archive_manifest"]["missing_count"] == 1
    assert payload["attention_flags"] == [
        "latest_run_failed",
        "pending_claims_present",
        "duplicate_event_blocks_present",
        "missing_apply_archive_manifest_path",
    ]
    assert payload["review_status"] == "attention_required"


def test_build_review_flags_row_count_mismatch_and_checkpoint_reached(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    manifests_dir = tmp_path / "manifests"
    receipts_dir = tmp_path / "receipts"

    row1 = _summary_row(
        run_id="run-1",
        timestamp_chicago="2026-04-01T16:10:00-05:00",
        signal_date="2026-04-01",
        event_id="evt-a",
        apply_submitted_order_count=1,
        daily_ops_manifest_path=_write_text(manifests_dir / "run-1-daily.json"),
        status_archive_manifest_path=_write_text(manifests_dir / "run-1-status.json"),
        apply_archive_manifest_path=_write_text(manifests_dir / "run-1-apply.json"),
        apply_event_receipt_path=_write_text(receipts_dir / "evt-a.json"),
        successful_signal_days_recorded=19,
    )
    row2 = _summary_row(
        run_id="run-2",
        timestamp_chicago="2026-04-02T16:10:00-05:00",
        signal_date="2026-04-02",
        event_id="evt-b",
        apply_submitted_order_count=1,
        daily_ops_manifest_path=_write_text(manifests_dir / "run-2-daily.json"),
        status_archive_manifest_path=_write_text(manifests_dir / "run-2-status.json"),
        apply_archive_manifest_path=_write_text(manifests_dir / "run-2-apply.json"),
        apply_event_receipt_path=_write_text(receipts_dir / "evt-b.json"),
        successful_signal_days_recorded=20,
    )
    _write_ops_history(archive_root, [row1, row2], csv_rows=[row1])

    payload = build_ibkr_paper_ops_review(archive_root=archive_root, limit=2)

    assert payload["total_runs_available"] == 2
    assert payload["total_runs_inspected"] == 2
    assert payload["review_checkpoint"]["reached"] is True
    assert payload["latest_successful_signal_days_recorded"] == 20
    assert payload["cumulative_artifacts"]["jsonl_row_count"] == 2
    assert payload["cumulative_artifacts"]["csv_row_count"] == 1
    assert payload["cumulative_artifacts"]["jsonl_csv_row_count_match"] is False
    assert payload["attention_flags"] == ["cumulative_row_count_mismatch"]
    assert payload["review_status"] == "attention_required"


def test_ibkr_paper_ops_review_cli_smoke(tmp_path: Path) -> None:
    archive_root = tmp_path / "archive"
    manifests_dir = tmp_path / "manifests"
    receipts_dir = tmp_path / "receipts"
    row = _summary_row(
        run_id="run-smoke",
        timestamp_chicago="2026-04-03T16:10:00-05:00",
        signal_date="2026-04-03",
        event_id="evt-smoke",
        apply_submitted_order_count=1,
        daily_ops_manifest_path=_write_text(manifests_dir / "run-smoke-daily.json"),
        status_archive_manifest_path=_write_text(manifests_dir / "run-smoke-status.json"),
        apply_archive_manifest_path=_write_text(manifests_dir / "run-smoke-apply.json"),
        apply_event_receipt_path=_write_text(receipts_dir / "evt-smoke.json"),
        successful_signal_days_recorded=4,
    )
    _write_ops_history(archive_root, [row])

    repo_root = Path(__file__).resolve().parents[1]
    script_path = repo_root / "scripts" / "ibkr_paper_ops_review.py"

    proc = subprocess.run(
        [
            sys.executable,
            str(script_path),
            "--emit",
            "json",
            "--archive-root",
            str(archive_root),
            "--limit",
            "1",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
    )

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["schema_name"] == REVIEW_SCHEMA_NAME
    assert payload["state_key"] == DEFAULT_IBKR_PAPER_STATE_KEY
    assert payload["total_runs_inspected"] == 1
    assert payload["latest_signal"]["event_id"] == "evt-smoke"
    assert payload["review_status"] == "ok"
