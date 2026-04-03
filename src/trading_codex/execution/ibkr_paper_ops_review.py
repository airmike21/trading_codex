from __future__ import annotations

import csv
import json
import os
import re
import zipfile
from pathlib import Path
from typing import Any

from trading_codex.execution.ibkr_paper_lane import DEFAULT_IBKR_PAPER_STATE_KEY
from trading_codex.run_archive import resolve_archive_root

REVIEW_SCHEMA_NAME = "ibkr_paper_ops_review"
REVIEW_SCHEMA_VERSION = 1
DEFAULT_REVIEW_LIMIT = 20
REVIEW_CHECKPOINT_MARKET_DAYS = 20

_PATH_CHECK_FIELDS = (
    "daily_ops_manifest",
    "status_archive_manifest",
    "apply_archive_manifest",
    "apply_event_claim",
    "apply_event_receipt",
)


def _expand_path(value: Path | str | None) -> Path | None:
    if value is None:
        return None
    return Path(os.path.expanduser(os.path.expandvars(str(value)))).resolve()


def _safe_slug(value: str, *, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return cleaned.strip("._-") or fallback


def resolve_ibkr_paper_ops_review_paths(
    *,
    state_key: str = DEFAULT_IBKR_PAPER_STATE_KEY,
    archive_root: Path | str | None = None,
    create: bool = False,
) -> dict[str, Path]:
    resolved_archive_root = resolve_archive_root(
        preferred_root=_expand_path(archive_root),
        create=create,
    )
    ops_root = resolved_archive_root / "stage2_ibkr_paper_ops" / _safe_slug(
        state_key,
        fallback=DEFAULT_IBKR_PAPER_STATE_KEY,
    )
    if create:
        ops_root.mkdir(parents=True, exist_ok=True)
    return {
        "archive_root": resolved_archive_root,
        "ops_root": ops_root,
        "jsonl_path": ops_root / "ibkr_paper_lane_daily_ops_log.jsonl",
        "csv_path": ops_root / "ibkr_paper_lane_daily_ops_runs.csv",
        "xlsx_path": ops_root / "ibkr_paper_lane_daily_ops_runs.xlsx",
    }


def _load_jsonl_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _load_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", errors="replace", newline="") as fh:
        return list(csv.DictReader(fh))


def _normalize_optional_string(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _to_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return value != 0
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"", "0", "false", "no", "off"}:
            return False
    return False


def _to_int(value: object, *, default: int = 0) -> int:
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return default
        try:
            return int(text)
        except ValueError:
            try:
                return int(float(text))
            except ValueError:
                return default
    return default


def _to_optional_int(value: object) -> int | None:
    if value is None or value == "":
        return None
    parsed = _to_int(value, default=0)
    if isinstance(value, str) and not value.strip():
        return None
    return parsed


def _is_success_exit_code(value: object) -> bool:
    return _normalize_optional_string(value) is not None and _to_int(value, default=1) == 0


def _resolve_path_value(value: object, *, anchor: Path) -> Path | None:
    text = _normalize_optional_string(value)
    if text is None:
        return None
    path = Path(os.path.expanduser(os.path.expandvars(text)))
    if path.is_absolute():
        return path.resolve()
    return (anchor / path).resolve()


def _build_path_check(
    value: object,
    *,
    expected: bool,
    anchor: Path,
) -> dict[str, Any]:
    path = _resolve_path_value(value, anchor=anchor)
    return {
        "expected": bool(expected),
        "exists": bool(path is not None and path.exists()),
        "path": None if path is None else str(path),
    }


def _empty_run_path_checks() -> dict[str, dict[str, Any]]:
    return {
        name: {
            "expected": False,
            "exists": False,
            "path": None,
        }
        for name in _PATH_CHECK_FIELDS
    }


def _row_has_pending_claim(row: dict[str, Any]) -> bool:
    pending_result = _normalize_optional_string(row.get("status_pending_claim_result"))
    apply_result = _normalize_optional_string(row.get("apply_result"))
    return any(
        [
            _to_bool(row.get("status_event_claim_pending")),
            _to_bool(row.get("apply_event_claim_pending")),
            bool(_normalize_optional_string(row.get("apply_event_claim_path"))),
            bool(pending_result and "claim_pending" in pending_result.lower()),
            bool(apply_result and "claim_pending" in apply_result.lower()),
        ]
    )


def _row_duplicate_blocked(row: dict[str, Any]) -> bool:
    apply_result = _normalize_optional_string(row.get("apply_result"))
    return _to_bool(row.get("apply_duplicate_event_blocked")) or apply_result == "duplicate_event_refused"


def _row_requires_event_receipt(row: dict[str, Any]) -> bool:
    apply_result = _normalize_optional_string(row.get("apply_result"))
    return apply_result == "applied" or bool(_normalize_optional_string(row.get("apply_event_receipt_path")))


def _build_run_path_checks(row: dict[str, Any] | None, *, anchor: Path) -> dict[str, dict[str, Any]]:
    if row is None:
        return _empty_run_path_checks()
    return {
        "daily_ops_manifest": _build_path_check(
            row.get("daily_ops_manifest_path"),
            expected=True,
            anchor=anchor,
        ),
        "status_archive_manifest": _build_path_check(
            row.get("status_archive_manifest_path"),
            expected=_is_success_exit_code(row.get("status_exit_code")),
            anchor=anchor,
        ),
        "apply_archive_manifest": _build_path_check(
            row.get("apply_archive_manifest_path"),
            expected=_is_success_exit_code(row.get("apply_exit_code")),
            anchor=anchor,
        ),
        "apply_event_claim": _build_path_check(
            row.get("apply_event_claim_path"),
            expected=_to_bool(row.get("apply_event_claim_pending")),
            anchor=anchor,
        ),
        "apply_event_receipt": _build_path_check(
            row.get("apply_event_receipt_path"),
            expected=_row_requires_event_receipt(row),
            anchor=anchor,
        ),
    }


def _summarize_inspected_path_checks(
    rows: list[dict[str, Any]],
    *,
    anchor: Path,
) -> dict[str, dict[str, int]]:
    summary = {
        name: {
            "expected_count": 0,
            "present_count": 0,
            "missing_count": 0,
        }
        for name in _PATH_CHECK_FIELDS
    }
    for row in rows:
        checks = _build_run_path_checks(row, anchor=anchor)
        for name, check in checks.items():
            if not check["expected"]:
                continue
            summary[name]["expected_count"] += 1
            if check["exists"]:
                summary[name]["present_count"] += 1
            else:
                summary[name]["missing_count"] += 1
    return summary


def _build_latest_signal(rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in reversed(rows):
        signal = {
            "date": _normalize_optional_string(row.get("status_signal_date")),
            "action": _normalize_optional_string(row.get("status_signal_action")),
            "symbol": _normalize_optional_string(row.get("status_signal_symbol")),
            "target_shares": _to_optional_int(row.get("status_target_shares")),
            "next_rebalance": _normalize_optional_string(row.get("status_next_rebalance")),
            "event_id": _normalize_optional_string(row.get("status_event_id")),
        }
        if any(value is not None for value in signal.values()):
            return signal
    return {
        "date": None,
        "action": None,
        "symbol": None,
        "target_shares": None,
        "next_rebalance": None,
        "event_id": None,
    }


def build_ibkr_paper_ops_review(
    *,
    archive_root: Path | str | None = None,
    state_key: str = DEFAULT_IBKR_PAPER_STATE_KEY,
    limit: int = DEFAULT_REVIEW_LIMIT,
) -> dict[str, Any]:
    if int(limit) <= 0:
        raise ValueError("--limit must be >= 1.")

    paths = resolve_ibkr_paper_ops_review_paths(
        state_key=state_key,
        archive_root=archive_root,
        create=False,
    )
    jsonl_exists = paths["jsonl_path"].exists()
    csv_exists = paths["csv_path"].exists()
    xlsx_exists = paths["xlsx_path"].exists()
    xlsx_is_zip = xlsx_exists and zipfile.is_zipfile(paths["xlsx_path"])

    all_rows = _load_jsonl_records(paths["jsonl_path"])
    inspected_rows = all_rows[-int(limit) :]
    latest_row = all_rows[-1] if all_rows else None
    latest_signal = _build_latest_signal(inspected_rows)
    csv_rows = _load_csv_rows(paths["csv_path"]) if csv_exists else []

    ok_count = sum(
        1
        for row in inspected_rows
        if (_normalize_optional_string(row.get("overall_result")) or "").lower() == "ok"
    )
    failed_count = sum(
        1
        for row in inspected_rows
        if (_normalize_optional_string(row.get("overall_result")) or "").lower() == "failed"
    )
    claim_pending_count = sum(1 for row in inspected_rows if _row_has_pending_claim(row))
    duplicate_blocked_count = sum(1 for row in inspected_rows if _row_duplicate_blocked(row))
    submitted_order_count_total = sum(_to_int(row.get("apply_submitted_order_count")) for row in inspected_rows)
    latest_successful_signal_days_recorded = _to_int(
        None if latest_row is None else latest_row.get("successful_signal_days_recorded")
    )

    latest_path_checks = _build_run_path_checks(latest_row, anchor=paths["archive_root"])
    inspected_path_checks = _summarize_inspected_path_checks(
        inspected_rows,
        anchor=paths["archive_root"],
    )

    jsonl_csv_row_count_match: bool | None = None
    if jsonl_exists and csv_exists:
        jsonl_csv_row_count_match = len(all_rows) == len(csv_rows)

    attention_flags: list[str] = []
    if not jsonl_exists:
        attention_flags.append("missing_cumulative_jsonl")
    if not csv_exists:
        attention_flags.append("missing_cumulative_csv")
    if not xlsx_exists:
        attention_flags.append("missing_cumulative_xlsx")
    if xlsx_exists and not xlsx_is_zip:
        attention_flags.append("invalid_cumulative_xlsx")
    if not all_rows:
        attention_flags.append("zero_runs_found")
    if jsonl_csv_row_count_match is False:
        attention_flags.append("cumulative_row_count_mismatch")
    if latest_row is not None and _normalize_optional_string(latest_row.get("overall_result")) == "failed":
        attention_flags.append("latest_run_failed")
    if claim_pending_count > 0:
        attention_flags.append("pending_claims_present")
    if duplicate_blocked_count > 0:
        attention_flags.append("duplicate_event_blocks_present")

    missing_flag_names = {
        "daily_ops_manifest": "missing_daily_ops_manifest_path",
        "status_archive_manifest": "missing_status_archive_manifest_path",
        "apply_archive_manifest": "missing_apply_archive_manifest_path",
        "apply_event_claim": "missing_apply_event_claim_path",
        "apply_event_receipt": "missing_apply_event_receipt_path",
    }
    for field_name, flag_name in missing_flag_names.items():
        if inspected_path_checks[field_name]["missing_count"] > 0:
            attention_flags.append(flag_name)

    return {
        "schema_name": REVIEW_SCHEMA_NAME,
        "schema_version": REVIEW_SCHEMA_VERSION,
        "state_key": state_key,
        "archive_root": str(paths["archive_root"]),
        "ops_root": str(paths["ops_root"]),
        "lookback_limit": int(limit),
        "total_runs_available": len(all_rows),
        "total_runs_inspected": len(inspected_rows),
        "ok_count": ok_count,
        "failed_count": failed_count,
        "latest_run_timestamp": _normalize_optional_string(None if latest_row is None else latest_row.get("timestamp_chicago")),
        "latest_overall_result": _normalize_optional_string(None if latest_row is None else latest_row.get("overall_result")),
        "latest_failed_step": _normalize_optional_string(None if latest_row is None else latest_row.get("failed_step")),
        "latest_signal": latest_signal,
        "claim_pending_count": claim_pending_count,
        "duplicate_blocked_count": duplicate_blocked_count,
        "submitted_order_count_total": submitted_order_count_total,
        "latest_successful_signal_days_recorded": latest_successful_signal_days_recorded,
        "review_checkpoint": {
            "market_day_target": REVIEW_CHECKPOINT_MARKET_DAYS,
            "reached": latest_successful_signal_days_recorded >= REVIEW_CHECKPOINT_MARKET_DAYS,
        },
        "cumulative_artifacts": {
            "jsonl_path": str(paths["jsonl_path"]),
            "jsonl_exists": jsonl_exists,
            "jsonl_row_count": len(all_rows),
            "csv_path": str(paths["csv_path"]),
            "csv_exists": csv_exists,
            "csv_row_count": len(csv_rows),
            "jsonl_csv_row_count_match": jsonl_csv_row_count_match,
            "xlsx_path": str(paths["xlsx_path"]),
            "xlsx_exists": xlsx_exists,
            "xlsx_is_zip": xlsx_is_zip,
        },
        "path_checks": {
            "latest_run": latest_path_checks,
            "inspected_runs": inspected_path_checks,
        },
        "attention_flags": attention_flags,
        "review_status": "ok" if not attention_flags else "attention_required",
    }


def _display_value(value: object) -> str:
    if value is None or value == "":
        return "-"
    return str(value)


def _display_expected_path_state(check: dict[str, Any]) -> str:
    if not check.get("expected"):
        return "n/a"
    return "yes" if check.get("exists") else "no"


def render_ibkr_paper_ops_review_text(review: dict[str, Any]) -> str:
    latest_signal = review.get("latest_signal") if isinstance(review.get("latest_signal"), dict) else {}
    latest_run_checks = review.get("path_checks", {}).get("latest_run", {})
    inspected_path_checks = review.get("path_checks", {}).get("inspected_runs", {})
    cumulative_artifacts = review.get("cumulative_artifacts", {})
    latest_signal_text = (
        "Latest signal: "
        f"{_display_value(latest_signal.get('date'))} "
        f"{_display_value(latest_signal.get('action'))} "
        f"{_display_value(latest_signal.get('symbol'))} "
        f"target={_display_value(latest_signal.get('target_shares'))} "
        f"next={_display_value(latest_signal.get('next_rebalance'))} "
        f"event_id={_display_value(latest_signal.get('event_id'))}"
    )
    lines = [
        f"IBKR paper ops review {review['state_key']}",
        f"Review status: {review['review_status']}",
        f"Archive root: {review['archive_root']}",
        f"Ops root: {review['ops_root']}",
        (
            "Runs: "
            f"inspected {review['total_runs_inspected']} of {review['total_runs_available']} "
            f"| ok {review['ok_count']} | failed {review['failed_count']}"
        ),
        (
            "Latest run: "
            f"{_display_value(review.get('latest_run_timestamp'))} "
            f"| result={_display_value(review.get('latest_overall_result'))} "
            f"| failed_step={_display_value(review.get('latest_failed_step'))}"
        ),
        latest_signal_text,
        (
            "Operational counts: "
            f"pending_claims={review['claim_pending_count']} "
            f"| duplicate_blocked={review['duplicate_blocked_count']} "
            f"| submitted_orders={review['submitted_order_count_total']}"
        ),
        (
            "Checkpoint: "
            f"successful_signal_days={review['latest_successful_signal_days_recorded']} "
            f"| target={review['review_checkpoint']['market_day_target']} "
            f"| reached={'yes' if review['review_checkpoint']['reached'] else 'no'}"
        ),
        (
            "Artifacts: "
            f"jsonl={'yes' if cumulative_artifacts.get('jsonl_exists') else 'no'} "
            f"rows={_display_value(cumulative_artifacts.get('jsonl_row_count'))} "
            f"| csv={'yes' if cumulative_artifacts.get('csv_exists') else 'no'} "
            f"rows={_display_value(cumulative_artifacts.get('csv_row_count'))} "
            f"match={_display_value(cumulative_artifacts.get('jsonl_csv_row_count_match'))} "
            f"| xlsx={'yes' if cumulative_artifacts.get('xlsx_exists') else 'no'} "
            f"zip={_display_value(cumulative_artifacts.get('xlsx_is_zip'))}"
        ),
        (
            "Latest paths: "
            f"daily_ops={_display_expected_path_state(latest_run_checks.get('daily_ops_manifest', {}))} "
            f"| status={_display_expected_path_state(latest_run_checks.get('status_archive_manifest', {}))} "
            f"| apply={_display_expected_path_state(latest_run_checks.get('apply_archive_manifest', {}))} "
            f"| claim={_display_expected_path_state(latest_run_checks.get('apply_event_claim', {}))} "
            f"| receipt={_display_expected_path_state(latest_run_checks.get('apply_event_receipt', {}))}"
        ),
        (
            "Inspected path gaps: "
            f"daily_ops={_display_value(inspected_path_checks.get('daily_ops_manifest', {}).get('missing_count'))} "
            f"| status={_display_value(inspected_path_checks.get('status_archive_manifest', {}).get('missing_count'))} "
            f"| apply={_display_value(inspected_path_checks.get('apply_archive_manifest', {}).get('missing_count'))} "
            f"| claim={_display_value(inspected_path_checks.get('apply_event_claim', {}).get('missing_count'))} "
            f"| receipt={_display_value(inspected_path_checks.get('apply_event_receipt', {}).get('missing_count'))}"
        ),
        (
            "Attention flags: "
            + ", ".join(review["attention_flags"])
            if review.get("attention_flags")
            else "Attention flags: none"
        ),
    ]
    return "\n".join(lines)
