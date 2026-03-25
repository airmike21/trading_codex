from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import live_canary_state_ops
from trading_codex.execution.live_canary import (
    live_canary_event_state_path,
    live_canary_session_state_path,
)


ACCOUNT_ID = "5WT00001"
AS_OF = "2026-03-23T10:50:00-04:00"
TIMESTAMP = "2026-03-23T10:55:00-04:00"
LAUNCH_TIMESTAMP = "2026-03-23T10:45:00-04:00"
SIGNAL_DATE = "2026-03-20"
STRATEGY = "dual_mom_vol10_cash"
FINGERPRINT = "live-fingerprint-reconcile-001"
ORDER_ID = "order-123"


def _event_id(payload: dict[str, object]) -> str:
    def s(value: object) -> str:
        return "" if value is None else str(value)

    return ":".join(
        [
            s(payload.get("date")),
            s(payload.get("strategy")),
            s(payload.get("action")),
            s(payload.get("symbol")),
            s(payload.get("target_shares")),
            s(payload.get("resize_new_shares")),
            s(payload.get("next_rebalance")),
        ]
    )


def _signal_payload() -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "date": SIGNAL_DATE,
        "strategy": STRATEGY,
        "action": "RESIZE",
        "symbol": "EFA",
        "price": 99.16,
        "target_shares": 100,
        "resize_prev_shares": 0,
        "resize_new_shares": 100,
        "next_rebalance": "2026-03-31",
    }
    payload["event_id"] = _event_id(payload)
    return payload


def _launch_order() -> dict[str, object]:
    return {
        "symbol": "EFA",
        "side": "BUY",
        "requested_qty": 1,
        "current_broker_shares": 0,
        "desired_canary_shares": 1,
    }


def _broker_snapshot(
    *,
    account_id: str = ACCOUNT_ID,
    as_of: str = AS_OF,
    shares: int = 1,
) -> dict[str, object]:
    return {
        "broker_name": "tastytrade",
        "account_id": account_id,
        "as_of": as_of,
        "buying_power": 20_000.0,
        "positions": [
            {
                "symbol": "EFA",
                "shares": shares,
                "price": 99.16,
                "instrument_type": "Equity",
            }
        ],
    }


def _orders_payload(
    *,
    account_id: str = ACCOUNT_ID,
    order_id: str = ORDER_ID,
    status: str = "filled",
    filled_quantity: int | None = 1,
    remaining_quantity: int | None = 0,
) -> dict[str, object]:
    order: dict[str, object] = {
        "account_id": account_id,
        "order_id": order_id,
        "status": status,
    }
    if filled_quantity is not None:
        order["filled_quantity"] = filled_quantity
    if remaining_quantity is not None:
        order["remaining_quantity"] = remaining_quantity
    return {"account_id": account_id, "orders": [order]}


def _write_json(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _write_jsonl(path: Path, records: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(record, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    return path


def _write_launch_fixture(
    tmp_path: Path,
    *,
    requested_live_submit: bool = True,
    submit_path_invoked: bool = True,
    submit_decision: str = "live_submitted",
    live_submit_attempted: bool = True,
    submission_result: str = "submitted",
    submission_succeeded: bool = True,
    manual_clearance_required: bool = False,
    include_broker_order_id: bool = True,
    include_event_state: bool = True,
    include_session_state: bool = True,
    include_ledger: bool = True,
    include_claim: bool = False,
) -> tuple[Path, Path]:
    base_dir = tmp_path / "live_canary"
    signal = _signal_payload()
    launch_order = _launch_order()
    launch_result_path = base_dir / "launches" / SIGNAL_DATE / "launch.json"
    event_path = live_canary_event_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        event_id=str(signal["event_id"]),
    )
    session_path = live_canary_session_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        strategy=STRATEGY,
        signal_date=SIGNAL_DATE,
    )
    ledger_path = base_dir / "broker_live_submission_fingerprints.jsonl"
    claim_path = base_dir / "claims" / f"{FINGERPRINT}.json"

    live_submission = None
    if submit_path_invoked and (live_submit_attempted or submit_decision == "live_submit_refused"):
        live_submission = {
            "live_submit_attempted": live_submit_attempted,
            "manual_clearance_required": manual_clearance_required,
            "orders": [
                {
                    "attempted": live_submit_attempted,
                    "broker_order_id": ORDER_ID if include_broker_order_id else None,
                    "broker_status": "filled" if live_submit_attempted else None,
                    "error": None,
                    "quantity": 1,
                    "side": "BUY",
                    "succeeded": submission_succeeded,
                    "symbol": "EFA",
                }
            ],
            "refusal_reasons": [],
            "submission_result": submission_result,
            "submission_succeeded": submission_succeeded,
            "live_submission_fingerprint": FINGERPRINT,
        }

    if include_event_state and submit_path_invoked:
        event_record: dict[str, object] = {
            "account_id": ACCOUNT_ID,
            "decision": submit_decision,
            "event_id": signal["event_id"],
            "generated_at_chicago": LAUNCH_TIMESTAMP,
            "manual_clearance_required": manual_clearance_required,
            "response_text": "submitted" if submit_decision == "live_submitted" else submit_decision,
            "result": submission_result,
        }
        if live_submission is not None:
            event_record["live_submission"] = live_submission
        _write_json(event_path, event_record)

    if include_session_state and submit_path_invoked:
        session_record: dict[str, object] = {
            "account_id": ACCOUNT_ID,
            "claimed_at_chicago": LAUNCH_TIMESTAMP,
            "decision": submit_decision,
            "event_id": signal["event_id"],
            "generated_at_chicago": LAUNCH_TIMESTAMP,
            "manual_clearance_required": manual_clearance_required,
            "response_text": "submitted" if submit_decision == "live_submitted" else submit_decision,
            "result": submission_result,
            "signal_date": SIGNAL_DATE,
            "strategy": STRATEGY,
            "updated_at_chicago": LAUNCH_TIMESTAMP,
        }
        if live_submission is not None:
            session_record["live_submission"] = live_submission
        _write_json(session_path, session_record)

    if include_ledger and live_submission is not None:
        _write_jsonl(
            ledger_path,
            [
                {
                    "account_id": ACCOUNT_ID,
                    "artifact_path": None,
                    "event_id": signal["event_id"],
                    "generated_at_chicago": LAUNCH_TIMESTAMP,
                    "live_submission_fingerprint": FINGERPRINT,
                    "manual_clearance_required": manual_clearance_required,
                    "plan_sha256": "plan-sha",
                    "result": submission_result,
                    "signal_date": SIGNAL_DATE,
                    "strategy": STRATEGY,
                    "submission_succeeded": submission_succeeded,
                }
            ],
        )

    if include_claim:
        _write_json(
            claim_path,
            {
                "account_id": ACCOUNT_ID,
                "claim_path": str(claim_path),
                "event_id": signal["event_id"],
                "generated_at_chicago": LAUNCH_TIMESTAMP,
                "live_submission_fingerprint": FINGERPRINT,
                "manual_clearance_required": True,
                "plan_sha256": "plan-sha",
                "result": "claim_pending_manual_clearance_required",
                "signal_date": SIGNAL_DATE,
                "strategy": STRATEGY,
            },
        )

    submit_result = None
    if submit_path_invoked:
        submit_result = {
            "schema_name": "live_canary_guardrail_result",
            "schema_version": 1,
            "timestamp_chicago": LAUNCH_TIMESTAMP,
            "account_id": ACCOUNT_ID,
            "action": signal["action"],
            "decision": submit_decision,
            "event_id": signal["event_id"],
            "event_state_path": str(event_path) if include_event_state else None,
            "broker_account_id": ACCOUNT_ID,
            "live_submission": live_submission,
            "orders": [launch_order],
            "response_text": "submitted" if submit_decision == "live_submitted" else submit_decision,
            "session_guard": (
                {"state_path": str(session_path)}
                if include_session_state
                else None
            ),
        }

    artifact_paths: dict[str, object] = {
        "live_canary_base_dir": str(base_dir),
        "result_path": str(launch_result_path),
    }
    if live_submission is not None:
        artifact_paths["submit_ledger_path"] = str(ledger_path)
        artifact_paths["submit_claim_path"] = str(claim_path)

    launch_payload = {
        "schema_name": "live_canary_launch_result",
        "schema_version": 1,
        "timestamp_chicago": LAUNCH_TIMESTAMP,
        "requested_live_submit": requested_live_submit,
        "submit_exit_code": 0,
        "submit_outcome": (
            "not_requested"
            if not requested_live_submit
            else submit_decision
        ),
        "submit_path_invoked": submit_path_invoked,
        "readiness_verdict": "ready" if requested_live_submit else "ready",
        "operator_message": "submitted" if submit_decision == "live_submitted" else submit_decision,
        "event_context": {
            "account_id": ACCOUNT_ID,
            "action": signal["action"],
            "broker_account_id": ACCOUNT_ID,
            "event_id": signal["event_id"],
            "live_submission_fingerprint": FINGERPRINT if live_submission is not None else None,
            "signal_date": SIGNAL_DATE,
            "strategy": STRATEGY,
            "symbol": signal["symbol"],
        },
        "artifact_paths": artifact_paths,
        "readiness": {
            "scope": {
                "account_id": ACCOUNT_ID,
                "event_id": signal["event_id"],
                "signal_date": SIGNAL_DATE,
                "strategy": STRATEGY,
            },
            "signal": {
                "event_id": signal["event_id"],
                "symbol": signal["symbol"],
                "action": signal["action"],
            },
            "evaluation": {
                "orders": [launch_order],
            },
        },
        "submit_result": submit_result,
    }
    _write_json(launch_result_path, launch_payload)
    return launch_result_path, base_dir


def _run_reconcile_json(
    capsys: pytest.CaptureFixture[str],
    *,
    args: list[str],
) -> tuple[int, dict[str, object] | None, str]:
    result = live_canary_state_ops.main(
        [
            "--emit",
            "json",
            "--timestamp",
            TIMESTAMP,
            *args,
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out.strip() else None
    return result, payload, captured.err


def _run_reconcile_text(
    capsys: pytest.CaptureFixture[str],
    *,
    args: list[str],
) -> tuple[int, str, str]:
    result = live_canary_state_ops.main(
        [
            "--emit",
            "text",
            "--timestamp",
            TIMESTAMP,
            *args,
        ]
    )
    captured = capsys.readouterr()
    return result, captured.out, captured.err


def test_live_canary_reconcile_happy_path_writes_durable_result_artifact(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path, base_dir = _write_launch_fixture(tmp_path)
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot(shares=1))
    orders_path = _write_json(tmp_path / "orders.json", _orders_payload())

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--orders-file",
            str(orders_path),
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result == 0
    assert stderr == ""
    assert payload is not None
    assert payload["schema_name"] == "live_canary_reconciliation_result"
    assert payload["verdict"] == "reconciled"
    assert payload["context"]["live_submission_fingerprint"] == FINGERPRINT
    assert payload["broker_truth"]["orders"][0]["fill_state"] == "filled"
    assert payload["broker_truth"]["orders"][0]["position_truth_matched"] is True
    result_path = Path(payload["artifact_paths"]["result_path"])
    assert result_path.exists()
    assert json.loads(result_path.read_text(encoding="utf-8")) == payload

    text_result, stdout, text_stderr = _run_reconcile_text(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--orders-file",
            str(orders_path),
            "--base-dir",
            str(base_dir),
        ],
    )
    assert text_result == 0
    assert text_stderr == ""
    assert "Verdict reconciled" in stdout
    assert f"fingerprint={FINGERPRINT}" in stdout
    assert f"Result path {result_path}" in stdout


def test_live_canary_reconcile_handles_preview_only_launch_artifact_deterministically(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path, base_dir = _write_launch_fixture(
        tmp_path,
        requested_live_submit=False,
        submit_path_invoked=False,
    )
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot(shares=0))

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result == 0
    assert stderr == ""
    assert payload is not None
    assert payload["verdict"] == "not_applicable"
    assert payload["mode"] == "preview_only"
    assert payload["next_actions"][0]["action_id"] == "no_closeout_required"


def test_live_canary_reconcile_fails_closed_when_launch_result_path_is_missing(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    missing_path = tmp_path / "missing-launch.json"
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot())

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(missing_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
        ],
    )

    assert result == 2
    assert payload is None
    assert "does not exist" in stderr


def test_live_canary_reconcile_fails_closed_when_launch_result_is_malformed(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path = _write_json(
        tmp_path / "launch.json",
        {
            "schema_name": "live_canary_launch_result",
            "schema_version": 1,
            "timestamp_chicago": LAUNCH_TIMESTAMP,
            "requested_live_submit": True,
            "submit_path_invoked": True,
            "submit_outcome": "live_submitted",
            "readiness_verdict": "ready",
        },
    )
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot())

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
        ],
    )

    assert result == 2
    assert payload is None
    assert "must include event_context" in stderr


def test_live_canary_reconcile_fails_closed_when_broker_truth_file_is_missing_for_submitted_launch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path, base_dir = _write_launch_fixture(tmp_path)
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot(shares=1))

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result == 2
    assert stderr == ""
    assert payload is not None
    assert "launch_broker_truth_missing:orders_file_required" in payload["blocking_reasons"]
    assert Path(payload["artifact_paths"]["result_path"]).exists()


def test_live_canary_reconcile_fails_closed_on_broker_account_mismatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path, base_dir = _write_launch_fixture(tmp_path)
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot(account_id="5WT99999"))
    orders_path = _write_json(tmp_path / "orders.json", _orders_payload())

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--orders-file",
            str(orders_path),
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result == 2
    assert stderr == ""
    assert payload is not None
    assert any(reason.startswith("broker_snapshot_account_mismatch:") for reason in payload["blocking_reasons"])


def test_live_canary_reconcile_fails_closed_on_open_order_without_fill_quantity(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path, base_dir = _write_launch_fixture(tmp_path)
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot(shares=0))
    orders_path = _write_json(
        tmp_path / "orders.json",
        _orders_payload(status="received", filled_quantity=None, remaining_quantity=None),
    )

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--orders-file",
            str(orders_path),
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result == 2
    assert stderr == ""
    assert payload is not None
    assert any(reason.startswith("broker_order_open_without_fill_quantity:") for reason in payload["blocking_reasons"])
    assert any(reason.startswith("broker_order_still_open:") for reason in payload["blocking_reasons"])


def test_live_canary_reconcile_fails_closed_on_resulting_position_truth_mismatch(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path, base_dir = _write_launch_fixture(tmp_path)
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot(shares=0))
    orders_path = _write_json(tmp_path / "orders.json", _orders_payload())

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--orders-file",
            str(orders_path),
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result == 2
    assert stderr == ""
    assert payload is not None
    assert any(reason.startswith("broker_position_truth_mismatch:EFA:0:1:") for reason in payload["blocking_reasons"])


def test_live_canary_reconcile_fails_closed_on_unreadable_durable_state(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    launch_result_path, base_dir = _write_launch_fixture(tmp_path)
    session_path = live_canary_session_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        strategy=STRATEGY,
        signal_date=SIGNAL_DATE,
    )
    session_path.write_text("{not-json", encoding="utf-8")
    positions_path = _write_json(tmp_path / "positions.json", _broker_snapshot(shares=1))
    orders_path = _write_json(tmp_path / "orders.json", _orders_payload())

    result, payload, stderr = _run_reconcile_json(
        capsys,
        args=[
            "reconcile",
            "--launch-result-file",
            str(launch_result_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--orders-file",
            str(orders_path),
            "--base-dir",
            str(base_dir),
        ],
    )

    assert result == 2
    assert payload is None
    assert "Expecting property name enclosed in double quotes" in stderr
