from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import pytest

from scripts import live_canary_guardrails, live_canary_state_ops
from trading_codex.execution import TastytradeBrokerExecutionAdapter, build_execution_plan, parse_broker_snapshot, parse_signal_payload
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    build_live_canary_submission_export,
    evaluate_live_canary,
    finalize_live_canary_event,
    finalize_live_canary_session,
    live_canary_event_state_path,
    live_canary_session_state_path,
    response_text_from_live_submission,
)


ACCOUNT_ID = "5WT00001"
AS_OF = "2026-03-23T10:40:00-04:00"
TIMESTAMP = "2026-03-23T10:45:00-04:00"
SIGNAL_DATE = "2026-03-20"
STRATEGY = "dual_mom_vol10_cash"


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


def _signal_payload(
    *,
    date: str = SIGNAL_DATE,
    strategy: str = STRATEGY,
    action: str = "RESIZE",
    symbol: str = "EFA",
    price: float | None = 99.16,
    target_shares: int = 100,
    resize_prev_shares: int | None = 82,
    resize_new_shares: int | None = 100,
    next_rebalance: str | None = "2026-03-31",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "date": date,
        "strategy": strategy,
        "action": action,
        "symbol": symbol,
        "price": price,
        "target_shares": target_shares,
        "resize_prev_shares": resize_prev_shares,
        "resize_new_shares": resize_new_shares,
        "next_rebalance": next_rebalance,
    }
    payload["event_id"] = _event_id(payload)
    return payload


def _broker_snapshot(
    *positions: dict[str, object],
    account_id: str = ACCOUNT_ID,
    as_of: str | None = AS_OF,
    buying_power: float | None = 20_000.0,
) -> dict[str, object]:
    return {
        "broker_name": "tastytrade",
        "account_id": account_id,
        "as_of": as_of,
        "buying_power": buying_power,
        "positions": list(positions),
    }


def _build_plan(signal_payload: dict[str, object]) -> object:
    signal = parse_signal_payload(signal_payload)
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
        )
    )
    return build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="state_ops_test",
        source_ref="signal.json",
        broker_source_ref=f"tastytrade:{ACCOUNT_ID}",
        data_dir=None,
    )


def _ready_live_export(signal_payload: dict[str, object]) -> object:
    plan = _build_plan(signal_payload)
    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account=ACCOUNT_ID,
        live_submit_requested=True,
        arm_live_canary=ACCOUNT_ID,
        allowed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        timestamp=datetime.fromisoformat(TIMESTAMP),
    )
    assert evaluation.decision == "ready_live_submit"
    return build_live_canary_submission_export(plan=plan, evaluation=evaluation)


class _MalformedLiveClient:
    def __init__(self) -> None:
        self.place_order_calls = 0

    def get_positions(self, *, account_id: str) -> object:
        raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

    def get_balances(self, *, account_id: str) -> object:
        raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

    def place_order(self, *, account_id: str, payload: dict[str, object]) -> object:
        assert account_id == ACCOUNT_ID
        assert payload["legs"][0]["symbol"] == "EFA"
        self.place_order_calls += 1
        return {"data": {"foo": "bar"}}


class _SuccessfulLiveClient:
    def __init__(self) -> None:
        self.place_order_calls = 0

    def get_positions(self, *, account_id: str) -> object:
        raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

    def get_balances(self, *, account_id: str) -> object:
        raise AssertionError("Snapshot reads are not part of this submit-only unit test.")

    def place_order(self, *, account_id: str, payload: dict[str, object]) -> object:
        assert account_id == ACCOUNT_ID
        assert payload["legs"][0]["symbol"] == "EFA"
        self.place_order_calls += 1
        return {"data": {"id": "order-123", "status": "received"}}


def _create_ambiguous_submit_tracking(base_dir: Path, signal_payload: dict[str, object]):
    export = _ready_live_export(signal_payload)
    ledger_path = base_dir / "broker_live_submission_fingerprints.jsonl"
    client = _MalformedLiveClient()
    submitted = TastytradeBrokerExecutionAdapter(account_id=ACCOUNT_ID, client=client).submit_live_orders(
        export=export,
        confirm_account_id=ACCOUNT_ID,
        live_allowed_account=ACCOUNT_ID,
        confirm_plan_sha256=export.plan_sha256,
        allowed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        live_max_order_notional=5000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=None,
    )
    assert submitted.submission_result == "ambiguous_attempted_submit_manual_clearance_required"
    assert submitted.durable_state is not None
    claim_path = Path(submitted.durable_state["claim_path"])
    assert ledger_path.exists()
    return export, submitted, ledger_path, claim_path, client


def _seed_pending_claim(claim_path: Path, *, export: object, signal_payload: dict[str, object]) -> None:
    claim_path.parent.mkdir(parents=True, exist_ok=True)
    claim_path.write_text(
        json.dumps(
            {
                "account_id": ACCOUNT_ID,
                "claim_path": str(claim_path),
                "event_id": signal_payload["event_id"],
                "generated_at_chicago": TIMESTAMP,
                "live_submission_fingerprint": claim_path.stem,
                "manual_clearance_required": True,
                "plan_sha256": export.plan_sha256,
                "result": "claim_pending_manual_clearance_required",
                "signal_date": signal_payload["date"],
                "strategy": signal_payload["strategy"],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_event_and_session_state(base_dir: Path, signal_payload: dict[str, object], live_submission: object) -> tuple[Path, Path]:
    signal = parse_signal_payload(signal_payload)
    live_submission_payload = live_canary_guardrails._render_live_submission_receipt(live_submission)
    response_text = response_text_from_live_submission(live_submission)

    session_record = live_canary_guardrails._live_canary_session_record(
        account_id=ACCOUNT_ID,
        signal=signal,
        claimed_at_chicago=TIMESTAMP,
        updated_at_chicago=live_submission.generated_at_chicago,
        decision="live_submit_refused",
        manual_clearance_required=True,
        response_text=response_text,
        result=live_submission.submission_result,
        live_submission=live_submission_payload,
    )
    event_record = live_canary_guardrails._live_canary_event_record(
        account_id=ACCOUNT_ID,
        signal=signal,
        generated_at_chicago=TIMESTAMP,
        decision="live_submit_refused",
        manual_clearance_required=True,
        response_text=response_text,
        result=live_submission.submission_result,
        live_submission=live_submission_payload,
    )

    session_path = live_canary_session_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        strategy=signal.strategy,
        signal_date=signal.date,
    )
    event_path = live_canary_event_state_path(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        event_id=signal.event_id,
    )
    finalize_live_canary_session(state_path=session_path, record=session_record)
    finalize_live_canary_event(state_path=event_path, record=event_record)
    return event_path, session_path


def _run_state_ops_cli(
    capsys: pytest.CaptureFixture[str],
    *,
    base_dir: Path,
    args: list[str],
) -> tuple[int, dict[str, object] | None, str]:
    result = live_canary_state_ops.main(
        [
            "--emit",
            "json",
            "--base-dir",
            str(base_dir),
            "--timestamp",
            TIMESTAMP,
            *args,
        ]
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out) if captured.out.strip() else None
    return result, payload, captured.err


def test_live_canary_state_ops_status_reports_populated_blocking_state(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_dir = tmp_path / "live_canary"
    signal_payload = _signal_payload()
    export, live_submission, _ledger_path, claim_path, _client = _create_ambiguous_submit_tracking(base_dir, signal_payload)
    _seed_pending_claim(claim_path, export=export, signal_payload=signal_payload)
    event_path, session_path = _write_event_and_session_state(base_dir, signal_payload, live_submission)

    result, payload, stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "status",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
        ],
    )

    assert result == 0
    assert stderr == ""
    assert payload is not None
    assert payload["summary"]["event_state_count"] == 1
    assert payload["summary"]["session_state_count"] == 1
    assert payload["summary"]["submit_tracking_count"] == 2
    assert {artifact["artifact_kind"] for artifact in payload["blocking_artifacts"]} == {
        "event_state",
        "session_state",
        "submit_tracking_claim",
        "submit_tracking_ledger",
    }
    assert {artifact["path"] for artifact in payload["blocking_artifacts"]} >= {
        str(event_path),
        str(session_path),
        str(claim_path),
    }


def test_live_canary_state_ops_clear_requires_explicit_scope_and_confirmation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_dir = tmp_path / "live_canary"

    result, _payload, stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--clear",
            "event",
        ],
    )
    assert result == 2
    assert "requires --event-id" in stderr

    signal_payload = _signal_payload()
    export, live_submission, _ledger_path, claim_path, _client = _create_ambiguous_submit_tracking(base_dir, signal_payload)
    _seed_pending_claim(claim_path, export=export, signal_payload=signal_payload)
    _event_path, session_path = _write_event_and_session_state(base_dir, signal_payload, live_submission)

    result, _payload, stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--signal-date",
            SIGNAL_DATE,
            "--clear",
            "session",
            "--apply",
        ],
    )
    assert result == 2
    assert "--apply requires --confirm" in stderr
    assert session_path.exists()


def test_live_canary_state_ops_clear_preview_is_dry_run_and_narrow_scope(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_dir = tmp_path / "live_canary"
    signal_payload = _signal_payload()
    export, live_submission, _ledger_path, claim_path, _client = _create_ambiguous_submit_tracking(base_dir, signal_payload)
    _seed_pending_claim(claim_path, export=export, signal_payload=signal_payload)
    event_path, session_path = _write_event_and_session_state(base_dir, signal_payload, live_submission)

    result, payload, stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--signal-date",
            SIGNAL_DATE,
            "--clear",
            "session",
        ],
    )

    assert result == 0
    assert stderr == ""
    assert payload is not None
    assert payload["apply"] is False
    assert [operation["artifact_kind"] for operation in payload["planned_operations"]] == ["session_state"]
    assert session_path.exists()
    assert event_path.exists()
    assert claim_path.exists()


def test_live_canary_state_ops_apply_session_clear_is_narrow_and_idempotent(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_dir = tmp_path / "live_canary"
    signal_payload = _signal_payload()
    export, live_submission, ledger_path, claim_path, _client = _create_ambiguous_submit_tracking(base_dir, signal_payload)
    _seed_pending_claim(claim_path, export=export, signal_payload=signal_payload)
    event_path, session_path = _write_event_and_session_state(base_dir, signal_payload, live_submission)

    preview_result, preview_payload, _stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--signal-date",
            SIGNAL_DATE,
            "--clear",
            "session",
        ],
    )
    assert preview_result == 0
    assert preview_payload is not None
    confirm = str(preview_payload["confirmation_token"])

    apply_result, apply_payload, stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--signal-date",
            SIGNAL_DATE,
            "--clear",
            "session",
            "--apply",
            "--confirm",
            confirm,
        ],
    )
    assert apply_result == 0
    assert stderr == ""
    assert apply_payload is not None
    assert session_path.exists() is False
    archive_targets = [Path(operation["archive_path"]) for operation in apply_payload["applied_operations"] if operation["artifact_kind"] == "session_state"]
    assert len(archive_targets) == 1
    assert archive_targets[0].exists()
    assert event_path.exists()
    assert claim_path.exists()
    ledger_lines = ledger_path.read_text(encoding="utf-8").splitlines()
    assert len(ledger_lines) == 1

    second_result, second_payload, second_stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--signal-date",
            SIGNAL_DATE,
            "--clear",
            "session",
            "--apply",
            "--confirm",
            confirm,
        ],
    )
    assert second_result == 0
    assert second_stderr == ""
    assert second_payload is not None
    assert second_payload["applied_operations"] == []
    assert len(ledger_path.read_text(encoding="utf-8").splitlines()) == 1


def test_live_canary_state_ops_partial_persistence_failure_can_be_inspected_and_cleared(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    base_dir = tmp_path / "live_canary"
    signal_payload = _signal_payload()
    export, live_submission, ledger_path, claim_path, _client = _create_ambiguous_submit_tracking(base_dir, signal_payload)
    _seed_pending_claim(claim_path, export=export, signal_payload=signal_payload)

    status_result, status_payload, status_stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "status",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--event-id",
            str(signal_payload["event_id"]),
        ],
    )
    assert status_result == 0
    assert status_stderr == ""
    assert status_payload is not None
    assert {artifact["artifact_kind"] for artifact in status_payload["blocking_artifacts"]} == {
        "submit_tracking_claim",
        "submit_tracking_ledger",
    }

    preview_result, preview_payload, _preview_stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--event-id",
            str(signal_payload["event_id"]),
            "--clear",
            "submit-tracking",
        ],
    )
    assert preview_result == 0
    assert preview_payload is not None
    assert [operation["artifact_kind"] for operation in preview_payload["planned_operations"]] == [
        "submit_tracking_claim",
        "submit_tracking_ledger",
    ]
    confirm = str(preview_payload["confirmation_token"])

    apply_result, apply_payload, apply_stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "clear",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--event-id",
            str(signal_payload["event_id"]),
            "--clear",
            "submit-tracking",
            "--apply",
            "--confirm",
            confirm,
        ],
    )
    assert apply_result == 0
    assert apply_stderr == ""
    assert apply_payload is not None
    assert claim_path.exists() is False
    ledger_records = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert ledger_records[-1]["result"] == "operator_cleared"
    assert ledger_records[-1]["event_id"] == signal_payload["event_id"]

    after_result, after_payload, after_stderr = _run_state_ops_cli(
        capsys,
        base_dir=base_dir,
        args=[
            "status",
            "--account-id",
            ACCOUNT_ID,
            "--strategy",
            STRATEGY,
            "--event-id",
            str(signal_payload["event_id"]),
        ],
    )
    assert after_result == 0
    assert after_stderr == ""
    assert after_payload is not None
    assert after_payload["summary"]["blocking_artifact_count"] == 0


def test_live_canary_operator_clear_marker_unblocks_duplicate_retry(tmp_path: Path) -> None:
    base_dir = tmp_path / "live_canary"
    signal_payload = _signal_payload()
    export, _live_submission, ledger_path, claim_path, _client = _create_ambiguous_submit_tracking(base_dir, signal_payload)

    blocked_client = _SuccessfulLiveClient()
    blocked_retry = TastytradeBrokerExecutionAdapter(account_id=ACCOUNT_ID, client=blocked_client).submit_live_orders(
        export=export,
        confirm_account_id=ACCOUNT_ID,
        live_allowed_account=ACCOUNT_ID,
        confirm_plan_sha256=export.plan_sha256,
        allowed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        live_max_order_notional=5000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=None,
    )
    assert blocked_retry.submission_result == "refused_duplicate"
    assert "live_submit_duplicate_fingerprint" in blocked_retry.refusal_reasons
    assert blocked_client.place_order_calls == 0

    preview_payload = live_canary_state_ops.preview_live_canary_state_clear(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        strategy=STRATEGY,
        event_id=str(signal_payload["event_id"]),
        clear_scopes={"submit-tracking"},
        timestamp=datetime.fromisoformat(TIMESTAMP),
    )
    confirm = str(preview_payload["confirmation_token"])
    live_canary_state_ops.apply_live_canary_state_clear(
        base_dir=base_dir,
        account_id=ACCOUNT_ID,
        strategy=STRATEGY,
        event_id=str(signal_payload["event_id"]),
        clear_scopes={"submit-tracking"},
        confirm=confirm,
        timestamp=datetime.fromisoformat(TIMESTAMP),
    )

    success_client = _SuccessfulLiveClient()
    cleared_retry = TastytradeBrokerExecutionAdapter(account_id=ACCOUNT_ID, client=success_client).submit_live_orders(
        export=export,
        confirm_account_id=ACCOUNT_ID,
        live_allowed_account=ACCOUNT_ID,
        confirm_plan_sha256=export.plan_sha256,
        allowed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        live_max_order_notional=5000.0,
        live_max_order_qty=100,
        ledger_path=ledger_path,
        live_submission_artifact_path=None,
    )
    assert cleared_retry.submission_result == "submitted"
    assert cleared_retry.submission_succeeded is True
    assert success_client.place_order_calls == 1
