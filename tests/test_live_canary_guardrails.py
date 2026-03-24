from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts import live_canary_guardrails
from trading_codex.execution import build_execution_plan, parse_broker_snapshot, parse_signal_payload
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    LiveCanaryEvaluation,
    LiveCanaryOrder,
    append_live_canary_audit,
    audit_rows_for_result,
    claim_live_canary_event,
    evaluate_live_canary,
    evaluate_live_canary_affordability,
    finalize_live_canary_event,
    live_canary_audit_path,
    resolve_live_canary_affordability_source,
)


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


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
    date: str = "2026-03-19",
    strategy: str = "dual_mom_vol10_cash",
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
    account_id: str = "5WT00001",
    as_of: str | None = None,
    cash: float | None = None,
    buying_power: float = 20_000.0,
) -> dict[str, object]:
    return {
        "broker_name": "tastytrade",
        "account_id": account_id,
        "as_of": as_of,
        "cash": cash,
        "buying_power": buying_power,
        "positions": list(positions),
    }


def _build_plan(
    signal_payload: dict[str, object],
    *,
    positions: list[dict[str, object]],
    account_id: str = "5WT00001",
    as_of: str | None = None,
    cash: float | None = None,
    buying_power: float | None = 20_000.0,
) -> object:
    signal = parse_signal_payload(signal_payload)
    broker = parse_broker_snapshot(
        _broker_snapshot(
            *positions,
            account_id=account_id,
            as_of=as_of,
            cash=cash,
            buying_power=buying_power,
        )
    )
    return build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols=set(DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS),
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="canary_test",
        source_ref="signal.json",
        broker_source_ref=f"tastytrade:{account_id}",
        data_dir=None,
    )


def _timestamp(value: str) -> datetime:
    return datetime.fromisoformat(value)


class _SnapshotSequenceAdapter:
    def __init__(
        self,
        *snapshots: dict[str, object],
        snapshot_errors: dict[int, Exception] | None = None,
        submit_result: object | None = None,
        submit_error: Exception | None = None,
    ) -> None:
        self._snapshots = [parse_broker_snapshot(snapshot) for snapshot in snapshots]
        self._snapshot_errors = dict(snapshot_errors or {})
        self._submit_result = submit_result
        self._submit_error = submit_error
        self.load_calls = 0
        self.submit_calls = 0
        self.events: list[tuple[str, int]] = []
        self.submit_kwargs: list[dict[str, object]] = []

    def load_snapshot(self):
        next_call = self.load_calls + 1
        self.events.append(("load_snapshot", next_call))
        self.load_calls = next_call
        if next_call in self._snapshot_errors:
            raise self._snapshot_errors[next_call]
        if self.load_calls > len(self._snapshots):
            raise AssertionError("No broker snapshot prepared for this load.")
        snapshot = self._snapshots[self.load_calls - 1]
        return snapshot

    def submit_live_orders(self, **kwargs: object):
        self.submit_calls += 1
        self.events.append(("submit_live_orders", self.load_calls))
        self.submit_kwargs.append(dict(kwargs))
        if self._submit_error is not None:
            raise self._submit_error
        if self._submit_result is None:
            raise AssertionError("submit_live_orders should not be called in this test.")
        return self._submit_result


def _live_submission_response(
    *,
    attempted: bool,
    succeeded: bool,
    submission_result: str,
    refusal_reasons: list[str] | None = None,
    manual_clearance_required: bool = False,
    order_error: str | None = None,
    live_submission_fingerprint: str | None = None,
    duplicate_submit_refusal: dict[str, object] | None = None,
    durable_state: dict[str, object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        live_submit_attempted=attempted,
        manual_clearance_required=manual_clearance_required,
        orders=[
            SimpleNamespace(
                attempted=attempted,
                broker_order_id="order-123" if attempted and succeeded else None,
                broker_status="accepted" if succeeded else None,
                error=order_error,
                quantity=1,
                side="BUY",
                succeeded=succeeded,
                symbol="EFA",
            )
        ],
        refusal_reasons=list(refusal_reasons or []),
        live_submission_fingerprint=live_submission_fingerprint,
        duplicate_submit_refusal=duplicate_submit_refusal,
        durable_state=durable_state,
        submission_result=submission_result,
        submission_succeeded=succeeded,
    )


def _run_live_submit_guardrails(
    *,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    snapshots: tuple[dict[str, object], ...],
    snapshot_errors: dict[int, Exception] | None = None,
    submit_result: object | None = None,
    submit_error: Exception | None = None,
    signal_payload: dict[str, object] | None = None,
) -> tuple[int, dict[str, object], Path, _SnapshotSequenceAdapter]:
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(
        json.dumps(signal_payload or _signal_payload(date="2026-03-20")),
        encoding="utf-8",
    )
    base_dir = tmp_path / "live_canary"
    created_adapters: list[_SnapshotSequenceAdapter] = []

    class FakeTastytradeBrokerExecutionAdapter(_SnapshotSequenceAdapter):
        def __init__(self, *, account_id: str, client: object | None = None) -> None:
            del account_id, client
            super().__init__(
                *snapshots,
                snapshot_errors=snapshot_errors,
                submit_result=submit_result,
                submit_error=submit_error,
            )
            created_adapters.append(self)

    monkeypatch.setattr(live_canary_guardrails, "load_tastytrade_secrets", lambda *, secrets_file=None: None)
    monkeypatch.setattr(
        live_canary_guardrails,
        "TastytradeBrokerExecutionAdapter",
        FakeTastytradeBrokerExecutionAdapter,
    )

    result = live_canary_guardrails.main(
        [
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "tastytrade",
            "--live-canary-account",
            "5WT00001",
            "--live-submit",
            "--arm-live-canary",
            "5WT00001",
            "--timestamp",
            "2026-03-23T10:45:00-04:00",
            "--base-dir",
            str(base_dir),
            "--emit",
            "json",
        ]
    )

    captured = capsys.readouterr()
    assert len(created_adapters) == 1
    return result, json.loads(captured.out), base_dir, created_adapters[0]


def test_live_canary_blocks_missing_account_binding() -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account=None,
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_requires_account_binding" in evaluation.blockers


def test_live_canary_blocks_mismatched_account_binding() -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT99999",
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_account_binding_mismatch" in evaluation.blockers


def test_live_canary_blocks_missing_arming() -> None:
    plan = _build_plan(
        _signal_payload(date="2026-03-20"),
        positions=[{"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary=None,
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_not_armed" in evaluation.blockers


@pytest.mark.parametrize(
    ("signal_payload", "expected_blocker"),
    [
        (
            _signal_payload(symbol="TLT", target_shares=10, resize_prev_shares=None, resize_new_shares=None),
            "live_canary_symbol_not_allowed:TLT",
        ),
        (
            _signal_payload(action="SOMETHING_ELSE", target_shares=10, resize_prev_shares=None, resize_new_shares=None),
            "live_canary_unsupported_action:SOMETHING_ELSE",
        ),
    ],
)
def test_live_canary_blocks_unsupported_symbols_and_actions(
    signal_payload: dict[str, object],
    expected_blocker: str,
) -> None:
    plan = _build_plan(
        signal_payload,
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert expected_blocker in evaluation.blockers


def test_live_canary_caps_buy_to_one_share_deterministically() -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "dry_run_ready"
    assert len(evaluation.orders) == 1
    order = evaluation.orders[0]
    assert order.symbol == "EFA"
    assert order.side == "BUY"
    assert order.requested_qty == 100
    assert order.executable_qty == 1
    assert order.cap_applied is True
    assert "live_canary_qty_capped:EFA:100:1" in evaluation.warnings


def test_live_canary_affordability_source_prefers_buying_power_over_cash() -> None:
    broker_snapshot = parse_broker_snapshot(
        _broker_snapshot(
            account_id="5WT00001",
            as_of="2026-03-23T10:40:00-04:00",
            cash=500.0,
            buying_power=1_000.0,
        )
    )

    source, selected_amount, available_amount, issue = resolve_live_canary_affordability_source(broker_snapshot)

    assert source == "buying_power"
    assert selected_amount == 1_000.0
    assert available_amount == 1_000.0
    assert issue is None


def test_live_canary_affordability_source_falls_back_to_cash_when_buying_power_missing() -> None:
    broker_snapshot = parse_broker_snapshot(
        _broker_snapshot(
            account_id="5WT00001",
            as_of="2026-03-23T10:40:00-04:00",
            cash=500.0,
            buying_power=None,
        )
    )

    source, selected_amount, available_amount, issue = resolve_live_canary_affordability_source(broker_snapshot)

    assert source == "cash"
    assert selected_amount == 500.0
    assert available_amount == 500.0
    assert issue is None


@pytest.mark.parametrize(
    ("cash", "buying_power", "expected_source", "expected_selected_amount", "expected_issue"),
    [
        (None, None, None, None, "buying_power_missing_and_cash_missing"),
        (500.0, 0.0, "buying_power", 0.0, "buying_power_non_positive"),
        (0.0, None, "cash", 0.0, "cash_non_positive"),
        (None, float("nan"), "buying_power", None, "buying_power_unusable"),
    ],
)
def test_live_canary_affordability_source_marks_missing_zero_and_unusable_values_unavailable(
    cash: float | None,
    buying_power: float | None,
    expected_source: str | None,
    expected_selected_amount: float | None,
    expected_issue: str,
) -> None:
    broker_snapshot = parse_broker_snapshot(
        _broker_snapshot(
            account_id="5WT00001",
            as_of="2026-03-23T10:40:00-04:00",
            cash=cash,
            buying_power=buying_power,
        )
    )

    source, selected_amount, available_amount, issue = resolve_live_canary_affordability_source(broker_snapshot)

    assert source == expected_source
    assert selected_amount == expected_selected_amount
    assert available_amount is None
    assert issue == expected_issue


def test_live_canary_affordability_evaluation_uses_buy_side_order_notional_and_total() -> None:
    broker_snapshot = parse_broker_snapshot(
        _broker_snapshot(
            account_id="5WT00001",
            as_of="2026-03-23T10:40:00-04:00",
            buying_power=150.0,
        )
    )
    affordability, messages = evaluate_live_canary_affordability(
        broker_snapshot=broker_snapshot,
        orders=[
            LiveCanaryOrder(
                symbol="EFA",
                side="BUY",
                requested_qty=100,
                executable_qty=1,
                current_broker_shares=0,
                desired_signal_shares=100,
                desired_canary_shares=1,
                classification="BUY",
                reference_price=99.16,
                estimated_notional=99.16,
                cap_applied=True,
            ),
            LiveCanaryOrder(
                symbol="SPY",
                side="BUY",
                requested_qty=100,
                executable_qty=1,
                current_broker_shares=0,
                desired_signal_shares=100,
                desired_canary_shares=1,
                classification="BUY",
                reference_price=75.0,
                estimated_notional=75.0,
                cap_applied=True,
            ),
        ],
    )

    assert affordability.applicable is True
    assert affordability.status == "insufficient"
    assert affordability.source == "buying_power"
    assert affordability.available_amount == 150.0
    assert affordability.required_buy_notional_total == 174.16
    assert affordability.required_buy_notional_complete is True
    assert affordability.sufficient is False
    assert affordability.order_details[0].status == "affordable"
    assert affordability.order_details[1].status == "affordable"
    assert messages == ["live_canary_total_buy_notional_exceeds_available:174.16:150.00"]


def test_live_canary_rotate_with_one_share_unwind_keeps_every_leg_at_cap() -> None:
    plan = _build_plan(
        _signal_payload(
            date="2026-03-20",
            action="ROTATE",
            symbol="EFA",
            target_shares=100,
            resize_prev_shares=None,
            resize_new_shares=None,
        ),
        positions=[{"symbol": "BIL", "shares": 1, "price": 91.20, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "ready_live_submit"
    assert [(order.symbol, order.side, order.executable_qty) for order in evaluation.orders] == [
        ("BIL", "SELL", 1),
        ("EFA", "BUY", 1),
    ]
    assert all(order.executable_qty <= 1 for order in evaluation.orders)


def test_live_canary_dry_run_warns_but_does_not_block_when_affordability_is_unavailable() -> None:
    plan = _build_plan(
        _signal_payload(date="2026-03-20"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
        buying_power=None,
        cash=None,
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "dry_run_ready"
    assert evaluation.blockers == []
    assert "live_canary_affordability_unavailable:buying_power_missing_and_cash_missing" in evaluation.warnings
    assert evaluation.affordability is not None
    assert evaluation.affordability.status == "unavailable"
    assert evaluation.affordability.source is None
    assert evaluation.affordability.available_amount is None
    assert evaluation.affordability.required_buy_notional_total == 99.16


def test_live_canary_live_submit_blocks_when_affordability_is_unavailable() -> None:
    plan = _build_plan(
        _signal_payload(date="2026-03-20"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
        buying_power=None,
        cash=None,
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_affordability_unavailable:buying_power_missing_and_cash_missing" in evaluation.blockers
    assert evaluation.affordability is not None
    assert evaluation.affordability.status == "unavailable"


def test_live_canary_live_submit_blocks_when_affordability_is_insufficient() -> None:
    plan = _build_plan(
        _signal_payload(date="2026-03-20"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
        buying_power=50.0,
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_buy_order_notional_exceeds_available:EFA:99.16:50.00" in evaluation.blockers
    assert "live_canary_total_buy_notional_exceeds_available:99.16:50.00" in evaluation.blockers
    assert evaluation.affordability is not None
    assert evaluation.affordability.status == "insufficient"
    assert evaluation.affordability.available_amount == 50.0
    assert len(evaluation.affordability.order_details) == 1
    assert evaluation.affordability.order_details[0].symbol == "EFA"
    assert evaluation.affordability.order_details[0].affordable is False
    assert evaluation.affordability.order_details[0].status == "insufficient"


def test_live_canary_missing_estimated_notional_warns_in_dry_run_and_blocks_live_submit() -> None:
    signal_payload = _signal_payload(
        date="2026-03-20",
        price=None,
        resize_prev_shares=None,
        resize_new_shares=None,
        target_shares=100,
    )
    plan = _build_plan(
        signal_payload,
        positions=[{"symbol": "EFA", "shares": 0, "price": None, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
        buying_power=500.0,
    )

    dry_run = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )
    live_submit = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert dry_run.decision == "dry_run_ready"
    assert dry_run.blockers == []
    assert "live_canary_missing_reference_price:EFA" in dry_run.warnings
    assert "live_canary_missing_estimated_notional:EFA" in dry_run.warnings
    assert live_submit.decision == "blocked"
    assert "live_canary_missing_estimated_notional:EFA" in live_submit.blockers
    assert live_submit.affordability is not None
    assert live_submit.affordability.status == "unavailable"
    assert live_submit.affordability.required_buy_notional_complete is False


def test_live_canary_sell_only_flow_is_unaffected_by_affordability_guardrail() -> None:
    plan = _build_plan(
        _signal_payload(
            date="2026-03-20",
            action="EXIT",
            symbol="CASH",
            price=None,
            target_shares=0,
            resize_prev_shares=None,
            resize_new_shares=None,
        ),
        positions=[{"symbol": "BIL", "shares": 1, "price": 91.20, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
        buying_power=None,
        cash=None,
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "ready_live_submit"
    assert evaluation.blockers == []
    assert evaluation.affordability is not None
    assert evaluation.affordability.status == "not_applicable"
    assert evaluation.affordability.order_details == []


def test_pre_submit_reconciliation_matches_when_refreshed_state_stays_ready() -> None:
    signal_payload = _signal_payload(date="2026-03-20")
    plan = _build_plan(
        signal_payload,
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
    )
    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )
    adapter = _SnapshotSequenceAdapter(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            account_id="5WT00001",
            as_of="2026-03-23T10:44:00-04:00",
        )
    )

    reconciliation = live_canary_guardrails._reconcile_live_canary_pre_submit(
        broker_adapter=adapter,
        original_plan=plan,
        original_evaluation=evaluation,
        account_id="5WT00001",
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert reconciliation.matched is True
    assert reconciliation.blockers == []
    assert reconciliation.evaluation.decision == "ready_live_submit"
    assert [(order.symbol, order.side, order.executable_qty) for order in reconciliation.evaluation.orders] == [
        ("EFA", "BUY", 1)
    ]
    assert adapter.load_calls == 1


def test_pre_submit_reconciliation_blocks_when_order_assumptions_change() -> None:
    signal_payload = _signal_payload(
        date="2026-03-20",
        action="ROTATE",
        target_shares=100,
        resize_prev_shares=None,
        resize_new_shares=None,
    )
    plan = _build_plan(
        signal_payload,
        positions=[{"symbol": "BIL", "shares": 1, "price": 91.20, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
    )
    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )
    adapter = _SnapshotSequenceAdapter(
        _broker_snapshot(
            account_id="5WT00001",
            as_of="2026-03-23T10:44:00-04:00",
        )
    )

    reconciliation = live_canary_guardrails._reconcile_live_canary_pre_submit(
        broker_adapter=adapter,
        original_plan=plan,
        original_evaluation=evaluation,
        account_id="5WT00001",
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert reconciliation.matched is False
    assert reconciliation.evaluation.decision == "ready_live_submit"
    assert "live_canary_pre_submit_order_missing:BIL" in reconciliation.blockers
    assert reconciliation.response_text == "live_canary_pre_submit_order_missing:BIL"
    assert reconciliation.details is not None
    assert reconciliation.details["order_drift"]["missing_orders"] == [
        {
            "cap_applied": False,
            "classification": "SELL",
            "current_broker_shares": 1,
            "desired_canary_shares": 0,
            "desired_signal_shares": 0,
            "estimated_notional": 91.2,
            "executable_qty": 1,
            "reference_price": 91.2,
            "requested_qty": 1,
            "side": "SELL",
            "symbol": "BIL",
        }
    ]


def test_pre_submit_reconciliation_details_expose_explicit_changed_field_values() -> None:
    signal = parse_signal_payload(_signal_payload(date="2026-03-20"))
    original_evaluation = LiveCanaryEvaluation(
        timestamp_chicago="2026-03-23T10:45:00-04:00",
        account_id="5WT00001",
        broker_account_id="5WT00001",
        signal=signal,
        live_submit_requested=True,
        armed=True,
        decision="ready_live_submit",
        blockers=[],
        warnings=[],
        orders=[
            LiveCanaryOrder(
                symbol="EFA",
                side="BUY",
                requested_qty=100,
                executable_qty=1,
                current_broker_shares=0,
                desired_signal_shares=100,
                desired_canary_shares=1,
                classification="BUY",
                reference_price=99.16,
                estimated_notional=99.16,
                cap_applied=True,
            )
        ],
    )
    refreshed_evaluation = LiveCanaryEvaluation(
        timestamp_chicago="2026-03-23T10:45:00-04:00",
        account_id="5WT00001",
        broker_account_id="5WT00001",
        signal=signal,
        live_submit_requested=True,
        armed=True,
        decision="ready_live_submit",
        blockers=[],
        warnings=[],
        orders=[
            LiveCanaryOrder(
                symbol="EFA",
                side="BUY",
                requested_qty=101,
                executable_qty=2,
                current_broker_shares=-1,
                desired_signal_shares=100,
                desired_canary_shares=1,
                classification="RESIZE_BUY",
                reference_price=99.16,
                estimated_notional=198.32,
                cap_applied=False,
            )
        ],
    )

    blockers, order_drift = live_canary_guardrails._compare_live_canary_order_assumptions(
        original_evaluation=original_evaluation,
        refreshed_evaluation=refreshed_evaluation,
    )

    assert "live_canary_pre_submit_order_changed:EFA:current_broker_shares:0:-1" in blockers
    assert "live_canary_pre_submit_order_changed:EFA:executable_qty:1:2" in blockers
    assert "live_canary_pre_submit_order_changed:EFA:classification:BUY:RESIZE_BUY" in blockers
    assert order_drift is not None
    changed_order = order_drift["changed_orders"][0]
    assert changed_order["symbol"] == "EFA"
    assert changed_order["original_order"]["current_broker_shares"] == 0
    assert changed_order["refreshed_order"]["current_broker_shares"] == -1
    changes = {change["field"]: change for change in changed_order["changes"]}
    assert changes["current_broker_shares"]["original_value"] == 0
    assert changes["current_broker_shares"]["refreshed_value"] == -1
    assert changes["executable_qty"]["original_value"] == 1
    assert changes["executable_qty"]["refreshed_value"] == 2
    assert changes["classification"]["original_value"] == "BUY"
    assert changes["classification"]["refreshed_value"] == "RESIZE_BUY"


def test_pre_submit_reconciliation_blocks_when_refreshed_state_adds_blocker() -> None:
    signal_payload = _signal_payload(date="2026-03-20")
    plan = _build_plan(
        signal_payload,
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
    )
    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )
    adapter = _SnapshotSequenceAdapter(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            account_id="5WT99999",
            as_of="2026-03-23T10:44:00-04:00",
        )
    )

    reconciliation = live_canary_guardrails._reconcile_live_canary_pre_submit(
        broker_adapter=adapter,
        original_plan=plan,
        original_evaluation=evaluation,
        account_id="5WT00001",
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert reconciliation.matched is False
    assert reconciliation.evaluation.decision == "blocked"
    assert "live_canary_pre_submit_decision_changed:ready_live_submit:blocked" in reconciliation.blockers
    assert "live_canary_pre_submit_blocker:live_canary_account_binding_mismatch" in reconciliation.blockers
    assert "live_canary_pre_submit_broker_account_changed:5WT00001:5WT99999" in reconciliation.blockers


@pytest.mark.parametrize(
    ("timestamp", "expected_decision", "expected_blocker"),
    [
        ("2026-03-23T10:45:00-04:00", "ready_live_submit", None),
        ("2026-03-23T09:29:59-04:00", "blocked", "live_canary_submit_outside_regular_session"),
    ],
)
def test_live_canary_live_submit_session_gate(
    timestamp: str,
    expected_decision: str,
    expected_blocker: str | None,
) -> None:
    plan = _build_plan(
        _signal_payload(date="2026-03-20"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp(timestamp),
    )

    assert evaluation.decision == expected_decision
    if expected_blocker is None:
        assert "live_canary_submit_outside_regular_session" not in evaluation.blockers
    else:
        assert expected_blocker in evaluation.blockers


@pytest.mark.parametrize(
    ("signal_date", "expected_blocker"),
    [
        ("2026-03-19", "live_canary_signal_date_mismatch:2026-03-19:2026-03-20"),
        ("2026-03-23", "live_canary_signal_date_mismatch:2026-03-23:2026-03-20"),
        ("2026-03-24", "live_canary_signal_date_mismatch:2026-03-24:2026-03-20"),
    ],
    ids=["older", "same_day", "future_dated"],
)
def test_live_canary_blocks_non_latest_completed_signal_date_for_live_submit(
    signal_date: str,
    expected_blocker: str,
) -> None:
    plan = _build_plan(
        _signal_payload(date=signal_date),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-03-23T10:40:00-04:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "blocked"
    assert expected_blocker in evaluation.blockers


def test_live_canary_blocks_market_holiday_during_nominal_session_hours() -> None:
    plan = _build_plan(
        _signal_payload(date="2026-07-02"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-07-03T10:40:00-04:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-07-03T10:45:00-04:00"),
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_submit_market_holiday:2026-07-03" in evaluation.blockers


@pytest.mark.parametrize(
    ("timestamp", "signal_date", "expected_decision", "expected_blocker"),
    [
        ("2026-11-27T13:00:00-05:00", "2026-11-25", "ready_live_submit", None),
        ("2026-11-27T13:05:00-05:00", "2026-11-27", "blocked", "live_canary_submit_outside_regular_session"),
    ],
    ids=["black_friday_boundary", "black_friday_after_close"],
)
def test_live_canary_respects_black_friday_early_close(
    timestamp: str,
    signal_date: str,
    expected_decision: str,
    expected_blocker: str | None,
) -> None:
    plan = _build_plan(
        _signal_payload(date=signal_date),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2026-11-27T12:55:00-05:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp(timestamp),
    )

    assert evaluation.decision == expected_decision
    if expected_blocker is None:
        assert "live_canary_submit_outside_regular_session" not in evaluation.blockers
    else:
        assert expected_blocker in evaluation.blockers


def test_live_canary_blocks_observed_new_year_closure_on_prior_year_dec_31() -> None:
    plan = _build_plan(
        _signal_payload(date="2027-12-30"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2027-12-31T10:40:00-05:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2027-12-31T10:45:00-05:00"),
    )

    assert evaluation.decision == "blocked"
    assert "live_canary_submit_market_holiday:2027-12-31" in evaluation.blockers


@pytest.mark.parametrize(
    ("signal_date", "expected_decision", "expected_blocker"),
    [
        ("2027-12-30", "ready_live_submit", None),
        ("2027-12-31", "blocked", "live_canary_signal_date_mismatch:2027-12-31:2027-12-30"),
    ],
    ids=["cross_year_latest_completed", "cross_year_observed_holiday_skipped"],
)
def test_live_canary_latest_completed_session_skips_cross_year_observed_new_year_closure(
    signal_date: str,
    expected_decision: str,
    expected_blocker: str | None,
) -> None:
    plan = _build_plan(
        _signal_payload(date=signal_date),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of="2028-01-03T10:40:00-05:00",
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2028-01-03T10:45:00-05:00"),
    )

    assert evaluation.decision == expected_decision
    if expected_blocker is None:
        assert "live_canary_signal_date_mismatch:2027-12-31:2027-12-30" not in evaluation.blockers
    else:
        assert expected_blocker in evaluation.blockers


@pytest.mark.parametrize(
    ("as_of", "expected_blocker"),
    [
        (None, "live_canary_broker_snapshot_as_of_missing"),
        ("not-a-timestamp", "live_canary_broker_snapshot_as_of_unparseable"),
        ("2026-03-23T10:25:00-04:00", "live_canary_broker_snapshot_stale:1200:900"),
    ],
)
def test_live_canary_blocks_stale_or_invalid_broker_snapshot_for_live_submit(
    as_of: str | None,
    expected_blocker: str,
) -> None:
    plan = _build_plan(
        _signal_payload(date="2026-03-20"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of=as_of,
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=True,
        arm_live_canary="5WT00001",
        timestamp=_timestamp("2026-03-23T10:45:00-04:00"),
    )

    assert evaluation.decision == "blocked"
    assert expected_blocker in evaluation.blockers


def test_live_canary_dry_run_warns_but_does_not_hard_block_submit_time_readiness_gates() -> None:
    plan = _build_plan(
        _signal_payload(date="2026-03-18"),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of=None,
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
        timestamp=_timestamp("2026-03-23T08:00:00-04:00"),
    )

    assert evaluation.decision == "dry_run_ready"
    assert evaluation.blockers == []
    assert "live_canary_submit_outside_regular_session" in evaluation.warnings
    assert "live_canary_signal_date_mismatch:2026-03-18:2026-03-20" in evaluation.warnings
    assert "live_canary_broker_snapshot_as_of_missing" in evaluation.warnings


@pytest.mark.parametrize(
    ("signal_date", "timestamp", "as_of", "expected_warning"),
    [
        (
            "2026-03-23",
            "2026-03-23T10:45:00-04:00",
            "2026-03-23T10:40:00-04:00",
            "live_canary_signal_date_mismatch:2026-03-23:2026-03-20",
        ),
        (
            "2026-03-24",
            "2026-03-23T10:45:00-04:00",
            "2026-03-23T10:40:00-04:00",
            "live_canary_signal_date_mismatch:2026-03-24:2026-03-20",
        ),
        (
            "2026-07-02",
            "2026-07-03T10:45:00-04:00",
            "2026-07-03T10:40:00-04:00",
            "live_canary_submit_market_holiday:2026-07-03",
        ),
        (
            "2026-11-27",
            "2026-11-27T13:05:00-05:00",
            "2026-11-27T12:55:00-05:00",
            "live_canary_submit_outside_regular_session",
        ),
        (
            "2027-12-30",
            "2027-12-31T10:45:00-05:00",
            "2027-12-31T10:40:00-05:00",
            "live_canary_submit_market_holiday:2027-12-31",
        ),
    ],
    ids=["same_day_signal", "future_signal", "market_holiday", "early_close", "cross_year_new_year"],
)
def test_live_canary_dry_run_preserves_usability_for_signal_mismatch_and_holiday_conditions(
    signal_date: str,
    timestamp: str,
    as_of: str,
    expected_warning: str,
) -> None:
    plan = _build_plan(
        _signal_payload(date=signal_date),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
        as_of=as_of,
    )

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
        timestamp=_timestamp(timestamp),
    )

    assert evaluation.decision == "dry_run_ready"
    assert evaluation.blockers == []
    assert expected_warning in evaluation.warnings


@pytest.mark.parametrize(
    ("signal_payload", "positions"),
    [
        (
            _signal_payload(
                action="ROTATE",
                symbol="EFA",
                target_shares=100,
                resize_prev_shares=None,
                resize_new_shares=None,
            ),
            [{"symbol": "BIL", "shares": 7, "price": 91.20, "instrument_type": "Equity"}],
        ),
        (
            _signal_payload(
                action="EXIT",
                symbol="CASH",
                price=None,
                target_shares=0,
                resize_prev_shares=None,
                resize_new_shares=None,
            ),
            [{"symbol": "BIL", "shares": 7, "price": 91.20, "instrument_type": "Equity"}],
        ),
    ],
)
def test_live_canary_blocks_oversized_existing_positions_before_any_leg_is_built(
    signal_payload: dict[str, object],
    positions: list[dict[str, object]],
) -> None:
    plan = _build_plan(signal_payload, positions=positions)

    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
    )

    assert evaluation.decision == "blocked"
    assert evaluation.orders == []
    assert "live_canary_existing_position_exceeds_cap:BIL:7:1" in evaluation.blockers


def test_live_canary_duplicate_event_state_blocks_repeat_event_for_same_account(tmp_path: Path) -> None:
    record = {
        "account_id": "5WT00001",
        "decision": "live_submitted",
        "event_id": "2026-03-19:dual_mom_vol10_cash:ROTATE:EFA:100::2026-03-31",
        "generated_at_chicago": "2026-03-19T10:00:00-05:00",
        "manual_clearance_required": False,
        "response_text": "submitted",
        "result": "submitted",
    }
    claimed, prior_record, state_path = claim_live_canary_event(
        base_dir=tmp_path,
        account_id="5WT00001",
        event_id=record["event_id"],
        record=record,
    )
    assert claimed is True
    assert prior_record is None
    finalize_live_canary_event(state_path=state_path, record=record)

    claimed, prior_record, second_path = claim_live_canary_event(
        base_dir=tmp_path,
        account_id="5WT00001",
        event_id=record["event_id"],
        record=record,
    )

    assert claimed is False
    assert second_path == state_path
    assert prior_record is not None
    assert prior_record["decision"] == "live_submitted"
    assert prior_record["event_id"] == record["event_id"]


def test_live_canary_audit_writes_expected_fields(tmp_path: Path) -> None:
    plan = _build_plan(
        _signal_payload(),
        positions=[{"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}],
    )
    evaluation = evaluate_live_canary(
        plan=plan,
        live_canary_account="5WT00001",
        live_submit_requested=False,
        arm_live_canary=None,
    )
    audit_path = live_canary_audit_path(tmp_path)
    rows = audit_rows_for_result(
        evaluation=evaluation,
        decision=evaluation.decision,
        duplicate=False,
        response_text="dry-run only",
    )

    append_live_canary_audit(audit_path=audit_path, rows=rows)

    written = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines()]
    assert len(written) == 1
    row = written[0]
    assert row["account"] == "5WT00001"
    assert row["event_id"] == evaluation.signal.event_id
    assert row["action"] == "RESIZE"
    assert row["symbol"] == "EFA"
    assert row["requested_qty"] == 100
    assert row["executable_qty"] == 1
    assert row["decision"] == "dry_run_ready"
    assert row["armed"] is False
    assert row["duplicate"] is False
    assert row["response_text"] == "dry-run only"


def test_live_canary_guardrails_cli_smoke_with_realistic_dual_mom_vol10_cash_payload(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "dual_mom_vol10_cash_signal.json"
    signal_path.write_text(
        json.dumps(
            _signal_payload(
                strategy="dual_mom_vol10_cash",
                action="ROTATE",
                symbol="EFA",
                target_shares=100,
                resize_prev_shares=None,
                resize_new_shares=None,
            )
        ),
        encoding="utf-8",
    )
    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            _broker_snapshot(
                {"symbol": "BIL", "shares": 1, "price": 91.20, "instrument_type": "Equity"},
                account_id="5WT00001",
            )
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "live_canary"

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "live_canary_guardrails.py"),
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--live-canary-account",
            "5WT00001",
            "--base-dir",
            str(base_dir),
            "--emit",
            "json",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=env,
    )

    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["schema_name"] == "live_canary_guardrail_result"
    assert payload["decision"] == "dry_run_ready"
    assert payload["account_id"] == "5WT00001"
    assert payload["event_id"].startswith("2026-03-19:dual_mom_vol10_cash:ROTATE:EFA:")
    assert [order["symbol"] for order in payload["orders"]] == ["BIL", "EFA"]
    assert [order["executable_qty"] for order in payload["orders"]] == [1, 1]
    assert payload["audit_path"] == str(base_dir / "audit.jsonl")

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 2
    assert audit_rows[0]["event_id"] == payload["event_id"]
    assert {row["symbol"] for row in audit_rows} == {"BIL", "EFA"}


def test_live_canary_guardrails_cli_blank_explicit_account_fails_closed_even_if_env_is_set(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    env["TRADING_CODEX_LIVE_CANARY_ACCOUNT"] = "5WT99999"
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")
    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
            )
        ),
        encoding="utf-8",
    )

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "live_canary_guardrails.py"),
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--live-canary-account",
            "",
            "--base-dir",
            str(tmp_path / "live_canary"),
            "--emit",
            "json",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=env,
    )

    assert proc.returncode == 2, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["decision"] == "blocked"
    assert payload["account_id"] is None
    assert payload["blockers"] == ["live_canary_requires_account_binding"]


def test_live_canary_guardrails_cli_live_submit_blocks_stale_broker_snapshot_with_audit(tmp_path: Path) -> None:
    repo_root, env = _repo_root_and_env()
    signal_path = tmp_path / "signal.json"
    signal_path.write_text(json.dumps(_signal_payload(date="2026-03-20")), encoding="utf-8")
    positions_path = tmp_path / "positions.json"
    positions_path.write_text(
        json.dumps(
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:25:00-04:00",
            )
        ),
        encoding="utf-8",
    )
    base_dir = tmp_path / "live_canary"

    proc = subprocess.run(
        [
            sys.executable,
            str(repo_root / "scripts" / "live_canary_guardrails.py"),
            "--signal-json-file",
            str(signal_path),
            "--broker",
            "file",
            "--positions-file",
            str(positions_path),
            "--live-canary-account",
            "5WT00001",
            "--live-submit",
            "--arm-live-canary",
            "5WT00001",
            "--timestamp",
            "2026-03-23T10:45:00-04:00",
            "--base-dir",
            str(base_dir),
            "--emit",
            "json",
        ],
        capture_output=True,
        text=True,
        cwd=str(repo_root),
        env=env,
    )

    assert proc.returncode == 2, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(proc.stdout)
    assert payload["decision"] == "blocked"
    assert payload["blockers"] == ["live_canary_broker_snapshot_stale:1200:900"]
    assert payload["response_text"] == "live_canary_broker_snapshot_stale:1200:900"

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 1
    assert audit_rows[0]["decision"] == "blocked"
    assert audit_rows[0]["response_text"] == "live_canary_broker_snapshot_stale:1200:900"


def test_live_canary_submit_path_reconciles_before_broker_submit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result, payload, base_dir, adapter = _run_live_submit_guardrails(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        snapshots=(
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:40:00-04:00",
            ),
            _broker_snapshot(
                {"symbol": "EFA", "shares": 1, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:44:00-04:00",
            ),
        ),
    )

    assert result == 2
    assert adapter.load_calls == 2
    assert adapter.submit_calls == 0
    assert adapter.events == [("load_snapshot", 1), ("load_snapshot", 2)]
    assert payload["decision"] == "blocked"
    assert payload["live_submission"] is None
    assert "live_canary_pre_submit_decision_changed:ready_live_submit:noop" in payload["blockers"]
    assert payload["response_text"] == "live_canary_pre_submit_decision_changed:ready_live_submit:noop"
    assert payload["pre_submit_reconciliation"]["matched"] is False
    assert payload["pre_submit_reconciliation"]["original_decision"] == "ready_live_submit"
    assert payload["pre_submit_reconciliation"]["refreshed_decision"] == "noop"
    assert payload["pre_submit_reconciliation"]["order_drift"]["missing_orders"] == [
        {
            "cap_applied": True,
            "classification": "BUY",
            "current_broker_shares": 0,
            "desired_canary_shares": 1,
            "desired_signal_shares": 100,
            "estimated_notional": 99.16,
            "executable_qty": 1,
            "reference_price": 99.16,
            "requested_qty": 100,
            "side": "BUY",
            "symbol": "EFA",
        }
    ]

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 1
    assert audit_rows[0]["decision"] == "blocked"
    assert audit_rows[0]["response_text"] == "live_canary_pre_submit_decision_changed:ready_live_submit:noop"
    assert audit_rows[0]["pre_submit_reconciliation"]["order_drift"]["missing_orders"][0]["symbol"] == "EFA"

    event_state_path = Path(payload["event_state_path"])
    state = json.loads(event_state_path.read_text(encoding="utf-8"))
    assert state["decision"] == "blocked"
    assert state["manual_clearance_required"] is True
    assert state["result"] == "claim_pending_manual_clearance_required"
    assert state["pre_submit_reconciliation"]["order_drift"]["missing_orders"][0]["symbol"] == "EFA"


def test_live_canary_submit_path_blocks_when_refreshed_affordability_drifts_lower(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result, payload, base_dir, adapter = _run_live_submit_guardrails(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        snapshots=(
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:40:00-04:00",
                buying_power=200.0,
            ),
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:44:00-04:00",
                buying_power=50.0,
            ),
        ),
    )

    assert result == 2
    assert adapter.load_calls == 2
    assert adapter.submit_calls == 0
    assert payload["decision"] == "blocked"
    assert payload["live_submission"] is None
    assert "live_canary_pre_submit_decision_changed:ready_live_submit:blocked" in payload["blockers"]
    assert (
        "live_canary_pre_submit_blocker:live_canary_buy_order_notional_exceeds_available:EFA:99.16:50.00"
        in payload["blockers"]
    )
    assert payload["pre_submit_reconciliation"]["matched"] is False
    assert payload["pre_submit_reconciliation"]["affordability"]["original"]["available_amount"] == 200.0
    assert payload["pre_submit_reconciliation"]["affordability"]["original"]["status"] == "affordable"
    assert payload["pre_submit_reconciliation"]["affordability"]["refreshed"]["available_amount"] == 50.0
    assert payload["pre_submit_reconciliation"]["affordability"]["refreshed"]["status"] == "insufficient"
    assert payload["pre_submit_reconciliation"]["affordability"]["refreshed"]["source"] == "buying_power"
    assert payload["affordability"]["available_amount"] == 50.0
    assert payload["affordability"]["status"] == "insufficient"

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 1
    assert audit_rows[0]["pre_submit_reconciliation"]["affordability"]["refreshed"]["available_amount"] == 50.0

    event_state_path = Path(payload["event_state_path"])
    state = json.loads(event_state_path.read_text(encoding="utf-8"))
    assert state["decision"] == "blocked"
    assert state["affordability"]["available_amount"] == 50.0
    assert state["pre_submit_reconciliation"]["affordability"]["refreshed"]["available_amount"] == 50.0


def test_live_canary_submit_path_submits_once_after_matched_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fingerprint = "live-fingerprint-success"
    durable_state = {
        "claim_path": str(tmp_path / "live_canary" / "claims" / f"{fingerprint}.json"),
        "ledger_path": str(tmp_path / "live_canary" / "broker_live_submission_fingerprints.jsonl"),
        "lock_path": str(tmp_path / "live_canary" / "live_submission_state.lock"),
        "state_dir": str(tmp_path / "live_canary"),
    }
    result, payload, base_dir, adapter = _run_live_submit_guardrails(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        snapshots=(
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:40:00-04:00",
            ),
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:44:00-04:00",
            ),
        ),
        submit_result=_live_submission_response(
            attempted=True,
            succeeded=True,
            submission_result="submitted",
            live_submission_fingerprint=fingerprint,
            durable_state=durable_state,
        ),
    )

    assert result == 0
    assert adapter.load_calls == 2
    assert adapter.submit_calls == 1
    assert adapter.events == [
        ("load_snapshot", 1),
        ("load_snapshot", 2),
        ("submit_live_orders", 2),
    ]
    assert payload["decision"] == "live_submitted"
    assert payload["response_text"] == "submitted"
    assert payload["live_submission"]["live_submit_attempted"] is True
    assert payload["live_submission"]["submission_succeeded"] is True
    assert payload["live_submission"]["live_submission_fingerprint"] == fingerprint
    assert payload["live_submission"]["durable_state"] == durable_state
    assert payload["pre_submit_reconciliation"]["blockers"] == []
    assert payload["pre_submit_reconciliation"]["matched"] is True
    assert payload["pre_submit_reconciliation"]["original_decision"] == "ready_live_submit"
    assert payload["pre_submit_reconciliation"]["refreshed_decision"] == "ready_live_submit"
    assert payload["pre_submit_reconciliation"]["affordability"]["original"]["status"] == "affordable"
    assert payload["pre_submit_reconciliation"]["affordability"]["refreshed"]["status"] == "affordable"
    assert payload["affordability"]["status"] == "affordable"

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 1
    assert audit_rows[0]["decision"] == "live_submitted"
    assert audit_rows[0]["response_text"] == "submitted"
    assert audit_rows[0]["live_submission"] == payload["live_submission"]
    assert audit_rows[0]["pre_submit_reconciliation"]["matched"] is True

    event_state_path = Path(payload["event_state_path"])
    state = json.loads(event_state_path.read_text(encoding="utf-8"))
    assert state["decision"] == "live_submitted"
    assert state["manual_clearance_required"] is False
    assert state["result"] == "submitted"
    assert state["live_submission"] == payload["live_submission"]
    assert state["pre_submit_reconciliation"]["matched"] is True


def test_live_canary_submit_path_persists_duplicate_refusal_provenance_after_matched_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    fingerprint = "live-fingerprint-duplicate"
    durable_state = {
        "claim_path": str(tmp_path / "live_canary" / "claims" / f"{fingerprint}.json"),
        "ledger_path": str(tmp_path / "live_canary" / "broker_live_submission_fingerprints.jsonl"),
        "lock_path": str(tmp_path / "live_canary" / "live_submission_state.lock"),
        "state_dir": str(tmp_path / "live_canary"),
    }
    duplicate_submit_refusal = {
        "durable_state": durable_state,
        "ledger_path": durable_state["ledger_path"],
        "live_submission_fingerprint": fingerprint,
        "prior_record": {
            "artifact_path": None,
            "generated_at_chicago": "2026-03-23T10:44:30-04:00",
            "live_submission_fingerprint": fingerprint,
            "manual_clearance_required": True,
            "plan_sha256": "plan-sha-123",
            "refusal_reasons": [],
            "result": "ambiguous_attempted_submit_manual_clearance_required",
            "submission_succeeded": False,
        },
    }
    result, payload, base_dir, adapter = _run_live_submit_guardrails(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        snapshots=(
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:40:00-04:00",
            ),
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:44:00-04:00",
            ),
        ),
        submit_result=_live_submission_response(
            attempted=False,
            succeeded=False,
            refusal_reasons=["live_submit_duplicate_fingerprint"],
            submission_result="refused_duplicate",
            order_error="live_submit_duplicate_fingerprint",
            live_submission_fingerprint=fingerprint,
            duplicate_submit_refusal=duplicate_submit_refusal,
            durable_state=durable_state,
        ),
    )

    assert result == 2
    assert adapter.submit_calls == 1
    assert payload["decision"] == "live_submit_refused"
    assert payload["response_text"] == "live_submit_duplicate_fingerprint"
    assert payload["live_submission"]["submission_succeeded"] is False
    assert payload["live_submission"]["live_submission_fingerprint"] == fingerprint
    assert payload["live_submission"]["durable_state"] == durable_state
    assert payload["live_submission"]["duplicate_submit_refusal"] == duplicate_submit_refusal
    assert payload["pre_submit_reconciliation"]["matched"] is True

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 1
    assert audit_rows[0]["decision"] == "live_submit_refused"
    assert audit_rows[0]["live_submission"] == payload["live_submission"]
    assert audit_rows[0]["pre_submit_reconciliation"]["matched"] is True

    event_state_path = Path(payload["event_state_path"])
    state = json.loads(event_state_path.read_text(encoding="utf-8"))
    assert state["decision"] == "live_submit_refused"
    assert state["manual_clearance_required"] is True
    assert state["result"] == "refused_duplicate"
    assert state["live_submission"] == payload["live_submission"]
    assert state["pre_submit_reconciliation"]["matched"] is True


def test_live_canary_submit_path_fails_closed_on_submit_error_after_matched_reconciliation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    result, payload, base_dir, adapter = _run_live_submit_guardrails(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        snapshots=(
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:40:00-04:00",
            ),
            _broker_snapshot(
                {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
                account_id="5WT00001",
                as_of="2026-03-23T10:44:00-04:00",
            ),
        ),
        submit_error=RuntimeError("simulated live submit failure"),
    )

    assert result == 2
    assert adapter.submit_calls == 1
    assert payload["decision"] == "live_submit_error"
    assert payload["response_text"] == "simulated live submit failure"
    assert payload["live_submission"] is None
    assert payload["submit_error"] == {
        "exception_type": "RuntimeError",
        "message": "simulated live submit failure",
        "stage": "submit_live_orders",
    }
    assert payload["pre_submit_reconciliation"]["matched"] is True

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 1
    assert audit_rows[0]["decision"] == "live_submit_error"
    assert audit_rows[0]["response_text"] == "simulated live submit failure"
    assert audit_rows[0]["submit_error"] == payload["submit_error"]
    assert audit_rows[0]["pre_submit_reconciliation"]["matched"] is True

    event_state_path = Path(payload["event_state_path"])
    state = json.loads(event_state_path.read_text(encoding="utf-8"))
    assert state["decision"] == "live_submit_error"
    assert state["manual_clearance_required"] is True
    assert state["result"] == "claim_pending_manual_clearance_required"
    assert state["submit_error"] == payload["submit_error"]
    assert state["pre_submit_reconciliation"]["matched"] is True


@pytest.mark.parametrize(
    ("stage", "error_message"),
    [
        ("refreshed_snapshot", "refreshed snapshot failed"),
        ("refreshed_plan_build", "refreshed plan build failed"),
        ("refreshed_evaluation", "refreshed evaluation failed"),
    ],
)
def test_live_canary_submit_path_blocks_fail_closed_when_reconciliation_stage_raises(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    stage: str,
    error_message: str,
) -> None:
    snapshots = (
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            account_id="5WT00001",
            as_of="2026-03-23T10:40:00-04:00",
        ),
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            account_id="5WT00001",
            as_of="2026-03-23T10:44:00-04:00",
        ),
    )

    if stage == "refreshed_plan_build":
        original_build_execution_plan = live_canary_guardrails.build_execution_plan
        build_calls = {"count": 0}

        def build_execution_plan_with_reconciliation_error(*args: object, **kwargs: object):
            build_calls["count"] += 1
            if build_calls["count"] == 2:
                raise RuntimeError(error_message)
            return original_build_execution_plan(*args, **kwargs)

        monkeypatch.setattr(
            live_canary_guardrails,
            "build_execution_plan",
            build_execution_plan_with_reconciliation_error,
        )
    elif stage == "refreshed_evaluation":
        original_evaluate_live_canary = live_canary_guardrails.evaluate_live_canary
        evaluation_calls = {"count": 0}

        def evaluate_live_canary_with_reconciliation_error(*args: object, **kwargs: object):
            evaluation_calls["count"] += 1
            if evaluation_calls["count"] == 2:
                raise RuntimeError(error_message)
            return original_evaluate_live_canary(*args, **kwargs)

        monkeypatch.setattr(
            live_canary_guardrails,
            "evaluate_live_canary",
            evaluate_live_canary_with_reconciliation_error,
        )

    result, payload, base_dir, adapter = _run_live_submit_guardrails(
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
        capsys=capsys,
        snapshots=snapshots,
        snapshot_errors={2: RuntimeError(error_message)} if stage == "refreshed_snapshot" else None,
    )

    blocker = f"live_canary_pre_submit_reconciliation_error:{error_message}"
    assert result == 2
    assert adapter.submit_calls == 0
    assert payload["decision"] == "blocked"
    assert payload["live_submission"] is None
    assert payload["blockers"] == [blocker]
    assert payload["response_text"] == blocker
    assert payload["pre_submit_reconciliation"]["matched"] is False
    assert payload["pre_submit_reconciliation"]["original_decision"] == "ready_live_submit"
    assert payload["pre_submit_reconciliation"]["refreshed_decision"] == "blocked"
    assert payload["pre_submit_reconciliation"]["error"] == {
        "exception_type": "RuntimeError",
        "message": error_message,
        "stage": stage,
    }

    audit_rows = [json.loads(line) for line in (base_dir / "audit.jsonl").read_text(encoding="utf-8").splitlines()]
    assert len(audit_rows) == 1
    assert audit_rows[0]["decision"] == "blocked"
    assert audit_rows[0]["response_text"] == blocker
    assert audit_rows[0]["pre_submit_reconciliation"]["error"]["stage"] == stage

    event_state_path = Path(payload["event_state_path"])
    state = json.loads(event_state_path.read_text(encoding="utf-8"))
    assert state["decision"] == "blocked"
    assert state["manual_clearance_required"] is True
    assert state["result"] == "claim_pending_manual_clearance_required"
    assert state["pre_submit_reconciliation"]["error"]["stage"] == stage
