from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pytest

from trading_codex.execution import build_execution_plan, parse_broker_snapshot, parse_signal_payload
from trading_codex.execution.live_canary import (
    DEFAULT_LIVE_CANARY_ALLOWED_SYMBOLS,
    append_live_canary_audit,
    audit_rows_for_result,
    claim_live_canary_event,
    evaluate_live_canary,
    finalize_live_canary_event,
    live_canary_audit_path,
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
    buying_power: float = 20_000.0,
) -> dict[str, object]:
    return {
        "broker_name": "tastytrade",
        "account_id": account_id,
        "as_of": as_of,
        "buying_power": buying_power,
        "positions": list(positions),
    }


def _build_plan(
    signal_payload: dict[str, object],
    *,
    positions: list[dict[str, object]],
    account_id: str = "5WT00001",
    as_of: str | None = None,
) -> object:
    signal = parse_signal_payload(signal_payload)
    broker = parse_broker_snapshot(_broker_snapshot(*positions, account_id=account_id, as_of=as_of))
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
