from __future__ import annotations

import pytest

from trading_codex.execution import build_execution_plan, parse_broker_snapshot, parse_signal_payload


def _signal_payload(
    *,
    action: str = "HOLD",
    symbol: str = "EFA",
    price: float | None = 99.16,
    target_shares: int = 100,
    resize_new_shares: int | None = None,
    next_rebalance: str = "2026-03-31",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "date": "2026-03-09",
        "strategy": "dual_mom",
        "action": action,
        "symbol": symbol,
        "price": price,
        "target_shares": target_shares,
        "resize_prev_shares": None,
        "resize_new_shares": resize_new_shares,
        "next_rebalance": next_rebalance,
    }
    payload["event_id"] = (
        f"{payload['date']}:{payload['strategy']}:{payload['action']}:{payload['symbol']}:"
        f"{payload['target_shares']}:{payload['resize_new_shares'] or ''}:{payload['next_rebalance']}"
    )
    return payload


def _broker_snapshot(*positions: dict[str, object], buying_power: float | None = None) -> dict[str, object]:
    return {
        "broker_name": "mock",
        "account_id": "paper-1",
        "buying_power": buying_power,
        "positions": list(positions),
    }


def test_execution_plan_exact_match_is_hold() -> None:
    signal = parse_signal_payload(_signal_payload())
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 100, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="exact_match",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert len(plan.items) == 1
    item = plan.items[0]
    assert item.classification == "HOLD"
    assert item.delta_shares == 0
    assert item.estimated_notional == 0.0
    assert plan.blockers == []


def test_execution_plan_symbol_missing_from_broker_positions_is_buy() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(_broker_snapshot())

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="missing_symbol",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert len(plan.items) == 1
    assert plan.items[0].classification == "BUY"
    assert plan.items[0].delta_shares == 100


def test_execution_plan_current_below_target_is_resize_buy() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 82, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="resize_buy",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert plan.items[0].classification == "RESIZE_BUY"
    assert plan.items[0].delta_shares == 18


def test_execution_plan_current_above_target_is_resize_sell() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=80, target_shares=80))
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 100, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="resize_sell",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert plan.items[0].classification == "RESIZE_SELL"
    assert plan.items[0].delta_shares == -20


def test_execution_plan_target_zero_is_exit() -> None:
    signal = parse_signal_payload(_signal_payload(action="EXIT", symbol="CASH", price=None, target_shares=0, next_rebalance="2026-04-30"))
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 100, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="exit",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert plan.items[0].classification == "EXIT"
    assert plan.items[0].delta_shares == -100


def test_execution_plan_rotation_creates_sell_and_buy() -> None:
    signal = parse_signal_payload(_signal_payload(action="ROTATE", symbol="EFA", target_shares=100))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "SPY", "shares": 100, "price": 500.0},
            {"symbol": "EFA", "shares": 0, "price": 99.16},
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="rotate",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    classes = {item.symbol: item.classification for item in plan.items}
    assert classes["SPY"] == "SELL"
    assert classes["EFA"] == "BUY"


def test_parse_signal_payload_rejects_bad_event_id() -> None:
    raw = _signal_payload()
    raw["event_id"] = "broken"
    with pytest.raises(ValueError, match="event_id"):
        parse_signal_payload(raw)


def test_parse_broker_snapshot_rejects_malformed_position() -> None:
    with pytest.raises(ValueError, match="shares"):
        parse_broker_snapshot({"positions": [{"symbol": "EFA"}]})


def test_execution_plan_marks_buying_power_blocker() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16}, buying_power=500.0))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="bp_blocker",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert "buy_notional_exceeds_buying_power" in plan.blockers
