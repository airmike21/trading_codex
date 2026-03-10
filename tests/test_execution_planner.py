from __future__ import annotations

import csv
from datetime import datetime

import pytest

from trading_codex.execution import (
    build_artifact_paths,
    build_execution_plan,
    build_order_intent_export,
    build_simulated_submission_export,
    execution_plan_to_dict,
    order_intent_export_to_dict,
    parse_broker_snapshot,
    parse_signal_payload,
    render_manual_order_checklist,
    render_markdown,
    simulated_submission_export_to_dict,
    write_manual_ticket_csv,
)


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


def test_execution_plan_sleeve_capital_scales_target_shares_from_capital() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(_broker_snapshot())

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="sleeve_capital",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
        sizing_mode="sleeve_capital",
        capital_input=5_000.0,
    )

    assert plan.sizing.mode == "sleeve_capital"
    assert plan.sizing.usable_capital == 5_000.0
    assert plan.items[0].desired_target_shares == 50
    assert plan.items[0].delta_shares == 50
    assert plan.blockers == []


def test_execution_plan_capital_sizing_blocks_when_target_is_too_small() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(_broker_snapshot())

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="too_small_capital",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
        sizing_mode="sleeve_capital",
        capital_input=50.0,
    )

    assert plan.items == []
    assert plan.blockers == ["capital_sizing_yields_zero_shares"]


def test_execution_plan_capital_sizing_buying_power_blocker_still_applies() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(
        _broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"}, buying_power=1_000.0)
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="capital_bp_blocker",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
        sizing_mode="sleeve_capital",
        capital_input=5_000.0,
    )

    assert plan.items[0].desired_target_shares == 50
    assert "buy_notional_exceeds_buying_power" in plan.blockers


def test_execution_plan_full_account_blocks_unmanaged_derivative_positions() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"},
            {
                "symbol": "XYZ  260417P00050000",
                "underlying_symbol": "XYZ",
                "instrument_type": "Equity Option",
                "shares": -1,
                "price": 1.25,
            },
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="full_account",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        source_kind="signal_json_file",
        source_label="full_account_blocked",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert "unmanaged_positions_present" in plan.blockers
    assert "full_account_scope_blocked_by_unmanaged_positions" in plan.blockers
    assert plan.unmanaged_positions[0].classification_reason == "derivative_position"
    assert [item.symbol for item in plan.items] == ["EFA"]


def test_execution_plan_full_account_blocks_unmanaged_long_equity_without_trade_item() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"},
            {"symbol": "XYZ", "shares": 7, "price": 77.0, "instrument_type": "Equity"},
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="full_account",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        source_kind="signal_json_file",
        source_label="full_account_unmanaged_equity",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert "unmanaged_positions_present" in plan.blockers
    assert "full_account_scope_blocked_by_unmanaged_positions" in plan.blockers
    assert plan.plan_math_scope == "managed_supported_positions_with_full_account_blockers"
    assert plan.unmanaged_positions[0].classification_reason == "outside_managed_universe"
    assert [item.symbol for item in plan.items] == ["EFA"]


def test_execution_plan_managed_sleeve_requires_ack_for_unmanaged_positions() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"},
            {"symbol": "XYZ", "shares": 7, "price": 77.0, "instrument_type": "Equity"},
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        source_kind="signal_json_file",
        source_label="managed_sleeve_needs_ack",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert "ack_unmanaged_holdings_required" in plan.blockers
    assert [item.symbol for item in plan.items] == ["EFA"]
    assert plan.unmanaged_positions[0].classification_reason == "outside_managed_universe"


def test_execution_plan_managed_sleeve_with_ack_computes_math_and_reports_unmanaged() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"},
            {"symbol": "XYZ", "shares": 7, "price": 77.0, "instrument_type": "Equity"},
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="managed_sleeve_ack",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert plan.blockers == []
    assert "unmanaged_positions_acknowledged_for_managed_sleeve" in plan.warnings
    assert [item.symbol for item in plan.items] == ["EFA"]
    assert plan.items[0].classification == "RESIZE_BUY"
    assert plan.items[0].delta_shares == 18
    assert plan.unmanaged_positions[0].symbol == "XYZ"


def test_execution_plan_managed_sleeve_blocks_managed_unsupported_derivative() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {
                "symbol": "EFA  260417C00100000",
                "underlying_symbol": "EFA",
                "instrument_type": "Equity Option",
                "shares": 1,
                "price": 2.75,
            }
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="managed_sleeve_unsupported",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert "managed_unsupported_positions_present" in plan.blockers
    assert plan.managed_unsupported_positions[0].classification_reason == "derivative_position"
    assert [item.symbol for item in plan.items] == ["EFA"]


def test_execution_plan_managed_sleeve_buying_power_blocker_still_applies() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 0, "price": 99.16, "instrument_type": "Equity"},
            buying_power=500.0,
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="managed_sleeve_bp",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    assert "buy_notional_exceeds_buying_power" in plan.blockers


def test_execution_plan_scope_metadata_renders_in_json_and_markdown(tmp_path) -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"},
            {"symbol": "XYZ", "shares": 7, "price": 77.0, "instrument_type": "Equity"},
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="scope_metadata",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )
    artifacts = build_artifact_paths(
        tmp_path / "execution_plans",
        timestamp=datetime.fromisoformat("2026-03-09T12:45:00-05:00"),
        source_label="scope_metadata",
    )

    payload = execution_plan_to_dict(plan)
    markdown = render_markdown(plan, artifacts=artifacts)

    assert payload["account_scope"] == "managed_sleeve"
    assert payload["plan_math_scope"] == "managed_sleeve_only"
    assert payload["sizing"]["mode"] == "signal_target_shares"
    assert payload["managed_symbols_universe"] == ["AAA", "BBB", "BIL", "CCC", "EFA"]
    assert payload["unmanaged_holdings_acknowledged"] is True
    assert payload["managed_supported_positions"][0]["symbol"] == "EFA"
    assert payload["unmanaged_positions"][0]["symbol"] == "XYZ"
    assert "Account scope" in markdown
    assert "Sizing mode" in markdown
    assert "Managed Supported Positions" in markdown
    assert "Unmanaged Positions" in markdown


def test_order_intent_export_clean_buy_item_exports_one_intent() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(_broker_snapshot())

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="intent_buy",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )
    export = build_order_intent_export(plan)
    payload = order_intent_export_to_dict(export)

    assert payload["schema_name"] == "order_intent_export"
    assert len(payload["intents"]) == 1
    assert payload["intents"][0]["side"] == "BUY"
    assert payload["intents"][0]["quantity"] == 100
    assert payload["intents"][0]["symbol"] == "EFA"


def test_order_intent_export_hold_only_exports_zero_intents() -> None:
    signal = parse_signal_payload(_signal_payload())
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 100, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="intent_hold",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    export = build_order_intent_export(plan)

    assert export.intents == []


def test_order_intent_export_refuses_blocked_plan_by_default() -> None:
    signal = parse_signal_payload(_signal_payload(action="ENTER"))
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 0, "price": 99.16}, buying_power=500.0))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="intent_blocked",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    with pytest.raises(ValueError, match="Order intent export refused"):
        build_order_intent_export(plan)


def test_order_intent_export_includes_managed_sleeve_metadata_and_unmanaged_summary() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(
        _broker_snapshot(
            {"symbol": "EFA", "shares": 82, "price": 99.16, "instrument_type": "Equity"},
            {"symbol": "XYZ", "shares": 7, "price": 77.0, "instrument_type": "Equity"},
        )
    )

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        account_scope="managed_sleeve",
        managed_symbols={"AAA", "BBB", "CCC", "BIL", "EFA"},
        ack_unmanaged_holdings=True,
        source_kind="signal_json_file",
        source_label="intent_managed_sleeve",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    payload = order_intent_export_to_dict(build_order_intent_export(plan))

    assert payload["account_scope"] == "managed_sleeve"
    assert payload["sizing"]["mode"] == "signal_target_shares"
    assert payload["plan_math_scope"] == "managed_sleeve_only"
    assert payload["managed_symbols_universe"] == ["AAA", "BBB", "BIL", "CCC", "EFA"]
    assert payload["unmanaged_holdings_acknowledged"] is True
    assert payload["unmanaged_positions_count"] == 1
    assert payload["unmanaged_positions_summary"][0]["symbol"] == "XYZ"
    assert [intent["symbol"] for intent in payload["intents"]] == ["EFA"]


def test_simulated_submission_export_builds_broker_shaped_payloads() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 82, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="simulated_submit",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    simulated = build_simulated_submission_export(build_order_intent_export(plan))
    payload = simulated_submission_export_to_dict(simulated)

    assert payload["schema_name"] == "simulated_submission_export"
    assert payload["broker_name"] == "mock"
    assert payload["account_id"] == "paper-1"
    assert len(payload["orders"]) == 1
    assert payload["orders"][0]["symbol"] == "EFA"
    assert payload["orders"][0]["side"] == "BUY"
    assert payload["orders"][0]["quantity"] == 18
    assert payload["orders"][0]["order_type"] == "MARKET"
    assert payload["orders"][0]["time_in_force"] == "DAY"


def test_manual_order_checklist_renders_from_order_intent_export() -> None:
    signal = parse_signal_payload(_signal_payload(action="RESIZE", resize_new_shares=100))
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 82, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="checklist",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    checklist = render_manual_order_checklist(build_order_intent_export(plan))

    assert "Manual Order Checklist checklist" in checklist
    assert "No orders were placed" in checklist
    assert "BUY 18 EFA" in checklist
    assert "Classification: `RESIZE_BUY`" in checklist


def test_manual_ticket_csv_hold_only_writes_header_only(tmp_path) -> None:
    signal = parse_signal_payload(_signal_payload())
    broker = parse_broker_snapshot(_broker_snapshot({"symbol": "EFA", "shares": 100, "price": 99.16}))

    plan = build_execution_plan(
        signal=signal,
        broker_snapshot=broker,
        source_kind="signal_json_file",
        source_label="csv_hold",
        source_ref="signal.json",
        broker_source_ref="positions.json",
        data_dir=None,
    )

    csv_path = tmp_path / "manual_ticket_export.csv"
    write_manual_ticket_csv(build_order_intent_export(plan), path=csv_path)

    with csv_path.open("r", encoding="utf-8", newline="") as fh:
        rows = list(csv.DictReader(fh))

    assert rows == []
