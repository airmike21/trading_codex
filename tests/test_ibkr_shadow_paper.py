from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import ibkr_shadow_paper
from trading_codex.execution.ibkr_shadow_paper import (
    DEFAULT_IBKR_SHADOW_HOST,
    DEFAULT_IBKR_SHADOW_PORT,
    IbkrShadowConfig,
    IbkrShadowSnapshot,
    _resolve_shadow_account,
    build_ibkr_shadow_report,
    load_ibkr_shadow_config,
    render_ibkr_shadow_text,
)
from trading_codex.execution.models import BrokerPosition, BrokerSnapshot


TIMESTAMP = "2026-04-02T09:15:00-05:00"
ACCOUNT_ID = "DU1234567"
STRATEGY = "dual_mom_vol10_cash"
ALLOWED_SYMBOLS = {"SPY", "QQQ", "IWM", "EFA", "BIL"}


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
    action: str,
    symbol: str,
    price: float | None,
    target_shares: int,
    resize_prev_shares: int | None = None,
    resize_new_shares: int | None = None,
    next_rebalance: str | None = "2026-04-30",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "schema_version": 1,
        "schema_minor": 0,
        "date": "2026-04-02",
        "strategy": STRATEGY,
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


class FakeShadowClient:
    def __init__(self, snapshot: IbkrShadowSnapshot) -> None:
        self.snapshot = snapshot
        self.calls: list[tuple[str, str, int, int]] = []

    def load_shadow_snapshot(
        self,
        *,
        config: IbkrShadowConfig,
        timestamp,
    ) -> IbkrShadowSnapshot:
        self.calls.append((config.host, config.account_id or "-", config.port, config.client_id))
        return self.snapshot


def _snapshot(*, positions: dict[str, int], cash: float = 10_000.0, buying_power: float = 10_000.0, net_liquidation: float = 10_000.0) -> IbkrShadowSnapshot:
    return _snapshot_with_metadata(
        positions=positions,
        cash=cash,
        buying_power=buying_power,
        net_liquidation=net_liquidation,
    )


def _snapshot_with_metadata(
    *,
    positions: dict[str, int],
    instrument_types: dict[str, str] | None = None,
    underlying_symbols: dict[str, str] | None = None,
    cash: float = 10_000.0,
    buying_power: float = 10_000.0,
    net_liquidation: float = 10_000.0,
) -> IbkrShadowSnapshot:
    broker_positions = {
        symbol: BrokerPosition(
            symbol=symbol,
            shares=shares,
            price=100.0,
            instrument_type=(instrument_types or {}).get(symbol, "Equity"),
            underlying_symbol=(underlying_symbols or {}).get(symbol, symbol),
            raw={"symbol": symbol, "shares": shares},
        )
        for symbol, shares in positions.items()
    }
    broker_snapshot = BrokerSnapshot(
        broker_name="ibkr_tws_paper_shadow",
        account_id=ACCOUNT_ID,
        as_of=TIMESTAMP,
        cash=cash,
        buying_power=buying_power,
        positions=broker_positions,
        raw={},
    )
    return IbkrShadowSnapshot(
        broker_snapshot=broker_snapshot,
        available_accounts=(ACCOUNT_ID,),
        resolved_account_id=ACCOUNT_ID,
        net_liquidation=net_liquidation,
        cash=cash,
        buying_power=buying_power,
        raw_positions=[
            {
                "account": ACCOUNT_ID,
                "symbol": symbol,
                "position": float(shares),
                "secType": "OPT" if (instrument_types or {}).get(symbol, "Equity").lower() == "option" else "STK",
                "avgCost": 100.0,
            }
            for symbol, shares in positions.items()
        ],
        raw_account_summary=[
            {"account": ACCOUNT_ID, "tag": "NetLiquidation", "value": str(net_liquidation), "currency": "USD"},
            {"account": ACCOUNT_ID, "tag": "TotalCashValue", "value": str(cash), "currency": "USD"},
            {"account": ACCOUNT_ID, "tag": "BuyingPower", "value": str(buying_power), "currency": "USD"},
        ],
        warnings=[],
    )


def test_load_ibkr_shadow_config_refuses_non_paper_port() -> None:
    with pytest.raises(ValueError, match="paper TWS port 7497"):
        load_ibkr_shadow_config(port=7496)


def test_load_ibkr_shadow_config_refuses_non_du_account() -> None:
    with pytest.raises(ValueError, match="explicit paper DU account ids"):
        load_ibkr_shadow_config(account_id="U123456")


def test_resolve_shadow_account_rejects_only_live_account() -> None:
    with pytest.raises(ValueError, match="not a paper DU account"):
        _resolve_shadow_account(
            configured_account_id=None,
            available_accounts=("U123456",),
            raw_positions=[],
            raw_account_summary=[],
        )


def test_resolve_shadow_account_rejects_multiple_du_accounts_without_config() -> None:
    with pytest.raises(ValueError, match="Multiple paper accounts were reported by TWS"):
        _resolve_shadow_account(
            configured_account_id=None,
            available_accounts=("DU111111", "DU222222"),
            raw_positions=[],
            raw_account_summary=[],
        )


def test_resolve_shadow_account_rejects_configured_du_missing_from_tws_list() -> None:
    with pytest.raises(ValueError, match="Configured paper account 'DU999999' was not reported by TWS"):
        _resolve_shadow_account(
            configured_account_id="DU999999",
            available_accounts=("DU111111",),
            raw_positions=[],
            raw_account_summary=[],
        )


def test_build_ibkr_shadow_report_maps_buy_order_shape_without_submit() -> None:
    client = FakeShadowClient(_snapshot(positions={}))
    config = IbkrShadowConfig(host=DEFAULT_IBKR_SHADOW_HOST, port=DEFAULT_IBKR_SHADOW_PORT, client_id=7601, account_id=ACCOUNT_ID)
    signal = _signal_payload(action="ENTER", symbol="SPY", price=100.0, target_shares=100)

    payload = build_ibkr_shadow_report(
        client=client,
        config=config,
        allowed_symbols=ALLOWED_SYMBOLS,
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_buy",
        source_ref=None,
        data_dir=None,
        timestamp=TIMESTAMP,
    )

    assert payload["simulation_only"] is True
    assert payload["no_submit"] is True
    assert payload["action_state"] == "actionable"
    assert payload["has_blockers"] is False
    assert payload["has_drift"] is True
    assert payload["is_noop"] is False
    assert payload["decision_summary"] == "would BUY 100 SPY"
    assert payload["proposed_order_count"] == 1
    assert payload["managed_symbol_count"] == len(ALLOWED_SYMBOLS)
    assert payload["broker_position_symbol_count"] == 0
    assert payload["sizing_mode"] == "account_capital"
    assert payload["signal_target"] == {
        "action": "ENTER",
        "desired_target_shares": 100,
        "event_id": signal["event_id"],
        "next_rebalance": "2026-04-30",
        "raw_target_shares": 100,
        "resize_new_shares": None,
        "symbol": "SPY",
    }
    assert payload["reconciliation_summary"] == {
        "action_state": "actionable",
        "actionable_symbol_count": 1,
        "blocked_symbol_count": 0,
        "blocker_count": 0,
        "broker_position_symbol_count": 0,
        "drift_symbol_count": 1,
        "has_blockers": False,
        "has_drift": True,
        "is_noop": False,
        "managed_symbol_count": len(ALLOWED_SYMBOLS),
        "noop_symbol_count": 0,
        "proposed_order_count": 1,
    }
    assert payload["reconciliation_items"] == [
        {
            "action": "BUY",
            "blockers": [],
            "broker_current_position": 0,
            "current_position": 0,
            "delta_to_target": 100,
            "estimated_notional": 10000.0,
            "has_blockers": False,
            "has_drift": True,
            "is_actionable": True,
            "is_noop": False,
            "reconciliation_status": "actionable",
            "reference_price": 100.0,
            "signal_target_shares": 100,
            "symbol": "SPY",
            "target_shares": 100,
            "warnings": [],
        }
    ]
    assert payload["proposed_orders"] == [
        {
            "action": "BUY",
            "classification": "BUY",
            "current_position": 0,
            "delta_to_target": 100,
            "endpoint_used": {"client_id": 7601, "host": DEFAULT_IBKR_SHADOW_HOST, "port": DEFAULT_IBKR_SHADOW_PORT},
            "estimated_notional": 10000.0,
            "intended_ibkr_order_shape": {
                "account": ACCOUNT_ID,
                "contract": {
                    "currency": "USD",
                    "exchange": "SMART",
                    "secType": "STK",
                    "symbol": "SPY",
                },
                "order": {
                    "action": "BUY",
                    "orderType": "MKT",
                    "outsideRth": False,
                    "timeInForce": "DAY",
                    "totalQuantity": 100,
                },
                "simulation_only": True,
                "no_submit": True,
            },
            "quantity": 100,
            "reference_price": 100.0,
            "simulation_only": True,
            "no_submit": True,
            "symbol": "SPY",
            "target_shares": 100,
        }
    ]
    assert isinstance(payload["shadow_action_fingerprint"], str)
    assert len(payload["shadow_action_fingerprint"]) == 64
    assert "Run state: actionable" in render_ibkr_shadow_text(payload)
    assert client.calls == [(DEFAULT_IBKR_SHADOW_HOST, ACCOUNT_ID, DEFAULT_IBKR_SHADOW_PORT, 7601)]


def test_build_ibkr_shadow_report_matching_position_is_noop() -> None:
    client = FakeShadowClient(_snapshot(positions={"SPY": 100}))
    config = IbkrShadowConfig(account_id=ACCOUNT_ID)
    signal = _signal_payload(action="ENTER", symbol="SPY", price=100.0, target_shares=100)

    payload = build_ibkr_shadow_report(
        client=client,
        config=config,
        allowed_symbols=ALLOWED_SYMBOLS,
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_noop",
        source_ref=None,
        data_dir=None,
        timestamp=TIMESTAMP,
    )

    assert payload["action_state"] == "no_op"
    assert payload["has_blockers"] is False
    assert payload["has_drift"] is False
    assert payload["is_noop"] is True
    assert payload["decision_summary"] == "HOLD"
    assert payload["proposed_order_count"] == 0
    assert payload["broker_position_symbol_count"] == 1
    assert payload["reconciliation_summary"] == {
        "action_state": "no_op",
        "actionable_symbol_count": 0,
        "blocked_symbol_count": 0,
        "blocker_count": 0,
        "broker_position_symbol_count": 1,
        "drift_symbol_count": 0,
        "has_blockers": False,
        "has_drift": False,
        "is_noop": True,
        "managed_symbol_count": len(ALLOWED_SYMBOLS),
        "noop_symbol_count": 1,
        "proposed_order_count": 0,
    }
    assert payload["reconciliation_items"][0]["reconciliation_status"] == "no_op"
    assert payload["reconciliation_items"][0]["signal_target_shares"] == 100
    assert payload["reconciliation_items"][0]["broker_current_position"] == 100
    assert payload["reconciliation_items"][0]["delta_to_target"] == 0
    text = render_ibkr_shadow_text(payload)
    assert "Run state: no-op" in text
    assert "Reconciliation: SPY no-op (target 100, current 100, delta +0)" in text
    assert "Proposed orders: none" in text


def test_build_ibkr_shadow_report_cash_signal_emits_sell_shadow_order() -> None:
    client = FakeShadowClient(_snapshot(positions={"SPY": 37}))
    config = IbkrShadowConfig(account_id=ACCOUNT_ID)
    signal = _signal_payload(action="EXIT", symbol="CASH", price=None, target_shares=0)

    payload = build_ibkr_shadow_report(
        client=client,
        config=config,
        allowed_symbols={"SPY", "BIL"},
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_cash_exit",
        source_ref=None,
        data_dir=None,
        timestamp=TIMESTAMP,
    )

    assert payload["decision_summary"] == "would SELL 37 SPY"
    assert payload["action_state"] == "actionable"
    assert payload["has_blockers"] is False
    assert payload["has_drift"] is True
    assert payload["is_noop"] is False
    assert payload["proposed_orders"][0]["action"] == "SELL"
    assert payload["proposed_orders"][0]["quantity"] == 37
    assert payload["reconciliation_items"][0]["symbol"] == "SPY"
    assert payload["reconciliation_items"][0]["reconciliation_status"] == "actionable"
    assert payload["reconciliation_items"][0]["target_shares"] == 0
    assert payload["reconciliation_items"][0]["current_position"] == 37
    assert payload["reconciliation_items"][0]["delta_to_target"] == -37
    assert payload["signal_target"]["symbol"] == "CASH"
    assert payload["signal_target"]["desired_target_shares"] == 0


def test_build_ibkr_shadow_report_blocked_by_managed_unsupported_position() -> None:
    client = FakeShadowClient(
        _snapshot_with_metadata(
            positions={"SPY": 10},
            instrument_types={"SPY": "Option"},
            underlying_symbols={"SPY": "SPY"},
        )
    )
    config = IbkrShadowConfig(account_id=ACCOUNT_ID)
    signal = _signal_payload(action="EXIT", symbol="CASH", price=None, target_shares=0)

    payload = build_ibkr_shadow_report(
        client=client,
        config=config,
        allowed_symbols={"SPY", "BIL"},
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_blocked_unsupported",
        source_ref=None,
        data_dir=None,
        timestamp=TIMESTAMP,
    )

    assert payload["action_state"] == "blocked"
    assert payload["has_blockers"] is True
    assert payload["has_drift"] is False
    assert payload["is_noop"] is False
    assert payload["proposed_order_count"] == 0
    assert payload["reconciliation_items"] == []
    assert "managed_unsupported_positions_present" in payload["blockers"]
    assert "managed_unsupported_symbols:SPY" in payload["blockers"]
    assert payload["reconciliation_summary"] == {
        "action_state": "blocked",
        "actionable_symbol_count": 0,
        "blocked_symbol_count": 0,
        "blocker_count": 2,
        "broker_position_symbol_count": 1,
        "drift_symbol_count": 0,
        "has_blockers": True,
        "has_drift": False,
        "is_noop": False,
        "managed_symbol_count": 2,
        "noop_symbol_count": 0,
        "proposed_order_count": 0,
    }
    text = render_ibkr_shadow_text(payload)
    assert "Run state: blocked" in text
    assert "Blockers: managed_unsupported_positions_present, managed_unsupported_symbols:SPY" in text
    assert "Reconciliation: blocked before managed-symbol reconciliation items were produced" in text
    assert "no managed symbol drift detected" not in text


def test_build_ibkr_shadow_report_blocked_run_overrides_actionable_item_state() -> None:
    client = FakeShadowClient(
        _snapshot_with_metadata(
            positions={"QQQ": 5},
            instrument_types={"QQQ": "Option"},
            underlying_symbols={"QQQ": "QQQ"},
        )
    )
    config = IbkrShadowConfig(account_id=ACCOUNT_ID)
    signal = _signal_payload(action="ENTER", symbol="SPY", price=100.0, target_shares=100)

    payload = build_ibkr_shadow_report(
        client=client,
        config=config,
        allowed_symbols=ALLOWED_SYMBOLS,
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_blocked_actionable_item",
        source_ref=None,
        data_dir=None,
        timestamp=TIMESTAMP,
    )

    assert payload["action_state"] == "blocked"
    assert payload["has_blockers"] is True
    assert payload["is_noop"] is False
    assert payload["has_drift"] is True
    assert payload["proposed_order_count"] == 1
    assert payload["reconciliation_summary"]["actionable_symbol_count"] == 0
    assert payload["reconciliation_summary"]["blocked_symbol_count"] == 1
    assert payload["reconciliation_items"][0]["symbol"] == "SPY"
    assert payload["reconciliation_items"][0]["reconciliation_status"] == "blocked"
    assert payload["reconciliation_items"][0]["is_actionable"] is False
    assert payload["reconciliation_items"][0]["has_blockers"] is True
    text = render_ibkr_shadow_text(payload)
    assert "Run state: blocked" in text
    assert "Reconciliation: SPY blocked (target 100, current 0, delta +100)" in text


def test_build_ibkr_shadow_report_fingerprint_is_stable_for_repeated_identical_shadow_actions() -> None:
    config = IbkrShadowConfig(account_id=ACCOUNT_ID)
    signal = _signal_payload(action="ENTER", symbol="SPY", price=100.0, target_shares=100)

    first_payload = build_ibkr_shadow_report(
        client=FakeShadowClient(_snapshot(positions={})),
        config=config,
        allowed_symbols=ALLOWED_SYMBOLS,
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_repeat_first",
        source_ref=None,
        data_dir=None,
        timestamp="2026-04-02T09:15:00-05:00",
    )
    second_payload = build_ibkr_shadow_report(
        client=FakeShadowClient(_snapshot(positions={})),
        config=config,
        allowed_symbols=ALLOWED_SYMBOLS,
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_repeat_second",
        source_ref=None,
        data_dir=None,
        timestamp="2026-04-02T09:20:00-05:00",
    )

    assert first_payload["shadow_action_fingerprint"] == second_payload["shadow_action_fingerprint"]
    assert first_payload["reconciliation_summary"] == second_payload["reconciliation_summary"]
    assert first_payload["proposed_orders"] == second_payload["proposed_orders"]


def test_build_ibkr_shadow_report_fingerprint_differs_between_blocked_and_non_blocked_runs() -> None:
    config = IbkrShadowConfig(account_id=ACCOUNT_ID)
    signal = _signal_payload(action="ENTER", symbol="SPY", price=100.0, target_shares=100)

    non_blocked_payload = build_ibkr_shadow_report(
        client=FakeShadowClient(_snapshot(positions={})),
        config=config,
        allowed_symbols=ALLOWED_SYMBOLS,
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_fingerprint_open",
        source_ref=None,
        data_dir=None,
        timestamp=TIMESTAMP,
    )
    blocked_payload = build_ibkr_shadow_report(
        client=FakeShadowClient(
            _snapshot_with_metadata(
                positions={"QQQ": 5},
                instrument_types={"QQQ": "Option"},
                underlying_symbols={"QQQ": "QQQ"},
            )
        ),
        config=config,
        allowed_symbols=ALLOWED_SYMBOLS,
        signal_raw=signal,
        source_kind="test",
        source_label="shadow_fingerprint_blocked",
        source_ref=None,
        data_dir=None,
        timestamp=TIMESTAMP,
    )

    assert non_blocked_payload["action_state"] == "actionable"
    assert blocked_payload["action_state"] == "blocked"
    assert non_blocked_payload["shadow_action_fingerprint"] != blocked_payload["shadow_action_fingerprint"]


def test_ibkr_shadow_paper_cli_json_archives_and_defaults_no_submit(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    signal_path = tmp_path / "signal.json"
    archive_root = tmp_path / "archive"
    signal_path.write_text(
        json.dumps(_signal_payload(action="ENTER", symbol="SPY", price=100.0, target_shares=100)),
        encoding="utf-8",
    )

    snapshot = _snapshot(positions={})
    client = FakeShadowClient(snapshot)

    def _factory(*, config: IbkrShadowConfig) -> FakeShadowClient:
        assert config.host == DEFAULT_IBKR_SHADOW_HOST
        assert config.port == DEFAULT_IBKR_SHADOW_PORT
        assert config.account_id == ACCOUNT_ID
        return client

    rc = ibkr_shadow_paper.main(
        [
            "--emit",
            "json",
            "--archive-root",
            str(archive_root),
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            "SPY,BIL",
        ],
        client_factory=_factory,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_name"] == "ibkr_paper_shadow_execution"
    assert payload["simulation_only"] is True
    assert payload["no_submit"] is True
    assert payload["paper_endpoint_used"] == f"{DEFAULT_IBKR_SHADOW_HOST}:{DEFAULT_IBKR_SHADOW_PORT}"
    assert payload["action_state"] == "actionable"
    assert payload["has_blockers"] is False
    assert payload["has_drift"] is True
    assert payload["is_noop"] is False
    assert payload["decision_summary"] == "would BUY 100 SPY"
    assert payload["proposed_order_count"] == 1
    manifest_path = Path(payload["archive_manifest_path"])
    assert manifest_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["account_id"] == ACCOUNT_ID
    assert manifest["endpoint_used"] == {
        "client_id": 7601,
        "host": DEFAULT_IBKR_SHADOW_HOST,
        "port": DEFAULT_IBKR_SHADOW_PORT,
    }
    assert manifest["simulation_only"] is True
    assert manifest["no_submit"] is True
    assert manifest["decision_summary"] == "would BUY 100 SPY"
    assert manifest["has_blockers"] is False
    assert manifest["proposed_order_count"] == 1
    assert manifest["has_drift"] is True
    assert manifest["is_noop"] is False
    assert manifest["reconciliation_summary"] == {
        "action_state": "actionable",
        "actionable_symbol_count": 1,
        "blocked_symbol_count": 0,
        "blocker_count": 0,
        "broker_position_symbol_count": 0,
        "drift_symbol_count": 1,
        "has_blockers": False,
        "has_drift": True,
        "is_noop": False,
        "managed_symbol_count": 2,
        "noop_symbol_count": 0,
        "proposed_order_count": 1,
    }
    assert manifest["shadow_action_fingerprint"] == payload["shadow_action_fingerprint"]
    report_path = manifest_path.parent / manifest["artifact_paths"]["shadow_execution_report"]
    archived_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert archived_report["action_state"] == "actionable"
    assert archived_report["proposed_order_count"] == 1
    assert archived_report["shadow_action_fingerprint"] == payload["shadow_action_fingerprint"]
    summary_text_path = manifest_path.parent / manifest["artifact_paths"]["summary_text"]
    summary_text = summary_text_path.read_text(encoding="utf-8")
    assert "Run state: actionable" in summary_text
    assert "Orders: 1 proposed" in summary_text
    assert client.calls == [(DEFAULT_IBKR_SHADOW_HOST, ACCOUNT_ID, DEFAULT_IBKR_SHADOW_PORT, 7601)]
