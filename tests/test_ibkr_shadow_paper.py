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
    build_ibkr_shadow_report,
    load_ibkr_shadow_config,
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
    broker_positions = {
        symbol: BrokerPosition(
            symbol=symbol,
            shares=shares,
            price=100.0,
            instrument_type="Equity",
            underlying_symbol=symbol,
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
                "secType": "STK",
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
    assert payload["decision_summary"] == "would BUY 100 SPY"
    assert payload["sizing_mode"] == "account_capital"
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
    assert client.calls == [(DEFAULT_IBKR_SHADOW_HOST, ACCOUNT_ID, DEFAULT_IBKR_SHADOW_PORT, 7601)]


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
    assert payload["proposed_orders"][0]["action"] == "SELL"
    assert payload["proposed_orders"][0]["quantity"] == 37
    assert payload["reconciliation_items"][0]["symbol"] == "SPY"
    assert payload["reconciliation_items"][0]["target_shares"] == 0
    assert payload["reconciliation_items"][0]["current_position"] == 37
    assert payload["reconciliation_items"][0]["delta_to_target"] == -37


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
    assert payload["decision_summary"] == "would BUY 100 SPY"
    assert Path(payload["archive_manifest_path"]).exists()
    assert client.calls == [(DEFAULT_IBKR_SHADOW_HOST, ACCOUNT_ID, DEFAULT_IBKR_SHADOW_PORT, 7601)]
