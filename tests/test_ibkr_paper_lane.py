from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts import ibkr_paper_lane
from trading_codex.execution.ibkr_paper_lane import (
    IbkrPaperClientConfig,
    apply_ibkr_paper_signal,
    build_ibkr_paper_status,
    event_already_applied,
    load_ibkr_paper_state,
    resolve_ibkr_paper_paths,
)


TIMESTAMP = "2026-03-30T16:05:00-05:00"
ACCOUNT_ID = "DU1234567"
STATE_KEY = "primary_live_candidate_v1"
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
    next_rebalance: str | None = "2026-04-20",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "schema_version": 1,
        "schema_minor": 0,
        "date": "2026-03-30",
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


def _summary_payload(
    *,
    net_liquidation: float = 10_000.0,
    cash: float = 10_000.0,
    buying_power: float = 10_000.0,
) -> dict[str, object]:
    return {
        "netliquidation": {"amount": f"{net_liquidation:.2f}"},
        "totalcashvalue": {"amount": f"{cash:.2f}"},
        "buyingpower": {"amount": f"{buying_power:.2f}"},
    }


def _positions_payload(*items: dict[str, object]) -> list[dict[str, object]]:
    return list(items)


class FakeIbkrClient:
    def __init__(
        self,
        *,
        positions: list[dict[str, object]] | None = None,
        summary: dict[str, object] | None = None,
        contracts: dict[str, dict[str, object]] | None = None,
        place_order_responses: list[object] | None = None,
        order_statuses: dict[str, dict[str, object]] | None = None,
        confirm_reply_responses: list[object] | None = None,
    ) -> None:
        self.positions = copy.deepcopy(positions or [])
        self.summary = copy.deepcopy(summary or _summary_payload())
        self.contracts = copy.deepcopy(
            contracts
            or {
                "EFA": {"conid": 1111, "exchange": "SMART", "isUS": True},
                "BIL": {"conid": 2222, "exchange": "SMART", "isUS": True},
                "SPY": {"conid": 3333, "exchange": "SMART", "isUS": True},
            }
        )
        self.place_order_responses = list(place_order_responses or [])
        self.order_statuses = copy.deepcopy(order_statuses or {})
        self.confirm_reply_responses = list(confirm_reply_responses or [])
        self.ensure_calls: list[str] = []
        self.place_order_payloads: list[dict[str, object]] = []
        self.order_status_requests: list[str] = []
        self.contract_requests: list[str] = []
        self.reply_requests: list[str] = []

    def ensure_account_access(self, *, account_id: str) -> dict[str, object]:
        self.ensure_calls.append(account_id)
        return {
            "brokerage_accounts": {"accounts": [account_id], "selectedAccount": account_id},
            "portfolio_accounts": [{"accountId": account_id, "id": account_id}],
        }

    def load_positions(self, *, account_id: str) -> list[dict[str, object]]:
        assert account_id == ACCOUNT_ID
        return copy.deepcopy(self.positions)

    def load_summary(self, *, account_id: str) -> dict[str, object]:
        assert account_id == ACCOUNT_ID
        return copy.deepcopy(self.summary)

    def resolve_stock_contract(self, *, symbol: str) -> dict[str, object]:
        self.contract_requests.append(symbol)
        return copy.deepcopy(self.contracts[symbol])

    def place_order(self, *, account_id: str, payload: dict[str, object]) -> object:
        assert account_id == ACCOUNT_ID
        self.place_order_payloads.append(copy.deepcopy(payload))
        if self.place_order_responses:
            return self.place_order_responses.pop(0)
        raise AssertionError("No fake place_order response configured.")

    def confirm_order_reply(self, *, reply_id: str, confirmed: bool) -> object:
        assert confirmed is True
        self.reply_requests.append(reply_id)
        if self.confirm_reply_responses:
            return self.confirm_reply_responses.pop(0)
        raise AssertionError("No fake confirm_order_reply response configured.")

    def load_order_status(self, *, order_id: str) -> dict[str, object]:
        self.order_status_requests.append(order_id)
        return copy.deepcopy(self.order_statuses[order_id])


def _config() -> IbkrPaperClientConfig:
    return IbkrPaperClientConfig(account_id=ACCOUNT_ID)


def test_ibkr_paper_status_reconciles_latest_signal_and_persists_state(tmp_path: Path) -> None:
    base_dir = tmp_path / "ibkr_paper"
    client = FakeIbkrClient(
        positions=[],
        summary=_summary_payload(),
    )
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)

    payload = build_ibkr_paper_status(
        client=client,
        config=_config(),
        allowed_symbols=ALLOWED_SYMBOLS,
        state_key=STATE_KEY,
        base_dir=base_dir,
        signal_raw=signal,
        source_kind="test",
        source_label="status",
        timestamp=TIMESTAMP,
    )

    assert payload["drift_present"] is True
    assert payload["event_already_applied"] is False
    assert payload["event_claim_pending"] is False
    assert payload["trade_required"] == [
        {
            "classification": "BUY",
            "current_broker_shares": 0,
            "delta_shares": 100,
            "desired_target_shares": 100,
            "estimated_notional": 10000.0,
            "quantity": 100,
            "reference_price": 100.0,
            "side": "BUY",
            "symbol": "EFA",
        }
    ]

    paths = resolve_ibkr_paper_paths(state_key=STATE_KEY, base_dir=base_dir, create=False)
    state = load_ibkr_paper_state(paths)
    assert state.account_id == ACCOUNT_ID
    assert state.strategy == STRATEGY
    assert state.allowed_symbols == tuple(sorted(ALLOWED_SYMBOLS))
    assert state.last_status is not None
    assert state.last_status["event_id"] == signal["event_id"]
    assert Path(payload["archive_manifest_path"]).exists()


def test_ibkr_paper_apply_happy_path_and_duplicate_protection(tmp_path: Path) -> None:
    base_dir = tmp_path / "ibkr_paper"
    client = FakeIbkrClient(
        positions=[],
        summary=_summary_payload(),
        place_order_responses=[[{"order_id": 7001, "order_status": "Submitted"}]],
        order_statuses={
            "7001": {
                "order_id": 7001,
                "order_status": "Filled",
                "symbol": "EFA",
                "side": "BUY",
                "cum_fill": "100",
            }
        },
    )
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)

    first = apply_ibkr_paper_signal(
        client=client,
        config=_config(),
        allowed_symbols=ALLOWED_SYMBOLS,
        state_key=STATE_KEY,
        base_dir=base_dir,
        signal_raw=signal,
        source_kind="test",
        source_label="apply",
        timestamp=TIMESTAMP,
    )
    second = apply_ibkr_paper_signal(
        client=client,
        config=_config(),
        allowed_symbols=ALLOWED_SYMBOLS,
        state_key=STATE_KEY,
        base_dir=base_dir,
        signal_raw=signal,
        source_kind="test",
        source_label="apply",
        timestamp=TIMESTAMP,
    )

    assert first["result"] == "applied"
    assert first["duplicate_event_blocked"] is False
    assert first["submitted_orders"][0]["broker_order_id"] == "7001"
    assert Path(str(first["event_receipt_path"])).exists()
    assert client.place_order_payloads[0]["orders"][0]["cOID"] == f"{signal['event_id']}:1:BUY:EFA"

    assert second["result"] == "duplicate_event_refused"
    assert second["duplicate_event_blocked"] is True
    assert second["submitted_orders"] == []
    assert len(client.place_order_payloads) == 1

    paths = resolve_ibkr_paper_paths(state_key=STATE_KEY, base_dir=base_dir, create=False)
    assert event_already_applied(paths, str(signal["event_id"])) is True
    state = load_ibkr_paper_state(paths)
    assert state.last_attempt is not None
    assert state.last_attempt["result"] == "duplicate_event_refused"
    assert state.last_applied is not None
    assert state.last_applied["result"] == "applied"


def test_ibkr_paper_apply_refuses_unmanaged_positions_plan_blockers(tmp_path: Path) -> None:
    base_dir = tmp_path / "ibkr_paper"
    client = FakeIbkrClient(
        positions=_positions_payload(
            {
                "position": 5,
                "description": "AAPL",
                "ticker": "AAPL",
                "mktPrice": 200.0,
                "mktValue": 1000.0,
                "assetClass": "STK",
                "secType": "STK",
                "timestamp": 1710000000,
            }
        ),
        summary=_summary_payload(net_liquidation=11_000.0, cash=10_000.0, buying_power=11_000.0),
    )
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)

    with pytest.raises(ValueError, match="reconciliation plan is blocked"):
        apply_ibkr_paper_signal(
            client=client,
            config=_config(),
            allowed_symbols=ALLOWED_SYMBOLS,
            state_key=STATE_KEY,
            base_dir=base_dir,
            signal_raw=signal,
            source_kind="test",
            source_label="apply_blocked",
            timestamp=TIMESTAMP,
        )


def test_ibkr_paper_lane_cli_smoke_with_fake_client(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    base_dir = tmp_path / "ibkr_paper"
    signal_path = tmp_path / "signal.json"
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    signal_path.write_text(json.dumps(signal), encoding="utf-8")

    client = FakeIbkrClient(
        positions=[],
        summary=_summary_payload(),
        place_order_responses=[[{"order_id": 9001, "order_status": "Submitted"}]],
        order_statuses={
            "9001": {
                "order_id": 9001,
                "order_status": "Submitted",
                "symbol": "EFA",
                "side": "BUY",
                "cum_fill": "0",
            }
        },
    )

    def _factory(*, config: IbkrPaperClientConfig) -> FakeIbkrClient:
        assert config.account_id == ACCOUNT_ID
        return client

    status_exit = ibkr_paper_lane.main(
        [
            "--emit",
            "json",
            "--base-dir",
            str(base_dir),
            "--ibkr-account-id",
            ACCOUNT_ID,
            "status",
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )
    assert status_exit == 0
    status_stdout = json.loads(capsys.readouterr().out)
    assert status_stdout["schema_name"] == "ibkr_paper_lane_status"
    assert status_stdout["trade_required"][0]["symbol"] == "EFA"

    apply_exit = ibkr_paper_lane.main(
        [
            "--emit",
            "json",
            "--base-dir",
            str(base_dir),
            "--ibkr-account-id",
            ACCOUNT_ID,
            "apply",
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )
    assert apply_exit == 0
    apply_stdout = json.loads(capsys.readouterr().out)
    assert apply_stdout["schema_name"] == "ibkr_paper_lane_apply_result"
    assert apply_stdout["result"] == "applied"
    assert apply_stdout["submitted_orders"][0]["broker_order_id"] == "9001"
