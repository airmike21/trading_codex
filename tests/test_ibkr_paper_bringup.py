from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts import daily_signal, ibkr_paper_bringup
from trading_codex.execution.ibkr_paper_lane import IbkrPaperClientConfig


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


def _documented_account_prep(
    *,
    account_id: str = ACCOUNT_ID,
    is_paper: bool,
    selected_account: str | None = None,
) -> dict[str, object]:
    return {
        "brokerage_accounts": {
            "accounts": [account_id],
            "isPaper": is_paper,
            "selectedAccount": selected_account or account_id,
        },
        "portfolio_accounts": [{"accountId": account_id, "id": account_id}],
    }


class FakeIbkrClient:
    def __init__(
        self,
        *,
        account_prep: dict[str, object] | None = None,
        positions: list[dict[str, object]] | None = None,
        summary: dict[str, object] | None = None,
        contracts: dict[str, dict[str, object]] | None = None,
        place_order_responses: list[object] | None = None,
        order_statuses: dict[str, dict[str, object]] | None = None,
        order_status_errors: dict[str, Exception] | None = None,
        confirm_reply_responses: list[object] | None = None,
    ) -> None:
        self.account_prep = copy.deepcopy(account_prep or _documented_account_prep(is_paper=True))
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
        self.order_status_errors = dict(order_status_errors or {})
        self.confirm_reply_responses = list(confirm_reply_responses or [])
        self.ensure_calls: list[str] = []
        self.position_requests: list[str] = []
        self.summary_requests: list[str] = []
        self.place_order_payloads: list[dict[str, object]] = []
        self.order_status_requests: list[str] = []
        self.contract_requests: list[str] = []
        self.reply_requests: list[str] = []

    def ensure_account_access(self, *, account_id: str) -> dict[str, object]:
        self.ensure_calls.append(account_id)
        return copy.deepcopy(self.account_prep)

    def load_positions(self, *, account_id: str) -> list[dict[str, object]]:
        assert account_id == ACCOUNT_ID
        self.position_requests.append(account_id)
        return copy.deepcopy(self.positions)

    def load_summary(self, *, account_id: str) -> dict[str, object]:
        assert account_id == ACCOUNT_ID
        self.summary_requests.append(account_id)
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
        if order_id in self.order_status_errors:
            raise self.order_status_errors[order_id]
        return copy.deepcopy(self.order_statuses[order_id])


def _write_signal(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_ibkr_paper_bringup_preflight_no_write_success_and_archives_report(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base_dir = tmp_path / "ibkr_paper"
    archive_root = tmp_path / "archive"
    signal_path = tmp_path / "signal.json"
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    _write_signal(signal_path, signal)

    client = FakeIbkrClient(positions=[], summary=_summary_payload())

    def _factory(*, config: IbkrPaperClientConfig) -> FakeIbkrClient:
        assert config.account_id == ACCOUNT_ID
        return client

    rc = ibkr_paper_bringup.main(
        [
            "--emit",
            "json",
            "--mode",
            "preflight",
            "--archive-root",
            str(archive_root),
            "--base-dir",
            str(base_dir),
            "--state-key",
            STATE_KEY,
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_name"] == "ibkr_paper_bringup_acceptance"
    assert payload["requested_mode"] == "preflight"
    assert payload["execution_mode"] == "no_write"
    assert payload["write_enabled"] is False
    assert payload["overall_status"] == "ok"
    assert payload["lane_reachable"] is True
    assert payload["paper_account_verified"] is True
    assert payload["lane_blocked"] is False
    assert payload["drift_present"] is True
    assert payload["event_already_applied"] is False
    assert payload["event_claim_pending"] is False
    assert payload["apply_result"] is None
    assert payload["status_payload"]["schema_name"] == "ibkr_paper_lane_status"

    manifest_path = Path(payload["archive"]["manifest_path"])
    report_path = Path(payload["archive"]["bringup_report_path"])
    summary_path = Path(payload["archive"]["bringup_summary_path"])
    assert manifest_path.exists()
    assert report_path.exists()
    assert summary_path.exists()

    archived_report = json.loads(report_path.read_text(encoding="utf-8"))
    assert archived_report["schema_name"] == "ibkr_paper_bringup_acceptance"
    assert archived_report["requested_mode"] == "preflight"
    assert archived_report["status_payload"]["signal"]["event_id"] == signal["event_id"]
    assert client.place_order_payloads == []


def test_ibkr_paper_bringup_preflight_from_preset_derives_full_allowed_universe(
    tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    base_dir = tmp_path / "ibkr_paper"
    archive_root = tmp_path / "archive"
    data_dir = tmp_path / "data"
    signal = {
        "schema_name": "next_action",
        "schema_version": 1,
        "schema_minor": 0,
        "date": "2026-03-26",
        "strategy": "dual_mom",
        "action": "HOLD",
        "symbol": "EFA",
        "price": 100.0,
        "target_shares": 105,
        "resize_prev_shares": None,
        "resize_new_shares": None,
        "next_rebalance": "2026-03-31",
    }
    signal["event_id"] = _event_id(signal)
    preset = daily_signal.Preset(
        name="dual_mom_core",
        description="test preset",
        run_backtest_args=[
            "--strategy",
            "dual_mom",
            "--symbols",
            "SPY",
            "QQQ",
            "IWM",
            "EFA",
            "--defensive",
            "BIL",
            "--data-dir",
            str(data_dir),
            "--no-plot",
        ],
    )

    def fake_load_signal_from_preset(
        *,
        repo_root: Path,
        preset_name: str,
        presets_path: Path | None,
        data_dir_override: Path | None = None,
    ):
        assert preset_name == "dual_mom_core"
        assert data_dir_override is None
        return signal, preset, tmp_path / "presets.json"

    monkeypatch.setattr(ibkr_paper_bringup.ibkr_paper_lane, "_load_signal_from_preset", fake_load_signal_from_preset)

    client = FakeIbkrClient(positions=[], summary=_summary_payload())

    def _factory(*, config: IbkrPaperClientConfig) -> FakeIbkrClient:
        assert config.account_id == ACCOUNT_ID
        return client

    rc = ibkr_paper_bringup.main(
        [
            "--emit",
            "json",
            "--mode",
            "preflight",
            "--archive-root",
            str(archive_root),
            "--base-dir",
            str(base_dir),
            "--state-key",
            STATE_KEY,
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--preset",
            "dual_mom_core",
        ],
        client_factory=_factory,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["overall_status"] == "ok"
    assert payload["allowed_symbols"] == ["BIL", "EFA", "IWM", "QQQ", "SPY"]
    assert payload["signal"]["symbol"] == "EFA"
    assert payload["status_payload"]["signal"]["event_id"] == signal["event_id"]


def test_ibkr_paper_bringup_apply_requires_explicit_opt_in_and_succeeds(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base_dir = tmp_path / "ibkr_paper"
    archive_root = tmp_path / "archive"
    signal_path = tmp_path / "signal.json"
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    _write_signal(signal_path, signal)

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

    rc = ibkr_paper_bringup.main(
        [
            "--emit",
            "json",
            "--mode",
            "apply",
            "--enable-ibkr-paper-apply",
            "--archive-root",
            str(archive_root),
            "--base-dir",
            str(base_dir),
            "--state-key",
            STATE_KEY,
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )

    assert rc == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["requested_mode"] == "apply"
    assert payload["execution_mode"] == "write_enabled"
    assert payload["write_enabled"] is True
    assert payload["overall_status"] == "ok"
    assert payload["lane_blocked"] is False
    assert payload["apply_result"] == "applied"
    assert payload["apply_payload"]["result"] == "applied"
    assert payload["apply_payload"]["submitted_orders"][0]["broker_order_id"] == "9001"
    assert client.place_order_payloads[0]["orders"][0]["cOID"] == f"{signal['event_id']}:1:BUY:EFA"


def test_ibkr_paper_bringup_fail_closed_non_paper_verification(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base_dir = tmp_path / "ibkr_paper"
    archive_root = tmp_path / "archive"
    signal_path = tmp_path / "signal.json"
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    _write_signal(signal_path, signal)

    client = FakeIbkrClient(account_prep=_documented_account_prep(is_paper=False))

    def _factory(*, config: IbkrPaperClientConfig) -> FakeIbkrClient:
        assert config.account_id == ACCOUNT_ID
        return client

    rc = ibkr_paper_bringup.main(
        [
            "--emit",
            "json",
            "--mode",
            "preflight",
            "--archive-root",
            str(archive_root),
            "--base-dir",
            str(base_dir),
            "--state-key",
            STATE_KEY,
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )

    assert rc == 2
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert payload["overall_status"] == "blocked"
    assert payload["lane_reachable"] is True
    assert payload["paper_account_verified"] is False
    assert payload["lane_blocked"] is True
    assert "paper_verification_failed" in payload["blocking_reasons"]
    assert "verified as paper" in payload["error"]["message"]
    assert Path(payload["archive"]["manifest_path"]).exists()
    assert client.ensure_calls == [ACCOUNT_ID]
    assert client.position_requests == []
    assert client.summary_requests == []
    assert client.place_order_payloads == []
    assert "acceptance blocked" in captured.err


def test_ibkr_paper_bringup_duplicate_event_state_is_fail_closed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base_dir = tmp_path / "ibkr_paper"
    archive_root = tmp_path / "archive"
    signal_path = tmp_path / "signal.json"
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    _write_signal(signal_path, signal)

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

    def _factory(*, config: IbkrPaperClientConfig) -> FakeIbkrClient:
        assert config.account_id == ACCOUNT_ID
        return client

    first_rc = ibkr_paper_bringup.main(
        [
            "--emit",
            "json",
            "--mode",
            "apply",
            "--enable-ibkr-paper-apply",
            "--archive-root",
            str(archive_root),
            "--base-dir",
            str(base_dir),
            "--state-key",
            STATE_KEY,
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )
    assert first_rc == 0
    capsys.readouterr()

    second_rc = ibkr_paper_bringup.main(
        [
            "--emit",
            "json",
            "--mode",
            "preflight",
            "--archive-root",
            str(archive_root),
            "--base-dir",
            str(base_dir),
            "--state-key",
            STATE_KEY,
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )

    assert second_rc == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["overall_status"] == "blocked"
    assert payload["lane_blocked"] is True
    assert payload["event_already_applied"] is True
    assert "duplicate_event" in payload["blocking_reasons"]
    assert len(client.place_order_payloads) == 1


def test_ibkr_paper_bringup_pending_claim_state_is_fail_closed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    base_dir = tmp_path / "ibkr_paper"
    archive_root = tmp_path / "archive"
    signal_path = tmp_path / "signal.json"
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    _write_signal(signal_path, signal)

    client = FakeIbkrClient(
        positions=[],
        summary=_summary_payload(),
        place_order_responses=[[{"order_id": 7001, "order_status": "Submitted"}]],
        order_status_errors={"7001": RuntimeError("simulated status lookup failure after acknowledgement")},
    )

    def _factory(*, config: IbkrPaperClientConfig) -> FakeIbkrClient:
        assert config.account_id == ACCOUNT_ID
        return client

    rc = ibkr_paper_bringup.main(
        [
            "--emit",
            "json",
            "--mode",
            "apply",
            "--enable-ibkr-paper-apply",
            "--archive-root",
            str(archive_root),
            "--base-dir",
            str(base_dir),
            "--state-key",
            STATE_KEY,
            "--timestamp",
            TIMESTAMP,
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            ",".join(sorted(ALLOWED_SYMBOLS)),
        ],
        client_factory=_factory,
    )

    assert rc == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["overall_status"] == "blocked"
    assert payload["lane_blocked"] is True
    assert payload["event_claim_pending"] is True
    assert payload["apply_result"] == "claim_pending_manual_clearance_required"
    assert "pending_claim" in payload["blocking_reasons"]
    assert payload["apply_payload"]["event_claim_path"] is not None
    assert payload["apply_payload"]["submitted_orders"][0]["broker_order_id"] == "7001"
