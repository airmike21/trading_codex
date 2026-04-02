from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts import ibkr_shadow_loop
from trading_codex.execution.ibkr_shadow_loop import (
    apply_ibkr_shadow_loop_change_detection,
    load_ibkr_shadow_loop_state,
    resolve_ibkr_shadow_loop_state_path,
    shadow_action_fingerprint_short,
)
from trading_codex.execution.ibkr_shadow_paper import (
    DEFAULT_IBKR_SHADOW_HOST,
    DEFAULT_IBKR_SHADOW_PORT,
    IbkrShadowConfig,
    IbkrShadowSnapshot,
)
from trading_codex.execution.models import BrokerPosition, BrokerSnapshot


TIMESTAMP = "2026-04-02T09:15:00-05:00"
ACCOUNT_ID = "DU1234567"


def _payload(
    *,
    fingerprint: str,
    action_state: str = "actionable",
    generated_at_chicago: str = TIMESTAMP,
) -> dict[str, object]:
    return {
        "action_state": action_state,
        "archive_manifest_path": "/tmp/archive/manifest.json",
        "decision_summary": "would BUY 100 SPY",
        "generated_at_chicago": generated_at_chicago,
        "no_submit": True,
        "shadow_action_fingerprint": fingerprint,
        "signal": {"event_id": "evt-1"},
        "simulation_only": True,
    }


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
        "schema_version": 1,
        "schema_minor": 0,
        "date": "2026-04-02",
        "strategy": "dual_mom_vol10_cash",
        "action": "ENTER",
        "symbol": "SPY",
        "price": 100.0,
        "target_shares": 100,
        "resize_prev_shares": None,
        "resize_new_shares": None,
        "next_rebalance": "2026-04-30",
    }
    payload["event_id"] = _event_id(payload)
    return payload


def _snapshot(*, positions: dict[str, int]) -> IbkrShadowSnapshot:
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
        cash=10_000.0,
        buying_power=10_000.0,
        positions=broker_positions,
        raw={},
    )
    return IbkrShadowSnapshot(
        broker_snapshot=broker_snapshot,
        available_accounts=(ACCOUNT_ID,),
        resolved_account_id=ACCOUNT_ID,
        net_liquidation=10_000.0,
        cash=10_000.0,
        buying_power=10_000.0,
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
            {"account": ACCOUNT_ID, "tag": "NetLiquidation", "value": "10000.0", "currency": "USD"},
            {"account": ACCOUNT_ID, "tag": "TotalCashValue", "value": "10000.0", "currency": "USD"},
            {"account": ACCOUNT_ID, "tag": "BuyingPower", "value": "10000.0", "currency": "USD"},
        ],
        warnings=[],
    )


class FakeShadowClient:
    def __init__(self, snapshot: IbkrShadowSnapshot) -> None:
        self.snapshot = snapshot

    def load_shadow_snapshot(
        self,
        *,
        config: IbkrShadowConfig,
        timestamp,
    ) -> IbkrShadowSnapshot:
        assert config.host == DEFAULT_IBKR_SHADOW_HOST
        assert config.port == DEFAULT_IBKR_SHADOW_PORT
        assert config.account_id == ACCOUNT_ID
        assert timestamp.isoformat()
        return self.snapshot


def test_first_run_persists_state_and_reports_first_seen(tmp_path: Path) -> None:
    state_path = tmp_path / "shadow_state.json"

    result = apply_ibkr_shadow_loop_change_detection(
        payload=_payload(fingerprint="a" * 64),
        state_key="shadow-test",
        state_path=state_path,
    )

    assert result["run_state"] == "actionable"
    assert result["change_status"] == "first_seen"
    assert result["shadow_action_fingerprint_short"] == "a" * 12
    state = load_ibkr_shadow_loop_state(state_path)
    assert state is not None
    assert state["state_key"] == "shadow-test"
    assert state["last_change_status"] == "first_seen"
    assert state["last_shadow_action_fingerprint"] == "a" * 64
    assert state["last_archive_manifest_path"] == "/tmp/archive/manifest.json"


def test_repeated_identical_run_reports_unchanged(tmp_path: Path) -> None:
    state_path = tmp_path / "shadow_state.json"
    apply_ibkr_shadow_loop_change_detection(
        payload=_payload(fingerprint="b" * 64),
        state_key="shadow-test",
        state_path=state_path,
    )

    result = apply_ibkr_shadow_loop_change_detection(
        payload=_payload(
            fingerprint="b" * 64,
            generated_at_chicago="2026-04-02T09:20:00-05:00",
        ),
        state_key="shadow-test",
        state_path=state_path,
    )

    assert result["change_status"] == "unchanged"
    state = load_ibkr_shadow_loop_state(state_path)
    assert state is not None
    assert state["last_change_status"] == "unchanged"
    assert state["last_shadow_action_fingerprint_short"] == "b" * 12


def test_changed_fingerprint_reports_changed(tmp_path: Path) -> None:
    state_path = tmp_path / "shadow_state.json"
    apply_ibkr_shadow_loop_change_detection(
        payload=_payload(fingerprint="c" * 64),
        state_key="shadow-test",
        state_path=state_path,
    )

    result = apply_ibkr_shadow_loop_change_detection(
        payload=_payload(
            fingerprint="d" * 64,
            action_state="no_op",
            generated_at_chicago="2026-04-02T09:25:00-05:00",
        ),
        state_key="shadow-test",
        state_path=state_path,
    )

    assert result["run_state"] == "no_op"
    assert result["change_status"] == "changed"
    state = load_ibkr_shadow_loop_state(state_path)
    assert state is not None
    assert state["last_run_state"] == "no_op"
    assert state["last_shadow_action_fingerprint"] == "d" * 64


def test_short_fingerprint_is_deterministic() -> None:
    assert shadow_action_fingerprint_short("ABCDEF1234567890fedcba") == "abcdef123456"
    assert shadow_action_fingerprint_short("abcdef1234567890fedcba") == "abcdef123456"


def test_ibkr_shadow_loop_cli_smoke_detects_first_seen_then_unchanged_then_changed(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    signal_path = tmp_path / "signal.json"
    archive_root = tmp_path / "archive"
    state_dir = tmp_path / "state"
    signal_path.write_text(json.dumps(_signal_payload()), encoding="utf-8")

    snapshot_holder = {"snapshot": _snapshot(positions={})}

    def _factory(*, config: IbkrShadowConfig) -> FakeShadowClient:
        assert config.host == DEFAULT_IBKR_SHADOW_HOST
        assert config.port == DEFAULT_IBKR_SHADOW_PORT
        assert config.account_id == ACCOUNT_ID
        return FakeShadowClient(snapshot_holder["snapshot"])

    first_rc = ibkr_shadow_loop.main(
        [
            "--emit",
            "json",
            "--archive-root",
            str(archive_root),
            "--state-dir",
            str(state_dir),
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

    assert first_rc == 0
    first_payload = json.loads(capsys.readouterr().out)
    assert first_payload["run_state"] == "actionable"
    assert first_payload["change_status"] == "first_seen"
    assert first_payload["simulation_only"] is True
    assert first_payload["no_submit"] is True
    first_state_path = resolve_ibkr_shadow_loop_state_path(
        state_key="signal",
        base_dir=state_dir,
        create=False,
    )
    assert Path(first_payload["state_file"]) == first_state_path
    assert first_state_path.exists()
    assert Path(first_payload["archive_manifest_path"]).exists()

    second_rc = ibkr_shadow_loop.main(
        [
            "--emit",
            "json",
            "--archive-root",
            str(archive_root),
            "--state-dir",
            str(state_dir),
            "--timestamp",
            "2026-04-02T09:20:00-05:00",
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            "SPY,BIL",
        ],
        client_factory=_factory,
    )

    assert second_rc == 0
    second_payload = json.loads(capsys.readouterr().out)
    assert second_payload["change_status"] == "unchanged"
    assert second_payload["shadow_action_fingerprint"] == first_payload["shadow_action_fingerprint"]

    snapshot_holder["snapshot"] = _snapshot(positions={"SPY": 100})
    third_rc = ibkr_shadow_loop.main(
        [
            "--emit",
            "json",
            "--archive-root",
            str(archive_root),
            "--state-dir",
            str(state_dir),
            "--timestamp",
            "2026-04-02T09:25:00-05:00",
            "--ibkr-account-id",
            ACCOUNT_ID,
            "--signal-json-file",
            str(signal_path),
            "--allowed-symbols",
            "SPY,BIL",
        ],
        client_factory=_factory,
    )

    assert third_rc == 0
    third_payload = json.loads(capsys.readouterr().out)
    assert third_payload["run_state"] == "no_op"
    assert third_payload["change_status"] == "changed"
    assert third_payload["shadow_action_fingerprint"] != first_payload["shadow_action_fingerprint"]
    assert third_payload["shadow_action_fingerprint_short"] == shadow_action_fingerprint_short(
        third_payload["shadow_action_fingerprint"]
    )
