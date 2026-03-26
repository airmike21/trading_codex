from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from scripts import paper_lane
from trading_codex.data import LocalStore
from trading_codex.execution.paper_lane import (
    apply_paper_lane_signal,
    build_paper_lane_status,
    event_already_applied,
    initialize_paper_lane,
    load_paper_state,
    resolve_paper_lane_paths,
)
from trading_codex.run_archive import recent_runs


TIMESTAMP = "2026-03-20T16:05:00-05:00"
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
    action: str,
    symbol: str,
    price: float | None,
    target_shares: int,
    resize_prev_shares: int | None = None,
    resize_new_shares: int | None = None,
    next_rebalance: str | None = "2026-03-31",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_name": "next_action",
        "schema_version": 1,
        "schema_minor": 0,
        "date": SIGNAL_DATE,
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


def _write_flat_bars(store: LocalStore, symbol: str, *, price: float, periods: int = 40) -> None:
    index = pd.date_range("2026-02-02", periods=periods, freq="B")
    close = pd.Series(price, index=index, dtype=float)
    store.write_bars(
        symbol,
        pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=index,
        ),
    )


def _read_ledger(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            rows.append(json.loads(line))
    return rows


def test_paper_lane_state_init_and_reset_clears_event_receipts(tmp_path: Path) -> None:
    base_dir = tmp_path / "paper"
    paths = resolve_paper_lane_paths(state_key="paper_test", base_dir=base_dir, create=True)

    initialized = initialize_paper_lane(
        state_key="paper_test",
        base_dir=base_dir,
        starting_cash=12_345.67,
        timestamp=TIMESTAMP,
    )
    assert initialized["paper_state"]["cash"] == 12_345.67
    state = load_paper_state(paths)
    assert state.cash == 12_345.67
    assert state.holdings == {}

    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    apply_paper_lane_signal(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=signal,
        source_kind="test",
        source_label="state_reset",
        timestamp=TIMESTAMP,
    )
    assert event_already_applied(paths, str(signal["event_id"])) is True

    reset = initialize_paper_lane(
        state_key="paper_test",
        base_dir=base_dir,
        starting_cash=12_345.67,
        timestamp=TIMESTAMP,
        reset=True,
    )
    assert reset["reset"] is True
    reset_state = load_paper_state(paths)
    assert reset_state.cash == 12_345.67
    assert reset_state.last_applied_event_id is None
    assert event_already_applied(paths, str(signal["event_id"])) is False


def test_duplicate_event_id_protection(tmp_path: Path) -> None:
    base_dir = tmp_path / "paper"
    initialize_paper_lane(state_key="paper_test", base_dir=base_dir, timestamp=TIMESTAMP)
    signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)

    first = apply_paper_lane_signal(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=signal,
        source_kind="test",
        source_label="duplicate",
        timestamp=TIMESTAMP,
    )
    second = apply_paper_lane_signal(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=signal,
        source_kind="test",
        source_label="duplicate",
        timestamp=TIMESTAMP,
    )

    assert first["duplicate_event_blocked"] is False
    assert first["result"] == "applied"
    assert second["duplicate_event_blocked"] is True
    assert second["result"] == "duplicate_event_noop"

    paths = resolve_paper_lane_paths(state_key="paper_test", base_dir=base_dir, create=False)
    state = load_paper_state(paths)
    assert state.cash == 0.0
    assert state.holdings["EFA"].shares == 100

    ledger_rows = _read_ledger(paths.ledger_path)
    assert ledger_rows[-1]["entry_kind"] == "duplicate_refused"
    assert ledger_rows[-1]["event_id"] == signal["event_id"]


def test_apply_enter_resize_rotate_exit_and_status_reconcile(tmp_path: Path) -> None:
    base_dir = tmp_path / "paper"
    data_dir = tmp_path / "data"
    store = LocalStore(base_dir=data_dir)
    _write_flat_bars(store, "EFA", price=100.0)
    _write_flat_bars(store, "BIL", price=50.0)

    initialize_paper_lane(state_key="paper_test", base_dir=base_dir, timestamp=TIMESTAMP)

    enter_signal = _signal_payload(action="ENTER", symbol="EFA", price=100.0, target_shares=100)
    status_before = build_paper_lane_status(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=enter_signal,
        source_kind="test",
        source_label="status_enter",
        data_dir=data_dir,
        timestamp=TIMESTAMP,
    )
    assert status_before["event_already_applied"] is False
    assert status_before["drift_present"] is True
    assert status_before["trade_required"][0]["side"] == "BUY"
    assert status_before["scaled_target_positions"] == {"EFA": 100}

    enter_apply = apply_paper_lane_signal(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=enter_signal,
        source_kind="test",
        source_label="apply_enter",
        data_dir=data_dir,
        timestamp=TIMESTAMP,
    )
    assert enter_apply["result"] == "applied"
    assert enter_apply["fills"] == [
        {
            "classification": "BUY",
            "notional": 10_000.0,
            "price": 100.0,
            "quantity": 100,
            "side": "BUY",
            "symbol": "EFA",
        }
    ]

    resize_signal = _signal_payload(
        action="RESIZE",
        symbol="EFA",
        price=100.0,
        target_shares=50,
        resize_prev_shares=100,
        resize_new_shares=50,
    )
    resize_apply = apply_paper_lane_signal(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=resize_signal,
        source_kind="test",
        source_label="apply_resize",
        data_dir=data_dir,
        timestamp=TIMESTAMP,
    )
    assert resize_apply["fills"][0]["classification"] == "RESIZE_SELL"
    assert resize_apply["paper_positions_after_apply"] == {"EFA": 50}

    rotate_signal = _signal_payload(action="ROTATE", symbol="BIL", price=50.0, target_shares=200)
    rotate_apply = apply_paper_lane_signal(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=rotate_signal,
        source_kind="test",
        source_label="apply_rotate",
        data_dir=data_dir,
        timestamp=TIMESTAMP,
    )
    assert [fill["side"] for fill in rotate_apply["fills"]] == ["SELL", "BUY"]
    assert rotate_apply["paper_positions_after_apply"] == {"BIL": 200}

    exit_signal = _signal_payload(action="EXIT", symbol="CASH", price=None, target_shares=0)
    exit_apply = apply_paper_lane_signal(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=exit_signal,
        source_kind="test",
        source_label="apply_exit",
        data_dir=data_dir,
        timestamp=TIMESTAMP,
    )
    assert exit_apply["fills"] == [
        {
            "classification": "EXIT",
            "notional": 10_000.0,
            "price": 50.0,
            "quantity": 200,
            "side": "SELL",
            "symbol": "BIL",
        }
    ]
    assert exit_apply["paper_positions_after_apply"] == {}
    assert exit_apply["paper_state_after"]["cash"] == 10_000.0

    final_status = build_paper_lane_status(
        state_key="paper_test",
        base_dir=base_dir,
        signal_raw=exit_signal,
        source_kind="test",
        source_label="status_exit",
        data_dir=data_dir,
        timestamp=TIMESTAMP,
    )
    assert final_status["event_already_applied"] is True
    assert final_status["drift_present"] is False
    assert final_status["trade_required"] == []


def test_paper_lane_fail_closed_on_malformed_payload(tmp_path: Path) -> None:
    base_dir = tmp_path / "paper"
    initialize_paper_lane(state_key="paper_test", base_dir=base_dir, timestamp=TIMESTAMP)

    bad_payload = {
        "schema_name": "next_action",
        "date": SIGNAL_DATE,
        "strategy": STRATEGY,
        "action": "ENTER",
        "symbol": "EFA",
        "price": 100.0,
        "target_shares": 100,
        "resize_prev_shares": None,
        "resize_new_shares": None,
        "next_rebalance": "2026-03-31",
        "event_id": "broken",
    }

    with pytest.raises(ValueError, match="event_id"):
        build_paper_lane_status(
            state_key="paper_test",
            base_dir=base_dir,
            signal_raw=bad_payload,
            source_kind="test",
            source_label="bad_payload",
            timestamp=TIMESTAMP,
        )


def _price_series(index: pd.DatetimeIndex, returns: np.ndarray, base: float) -> pd.Series:
    return pd.Series(base * np.cumprod(1.0 + returns.astype(float)), index=index)


def _write_dual_mom_vol10_cash_store(base_dir: Path) -> None:
    periods = 320
    idx = np.arange(periods)
    dates = pd.date_range("2024-01-02", periods=periods, freq="B")
    store = LocalStore(base_dir=base_dir)

    close_map = {
        "SPY": _price_series(dates, np.full(periods, 0.0003), 100.0),
        "QQQ": _price_series(dates, np.where(idx % 2 == 0, 0.0010, -0.0006), 105.0),
        "IWM": _price_series(dates, np.full(periods, -0.0001), 95.0),
        "EFA": _price_series(dates, np.where(idx % 2 == 0, 0.0025, 0.0002), 98.0),
        "BIL": _price_series(dates, np.full(periods, 0.0001), 100.0),
    }

    for symbol, close in close_map.items():
        store.write_bars(
            symbol,
            pd.DataFrame(
                {
                    "open": close,
                    "high": close,
                    "low": close,
                    "close": close,
                    "volume": 1_000,
                },
                index=dates,
            ),
        )


def test_paper_lane_cli_preset_smoke(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    data_dir = tmp_path / "data"
    _write_dual_mom_vol10_cash_store(data_dir)

    presets_path = tmp_path / "presets.json"
    presets_path.write_text(
        json.dumps(
            {
                "presets": {
                    "dual_mom_vol10_cash_core": {
                        "description": "paper lane cli smoke",
                        "run_backtest_args": [
                            "--strategy",
                            "dual_mom_vol10_cash",
                            "--symbols",
                            "SPY",
                            "QQQ",
                            "IWM",
                            "EFA",
                            "--dmv-defensive-symbol",
                            "BIL",
                            "--dmv-mom-lookback",
                            "63",
                            "--dmv-rebalance",
                            "21",
                            "--dmv-vol-lookback",
                            "20",
                            "--dmv-target-vol",
                            "0.10",
                            "--start",
                            "2024-04-01",
                            "--end",
                            "2025-03-20",
                            "--data-dir",
                            str(data_dir),
                            "--no-plot",
                        ],
                    }
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    base_dir = tmp_path / "paper_cli"
    init_exit = paper_lane.main(
        [
            "--emit",
            "json",
            "--base-dir",
            str(base_dir),
            "init",
        ]
    )
    init_out = capsys.readouterr()
    assert init_exit == 0, init_out.err

    status_exit = paper_lane.main(
        [
            "--emit",
            "json",
            "--base-dir",
            str(base_dir),
            "status",
            "--preset",
            "dual_mom_vol10_cash_core",
            "--presets-file",
            str(presets_path),
        ]
    )
    status_out = capsys.readouterr()
    assert status_exit == 0, status_out.err
    status_payload = json.loads(status_out.out)
    assert status_payload["schema_name"] == "paper_lane_status"

    apply_exit = paper_lane.main(
        [
            "--emit",
            "json",
            "--base-dir",
            str(base_dir),
            "apply",
            "--preset",
            "dual_mom_vol10_cash_core",
            "--presets-file",
            str(presets_path),
        ]
    )
    apply_out = capsys.readouterr()
    assert apply_exit == 0, apply_out.err
    apply_payload = json.loads(apply_out.out)
    assert apply_payload["schema_name"] == "paper_lane_apply_result"
    assert apply_payload["result"] == "applied"

    post_status_exit = paper_lane.main(
        [
            "--emit",
            "json",
            "--base-dir",
            str(base_dir),
            "status",
            "--preset",
            "dual_mom_vol10_cash_core",
            "--presets-file",
            str(presets_path),
        ]
    )
    post_status_out = capsys.readouterr()
    assert post_status_exit == 0, post_status_out.err
    post_status_payload = json.loads(post_status_out.out)
    assert post_status_payload["event_already_applied"] is True

    run_kinds = [entry["run_kind"] for entry in recent_runs(limit=10)]
    assert "paper_lane_status" in run_kinds
    assert "paper_lane_apply" in run_kinds
