from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd

from scripts.run_backtest import build_dual_actions, build_next_action_payload
from trading_codex.data import LocalStore
from trading_codex.strategies.dual_mom_vol10_cash import DualMomentumVol10CashStrategy


def _repo_root_and_env() -> tuple[Path, dict[str, str]]:
    repo_root = Path(__file__).resolve().parents[1]
    env = os.environ.copy()
    src_path = str(repo_root / "src")
    env["PYTHONPATH"] = f"{src_path}:{env['PYTHONPATH']}" if env.get("PYTHONPATH") else src_path
    return repo_root, env


def make_panel(close_map: dict[str, pd.Series]) -> pd.DataFrame:
    frames: dict[str, pd.DataFrame] = {}
    for symbol, close in close_map.items():
        frames[symbol] = pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=close.index,
        )
    return pd.concat(frames, axis=1)


def _expected_event_id(payload: dict[str, object]) -> str:
    def s(key: str) -> str:
        value = payload.get(key, "")
        return "" if value is None else str(value)

    return ":".join(
        [
            s("date"),
            s("strategy"),
            s("action"),
            s("symbol"),
            s("target_shares"),
            s("resize_new_shares"),
            s("next_rebalance"),
        ]
    )


def _price_series(index: pd.DatetimeIndex, returns: np.ndarray, base: float) -> pd.Series:
    return pd.Series(base * np.cumprod(1.0 + returns.astype(float)), index=index)


def _write_symbol_bars(store: LocalStore, symbol: str, close: pd.Series) -> None:
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
            index=close.index,
        ),
    )


def _strategy_bars(
    *,
    spy_returns: np.ndarray,
    qqq_returns: np.ndarray,
    iwm_returns: np.ndarray,
    efa_returns: np.ndarray,
    bil_returns: np.ndarray,
) -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=len(spy_returns), freq="B")
    return make_panel(
        {
            "SPY": _price_series(idx, spy_returns, 100.0),
            "QQQ": _price_series(idx, qqq_returns, 105.0),
            "IWM": _price_series(idx, iwm_returns, 95.0),
            "EFA": _price_series(idx, efa_returns, 98.0),
            "BIL": _price_series(idx, bil_returns, 100.0),
        }
    )


def test_dual_mom_vol10_cash_selects_risk_asset_when_it_beats_defensive() -> None:
    periods = 260
    idx = np.arange(periods)
    bars = _strategy_bars(
        spy_returns=np.full(periods, 0.0006),
        qqq_returns=np.where(idx % 2 == 0, 0.0012, -0.0008),
        iwm_returns=np.full(periods, -0.0002),
        efa_returns=np.where(idx % 2 == 0, 0.0040, -0.0010),
        bil_returns=np.full(periods, 0.0001),
    )

    weights = DualMomentumVol10CashStrategy(
        symbols=["SPY", "QQQ", "IWM", "EFA"],
        defensive_symbol="BIL",
        momentum_lookback=63,
        rebalance=21,
        vol_lookback=20,
        target_vol=0.10,
    ).generate_signals(bars)

    active = weights["EFA"] > 0.0
    assert bool(active.any())
    assert bool((weights.loc[active, "EFA"] > 0.0).all())
    assert bool(np.isclose(weights.loc[active, "BIL"], 0.0, atol=1e-12).all())


def test_dual_mom_vol10_cash_falls_back_when_absolute_momentum_fails() -> None:
    periods = 260
    bars = _strategy_bars(
        spy_returns=np.full(periods, -0.0004),
        qqq_returns=np.full(periods, -0.0003),
        iwm_returns=np.full(periods, -0.0002),
        efa_returns=np.full(periods, -0.0005),
        bil_returns=np.full(periods, 0.0002),
    )

    weights = DualMomentumVol10CashStrategy(
        symbols=["SPY", "QQQ", "IWM", "EFA"],
        defensive_symbol="BIL",
    ).generate_signals(bars)

    active = weights.sum(axis=1) > 0.0
    assert bool(active.any())
    assert bool(np.isclose(weights.loc[active, "BIL"], 1.0, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[active, ["SPY", "QQQ", "IWM", "EFA"]], 0.0, atol=1e-12).all().all())


def test_dual_mom_vol10_cash_reduces_weight_when_realized_vol_is_high() -> None:
    periods = 260
    idx = np.arange(periods)
    bars = _strategy_bars(
        spy_returns=np.full(periods, 0.0005),
        qqq_returns=np.where(idx % 2 == 0, 0.0015, -0.0012),
        iwm_returns=np.full(periods, -0.0001),
        efa_returns=np.where(idx % 2 == 0, 0.03, -0.01),
        bil_returns=np.full(periods, 0.0001),
    )

    weights = DualMomentumVol10CashStrategy(
        symbols=["SPY", "QQQ", "IWM", "EFA"],
        defensive_symbol="BIL",
    ).generate_signals(bars)

    active = weights["EFA"] > 0.0
    assert bool(active.any())
    assert float(weights.loc[active, "EFA"].max()) < 1.0
    assert float(weights.loc[active, "EFA"].min()) > 0.0
    assert float(weights.max().max()) <= 1.0


def test_dual_mom_vol10_cash_invalid_risk_vol_stays_defensive() -> None:
    periods = 260
    bars = _strategy_bars(
        spy_returns=np.full(periods, 0.0005),
        qqq_returns=np.full(periods, 0.0004),
        iwm_returns=np.full(periods, 0.0003),
        efa_returns=np.full(periods, 0.0020),
        bil_returns=np.full(periods, 0.0001),
    )

    weights = DualMomentumVol10CashStrategy(
        symbols=["SPY", "QQQ", "IWM", "EFA"],
        defensive_symbol="BIL",
    ).generate_signals(bars)

    active = weights.sum(axis=1) > 0.0
    assert bool(active.any())
    assert float(weights["EFA"].max()) == 0.0
    assert bool(np.isclose(weights.loc[active, "BIL"], 1.0, atol=1e-12).all())


def test_dual_mom_vol10_cash_near_zero_nonzero_risk_vol_stays_defensive() -> None:
    periods = 260
    idx = np.arange(periods)
    tiny_noise = np.where(idx % 2 == 0, 0.0001, -0.0001)
    bars = _strategy_bars(
        spy_returns=np.full(periods, 0.0005),
        qqq_returns=np.full(periods, 0.0004),
        iwm_returns=np.full(periods, 0.0003),
        efa_returns=0.0010 + tiny_noise,
        bil_returns=np.full(periods, 0.0001),
    )

    close = bars.xs("close", axis=1, level=1)
    efa_realized_vol = (
        close["EFA"]
        .pct_change()
        .rolling(window=20, min_periods=20)
        .std(ddof=1)
        * np.sqrt(252.0)
    ).dropna()
    assert not efa_realized_vol.empty
    assert float(efa_realized_vol.min()) > 0.0
    assert float(efa_realized_vol.max()) < DualMomentumVol10CashStrategy._MIN_USABLE_ANNUALIZED_VOL

    weights = DualMomentumVol10CashStrategy(
        symbols=["SPY", "QQQ", "IWM", "EFA"],
        defensive_symbol="BIL",
    ).generate_signals(bars)

    active = weights.sum(axis=1) > 0.0
    assert bool(active.any())
    assert float(weights["EFA"].max()) == 0.0
    assert bool(np.isclose(weights.loc[active, "BIL"], 1.0, atol=1e-12).all())


def test_dual_mom_vol10_cash_resize_path_supports_next_action_resizes_without_overlay() -> None:
    idx = pd.date_range("2020-01-01", periods=6, freq="B")
    close = pd.Series([100.0, 101.0, 102.0, 103.0, 104.0, 105.0], index=idx)
    bars = make_panel({"EFA": close})
    weights = pd.DataFrame(
        {"EFA": [0.80, 0.80, 0.80, 0.45, 0.45, 0.45]},
        index=idx,
        dtype=float,
    )

    actions = build_dual_actions(
        bars,
        weights,
        rebalance=3,
        allow_resize_without_vol_target=True,
    )
    resize_rows = actions[actions["action"] == "RESIZE"]
    assert not resize_rows.empty

    resize_dt = pd.to_datetime(resize_rows.iloc[-1]["date"])
    bars_upto = bars.loc[:resize_dt]
    weights_upto = weights.loc[:resize_dt]
    actions_upto = build_dual_actions(
        bars_upto,
        weights_upto,
        rebalance=3,
        allow_resize_without_vol_target=True,
    )
    payload = build_next_action_payload(
        strategy_label="dual_mom_vol10_cash",
        bars=bars_upto,
        weights=weights_upto,
        actions=actions_upto,
        resize_rebalance=3,
        next_rebalance=3,
        allow_resize_without_vol_target=True,
    )

    assert payload["action"] == "RESIZE"
    assert isinstance(payload["resize_prev_shares"], int)
    assert isinstance(payload["resize_new_shares"], int)
    assert payload["resize_prev_shares"] != payload["resize_new_shares"]


def test_dual_mom_vol10_cash_cli_next_action_json_is_one_line_and_event_id_unchanged(
    tmp_path: Path,
) -> None:
    repo_root, env = _repo_root_and_env()
    periods = 320
    idx = pd.date_range("2020-01-01", periods=periods, freq="B")
    alt = np.arange(periods)

    store = LocalStore(base_dir=tmp_path)
    close_map = {
        "SPY": _price_series(idx, np.full(periods, 0.0005), 100.0),
        "QQQ": _price_series(idx, np.where(alt % 2 == 0, 0.0015, -0.0010), 105.0),
        "IWM": _price_series(idx, np.full(periods, -0.0001), 95.0),
        "EFA": _price_series(idx, np.where(alt % 2 == 0, 0.03, -0.01), 98.0),
        "BIL": _price_series(idx, np.full(periods, 0.0001), 100.0),
    }
    for symbol, close in close_map.items():
        _write_symbol_bars(store, symbol, close)

    cmd = [
        sys.executable,
        str(repo_root / "scripts" / "run_backtest.py"),
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
        idx[0].date().isoformat(),
        "--end",
        idx[-1].date().isoformat(),
        "--no-plot",
        "--next-action-json",
        "--data-dir",
        str(tmp_path),
    ]
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env, cwd=str(repo_root))
    assert proc.returncode == 0, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"

    lines = proc.stdout.splitlines()
    assert len(lines) == 1, f"stdout={proc.stdout!r}\nstderr={proc.stderr!r}"
    payload = json.loads(lines[0])

    assert payload["strategy"] == "dual_mom_vol10_cash"
    assert isinstance(payload["target_shares"], int)
    assert payload["event_id"] == _expected_event_id(payload)
    assert lines[0] == json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
