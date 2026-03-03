import json

import numpy as np
import pandas as pd

from scripts.run_backtest import build_dual_actions, build_next_action_payload
from trading_codex.backtest.allocations import (
    RiskParityConfig,
    erc_weights,
    erc_weights_from_cov,
    risk_contributions,
)
from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.risk_parity_erc import RiskParityERCStrategy


def make_panel(close_map: dict[str, pd.Series]) -> pd.DataFrame:
    frames = {}
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


def test_erc_weights_sum_to_one_and_are_long_only():
    rng = np.random.default_rng(7)
    idx = pd.date_range("2020-01-01", periods=300, freq="B")
    returns = pd.DataFrame(
        rng.normal(0.0, [0.01, 0.015, 0.02], size=(len(idx), 3)),
        index=idx,
        columns=["A", "B", "C"],
    )
    weights = erc_weights(returns, RiskParityConfig(lookback=63))
    assert np.isclose(float(weights.sum()), 1.0, atol=1e-12)
    assert bool((weights >= 0.0).all())


def test_erc_higher_vol_asset_gets_lower_weight():
    rng = np.random.default_rng(11)
    idx = pd.date_range("2020-01-01", periods=350, freq="B")
    returns = pd.DataFrame(
        {
            "LOW_VOL": rng.normal(0.0, 0.008, size=len(idx)),
            "HIGH_VOL": rng.normal(0.0, 0.03, size=len(idx)),
        },
        index=idx,
    )
    weights = erc_weights(returns, RiskParityConfig(lookback=80))
    assert float(weights["LOW_VOL"]) > float(weights["HIGH_VOL"])


def test_erc_risk_contributions_are_close():
    rng = np.random.default_rng(19)
    cov_true = np.array(
        [
            [0.0004, 0.00008, 0.00004],
            [0.00008, 0.0009, 0.00012],
            [0.00004, 0.00012, 0.0016],
        ],
        dtype=float,
    )
    samples = rng.multivariate_normal(np.zeros(3, dtype=float), cov_true, size=1200)
    sample_cov = np.cov(samples, rowvar=False, ddof=1)
    cfg = RiskParityConfig(max_iter=2000, tol=1e-10)
    weights = erc_weights_from_cov(sample_cov, cfg)
    rc = risk_contributions(weights, sample_cov)
    assert float(np.max(rc) - np.min(rc)) < 0.02


def test_risk_parity_pipeline_next_action_json_is_one_line():
    rng = np.random.default_rng(23)
    idx = pd.date_range("2020-01-01", periods=180, freq="B")
    returns = pd.DataFrame(
        rng.normal(0.0002, [0.01, 0.012, 0.009], size=(len(idx), 3)),
        index=idx,
        columns=["A", "B", "C"],
    )
    close_map = {
        symbol: pd.Series(100.0 * np.cumprod(1.0 + returns[symbol].to_numpy()), index=idx)
        for symbol in returns.columns
    }
    bars = make_panel(close_map)

    strategy = RiskParityERCStrategy(symbols=["A", "B", "C"], lookback=20, rebalance="M")
    result = run_backtest(bars, strategy, slippage_bps=0.0, commission_bps=0.0)
    assert isinstance(result.weights, pd.DataFrame)

    actions = build_dual_actions(bars, result.weights, rebalance="M")
    payload = build_next_action_payload(
        strategy_label="risk_parity_erc",
        bars=bars,
        weights=result.weights,
        actions=actions,
        resize_rebalance="M",
        next_rebalance="M",
    )
    line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    assert len(line.splitlines()) == 1
    obj = json.loads(line)
    assert obj["schema_name"] == "next_action"
    assert obj.get("event_id")
