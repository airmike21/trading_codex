import json

import numpy as np
import pandas as pd
import pandas.testing as pdt

from scripts.run_backtest import build_dual_actions, build_next_action_payload
from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.xsmom_v1 import CrossSectionalMomentumV1Strategy


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


def test_xsmom_v1_selects_top_asset_and_weights_sum_to_one():
    idx = pd.date_range("2020-01-01", periods=320, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 160.0, len(idx)), index=idx),  # strong
            "B": pd.Series(np.linspace(100.0, 120.0, len(idx)), index=idx),  # weaker
            "D": pd.Series(np.linspace(100.0, 105.0, len(idx)), index=idx),  # defensive
        }
    )
    strat = CrossSectionalMomentumV1Strategy(symbols=["A", "B"], lookback=63, top_n=1, rebalance="M", defensive="D")
    w = strat.generate_signals(bars)

    assert bool((w >= 0.0).all().all())
    sums = w.sum(axis=1)
    assert bool((np.isclose(sums, 0.0, atol=1e-12) | np.isclose(sums, 1.0, atol=1e-12)).all())
    active = sums > 0.0
    assert bool(active.any())
    # When active and selecting top-1, should generally be A (not guaranteed every day but should occur)
    assert bool((w.loc[active, "A"] > 0.0).any())


def test_xsmom_v1_defensive_when_all_momentum_negative():
    idx = pd.date_range("2020-01-01", periods=200, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 60.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(120.0, 70.0, len(idx)), index=idx),
            "D": pd.Series(np.linspace(90.0, 110.0, len(idx)), index=idx),
        }
    )
    strat = CrossSectionalMomentumV1Strategy(symbols=["A", "B"], lookback=20, top_n=1, rebalance="M", defensive="D")
    w = strat.generate_signals(bars)
    active = w.sum(axis=1) > 0.0
    assert bool(active.any())
    assert bool(np.isclose(w.loc[active, "D"], 1.0, atol=1e-12).all())


def test_xsmom_v1_applies_on_next_bar_no_lookahead():
    idx = pd.date_range("2020-01-01", periods=170, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 140.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 80.0, len(idx)), index=idx),
            "D": pd.Series(100.0, index=idx),
        }
    )
    strat = CrossSectionalMomentumV1Strategy(symbols=["A", "B"], lookback=20, top_n=1, rebalance="W", defensive="D")
    base = strat.generate_signals(bars)

    ps = pd.Series(idx.to_period("W-FRI"), index=idx)
    decision_dates = ps.index[ps.ne(ps.shift(-1)).fillna(False)]
    # Pick a decision date with a next bar where base allocation holds A.
    decision_date = None
    next_date = None
    for dt in decision_dates:
        pos = idx.get_loc(dt)
        if isinstance(pos, slice):
            pos = int(pos.start or 0)
        if int(pos) + 1 >= len(idx):
            continue
        candidate_next = idx[int(pos) + 1]
        if float(base.loc[candidate_next, "A"]) > 0.0:
            decision_date = dt
            next_date = candidate_next
            break

    assert decision_date is not None and next_date is not None

    altered = bars.copy()
    # Mutate close on decision date only; should not change weights on decision date itself
    for field in ["open", "high", "low", "close"]:
        altered.loc[decision_date, ("A", field)] = 1.0
    changed = strat.generate_signals(altered)

    pdt.assert_series_equal(base.loc[decision_date], changed.loc[decision_date], check_names=False)
    # next bar should be impacted (selection/defensive should change)
    assert not bool(np.isclose(base.loc[next_date], changed.loc[next_date], atol=1e-12).all())


def test_xsmom_v1_pipeline_next_action_payload_is_one_line():
    idx = pd.date_range("2020-01-01", periods=260, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 150.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 90.0, len(idx)), index=idx),
            "D": pd.Series(np.linspace(90.0, 100.0, len(idx)), index=idx),
        }
    )
    strat = CrossSectionalMomentumV1Strategy(symbols=["A", "B"], lookback=63, top_n=1, rebalance="M", defensive="D")
    result = run_backtest(bars, strat, slippage_bps=0.0, commission_bps=0.0)
    assert isinstance(result.weights, pd.DataFrame)

    actions = build_dual_actions(bars, result.weights, rebalance="M")
    payload = build_next_action_payload(
        strategy_label="xsmom_v1",
        bars=bars,
        weights=result.weights,
        actions=actions,
        resize_rebalance="M",
        next_rebalance="M",
    )
    line = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
    assert len(line.splitlines()) == 1
    obj = json.loads(line)
    assert obj.get("schema_name") == "next_action"
    assert obj.get("event_id")
