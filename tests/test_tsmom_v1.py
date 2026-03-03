import json

import numpy as np
import pandas as pd
import pandas.testing as pdt

from scripts.run_backtest import build_dual_actions, build_next_action_payload
from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.tsmom_v1 import TimeSeriesMomentumV1Strategy


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


def test_tsmom_v1_weights_are_long_only_and_sum_to_one_when_active():
    idx = pd.date_range("2020-01-01", periods=320, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 180.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(80.0, 130.0, len(idx)), index=idx),
            "C": pd.Series(np.linspace(120.0, 85.0, len(idx)), index=idx),
            "D": pd.Series(np.linspace(90.0, 110.0, len(idx)), index=idx),
        }
    )

    strat = TimeSeriesMomentumV1Strategy(
        symbols=["A", "B", "C"],
        lookback=63,
        rebalance="M",
        defensive="D",
    )
    weights = strat.generate_signals(bars)

    assert bool((weights >= 0.0).all().all())
    row_sums = weights.sum(axis=1)
    assert bool((np.isclose(row_sums, 0.0, atol=1e-12) | np.isclose(row_sums, 1.0, atol=1e-12)).all())
    assert bool((row_sums > 0.0).any())


def test_tsmom_v1_defensive_when_all_risk_assets_negative():
    idx = pd.date_range("2020-01-01", periods=160, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 60.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(120.0, 70.0, len(idx)), index=idx),
            "D": pd.Series(np.linspace(90.0, 110.0, len(idx)), index=idx),
        }
    )

    strat = TimeSeriesMomentumV1Strategy(
        symbols=["A", "B"],
        lookback=20,
        rebalance="M",
        defensive="D",
    )
    weights = strat.generate_signals(bars)
    active = weights.sum(axis=1) > 0.0

    assert bool(active.any())
    assert bool(np.isclose(weights.loc[active, "D"], 1.0, atol=1e-12).all())
    assert bool(np.isclose(weights.loc[active, ["A", "B"]], 0.0, atol=1e-12).all().all())


def test_tsmom_v1_decision_applies_on_next_bar_no_lookahead():
    idx = pd.date_range("2020-01-01", periods=150, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 150.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 70.0, len(idx)), index=idx),
            "D": pd.Series(100.0, index=idx),
        }
    )

    strat = TimeSeriesMomentumV1Strategy(
        symbols=["A", "B"],
        lookback=20,
        rebalance="W",
        defensive="D",
    )
    base = strat.generate_signals(bars)

    period_series = pd.Series(idx.to_period("W-FRI"), index=idx)
    rebalance_dates = period_series.index[period_series.ne(period_series.shift(-1)).fillna(False)]
    decision_date = None
    next_date = None
    for dt in rebalance_dates:
        pos = idx.get_loc(dt)
        if isinstance(pos, slice):
            pos = int(pos.start or 0)
        if int(pos) + 1 >= len(idx):
            continue
        cand_next = idx[int(pos) + 1]
        if float(base.loc[cand_next, "A"]) > 0.0:
            decision_date = dt
            next_date = cand_next
            break

    assert decision_date is not None
    assert next_date is not None

    altered = bars.copy()
    for field in ["open", "high", "low", "close"]:
        altered.loc[decision_date, ("A", field)] = 1.0
    changed = strat.generate_signals(altered)

    pdt.assert_series_equal(base.loc[decision_date], changed.loc[decision_date], check_names=False)
    assert not bool(np.isclose(base.loc[next_date], changed.loc[next_date], atol=1e-12).all())
    assert float(changed.loc[next_date, "D"]) == 1.0


def test_tsmom_v1_pipeline_next_action_payload_is_one_line():
    idx = pd.date_range("2020-01-01", periods=240, freq="B")
    bars = make_panel(
        {
            "A": pd.Series(np.linspace(100.0, 140.0, len(idx)), index=idx),
            "B": pd.Series(np.linspace(100.0, 90.0, len(idx)), index=idx),
            "D": pd.Series(np.linspace(90.0, 100.0, len(idx)), index=idx),
        }
    )

    strat = TimeSeriesMomentumV1Strategy(
        symbols=["A", "B"],
        lookback=63,
        rebalance="M",
        defensive="D",
    )
    result = run_backtest(bars, strat, slippage_bps=0.0, commission_bps=0.0)
    assert isinstance(result.weights, pd.DataFrame)

    actions = build_dual_actions(
        bars,
        result.weights,
        rebalance="M",
    )
    payload = build_next_action_payload(
        strategy_label="tsmom_v1",
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
