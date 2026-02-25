import numpy as np
import pandas as pd

from trading_codex.strategies.dual_momentum import DualMomentumStrategy


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


def test_dual_momentum_uses_next_day_and_defensive_when_risk_negative():
    idx = pd.date_range("2020-01-01", "2020-03-31", freq="B")
    close_map = {
        "SPY": pd.Series(np.linspace(100, 80, len(idx)), index=idx),
        "QQQ": pd.Series(np.linspace(100, 70, len(idx)), index=idx),
        "IWM": pd.Series(np.linspace(100, 85, len(idx)), index=idx),
        "EFA": pd.Series(np.linspace(100, 90, len(idx)), index=idx),
        "TLT": pd.Series(np.linspace(100, 110, len(idx)), index=idx),
    }
    bars = make_panel(close_map)

    strat = DualMomentumStrategy(
        risk_universe=["SPY", "QQQ", "IWM", "EFA"],
        defensive="TLT",
        lookback=5,
        rebalance="M",
    )
    weights = strat.generate_signals(bars)

    month_periods = pd.Series(idx.to_period("M"), index=idx)
    month_end = month_periods.index[month_periods.ne(month_periods.shift(-1))][0]
    next_day = idx[idx.get_loc(month_end) + 1]

    assert float(weights.loc[month_end].sum()) == 0.0
    assert float(weights.loc[next_day, "TLT"]) == 1.0


def test_dual_momentum_goes_to_cash_when_defensive_disabled():
    idx = pd.date_range("2020-01-01", "2020-03-31", freq="B")
    close_map = {
        "SPY": pd.Series(np.linspace(100, 80, len(idx)), index=idx),
        "QQQ": pd.Series(np.linspace(100, 70, len(idx)), index=idx),
        "IWM": pd.Series(np.linspace(100, 85, len(idx)), index=idx),
        "EFA": pd.Series(np.linspace(100, 90, len(idx)), index=idx),
    }
    bars = make_panel(close_map)

    strat = DualMomentumStrategy(
        risk_universe=["SPY", "QQQ", "IWM", "EFA"],
        defensive=None,
        lookback=5,
        rebalance="M",
    )
    weights = strat.generate_signals(bars)

    month_periods = pd.Series(idx.to_period("M"), index=idx)
    month_end = month_periods.index[month_periods.ne(month_periods.shift(-1))][0]
    next_day = idx[idx.get_loc(month_end) + 1]

    assert float(weights.loc[next_day].sum()) == 0.0


def test_dual_momentum_sma200_gate_forces_defensive_next_day():
    idx = pd.date_range("2020-01-01", "2020-03-31", freq="B")
    close_map = {
        "QQQ": pd.Series(np.linspace(100, 130, len(idx)), index=idx),
        "IWM": pd.Series(np.linspace(100, 120, len(idx)), index=idx),
        "TLT": pd.Series(np.linspace(100, 102, len(idx)), index=idx),
        "SPY": pd.Series(np.linspace(120, 80, len(idx)), index=idx),
    }
    bars = make_panel(close_map)

    strat_no_gate = DualMomentumStrategy(
        risk_universe=["QQQ", "IWM"],
        defensive="TLT",
        lookback=5,
        rebalance="M",
    )
    strat_gate = DualMomentumStrategy(
        risk_universe=["QQQ", "IWM"],
        defensive="TLT",
        lookback=5,
        rebalance="M",
        regime_gate="sma200",
        gate_symbol="SPY",
        gate_sma_window=5,
    )
    weights_no_gate = strat_no_gate.generate_signals(bars)
    weights_gate = strat_gate.generate_signals(bars)

    month_periods = pd.Series(idx.to_period("M"), index=idx)
    month_end = month_periods.index[month_periods.ne(month_periods.shift(-1))][0]
    next_day = idx[idx.get_loc(month_end) + 1]

    assert float(weights_gate.loc[month_end].sum()) == 0.0
    assert float(weights_no_gate.loc[next_day, "QQQ"]) == 1.0
    assert float(weights_gate.loc[next_day, "QQQ"]) == 0.0
    assert float(weights_gate.loc[next_day, "TLT"]) == 1.0
