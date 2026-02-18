import numpy as np
import pandas as pd

from scripts.run_backtest import build_dual_actions
from trading_codex.strategies.sma200 import Sma200RegimeStrategy


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


def test_sma200_weights_do_not_use_same_day_rebalance_close():
    idx = pd.date_range("2020-01-01", "2020-04-30", freq="B")
    close_map = {
        "SPY": pd.Series(np.linspace(100.0, 140.0, len(idx)), index=idx),
        "TLT": pd.Series(100.0, index=idx),
    }
    bars = make_panel(close_map)
    strat = Sma200RegimeStrategy(risk_symbol="SPY", defensive="TLT", sma_window=5, rebalance="W")

    week_periods = pd.Series(idx.to_period("W-FRI"), index=idx)
    rebalance_dates = week_periods.index[week_periods.ne(week_periods.shift(-1))]
    rebalance_date = rebalance_dates[2]
    next_day = idx[idx.get_loc(rebalance_date) + 1]

    altered_bars = bars.copy()
    altered_bars.loc[rebalance_date, ("SPY", "close")] = 1.0

    base_weights = strat.generate_signals(bars)
    altered_weights = strat.generate_signals(altered_bars)

    pd.testing.assert_series_equal(base_weights.loc[next_day], altered_weights.loc[next_day])


def test_sma200_rebalance_and_next_day_application():
    idx = pd.date_range("2020-01-01", "2020-03-31", freq="B")
    risk = pd.Series(100.0, index=idx)
    feb = idx.month == 2
    mar = idx.month == 3
    risk.loc[feb] = np.linspace(90.0, 130.0, feb.sum())
    risk.loc[mar] = 130.0

    close_map = {
        "SPY": risk,
        "TLT": pd.Series(100.0, index=idx),
    }
    bars = make_panel(close_map)

    strat = Sma200RegimeStrategy(risk_symbol="SPY", defensive="TLT", sma_window=5, rebalance="M")
    weights = strat.generate_signals(bars)

    month_periods = pd.Series(idx.to_period("M"), index=idx)
    month_end = month_periods.index[month_periods.ne(month_periods.shift(-1))]
    jan_end = month_end[0]
    feb_end = month_end[1]
    after_jan = idx[idx.get_loc(jan_end) + 1]
    after_feb = idx[idx.get_loc(feb_end) + 1]

    assert float(weights.loc[jan_end].sum()) == 0.0
    assert float(weights.loc[after_jan, "TLT"]) == 1.0
    assert float(weights.loc[feb_end, "TLT"]) == 1.0
    assert float(weights.loc[after_feb, "SPY"]) == 1.0


def test_sma200_cash_mode_exits_and_reenters():
    idx = pd.date_range("2020-01-01", "2020-06-30", freq="B")

    risk = pd.Series(index=idx, dtype=float)
    for month, start, end in [
        (1, 100.0, 110.0),
        (2, 111.0, 120.0),
        (3, 119.0, 90.0),
        (4, 89.0, 80.0),
        (5, 81.0, 140.0),
        (6, 141.0, 150.0),
    ]:
        month_mask = idx.month == month
        risk.loc[month_mask] = np.linspace(start, end, month_mask.sum())

    bars = make_panel({"SPY": risk})
    strat = Sma200RegimeStrategy(risk_symbol="SPY", defensive=None, sma_window=5, rebalance="M")
    weights = strat.generate_signals(bars)

    sma = risk.rolling(5).mean()
    risk_on = (risk.shift(1) > sma.shift(1)).fillna(False)
    month_periods = pd.Series(idx.to_period("M"), index=idx)
    month_end = month_periods.index[month_periods.ne(month_periods.shift(-1))]
    off_rebalance = next(
        dt for dt in month_end if (not bool(risk_on.loc[dt])) and (idx.get_loc(dt) + 1 < len(idx))
    )
    off_apply = idx[idx.get_loc(off_rebalance) + 1]
    assert float(weights.loc[off_apply].sum()) == 0.0

    actions = build_dual_actions(bars, weights)
    exit_rows = actions[
        (actions["action"] == "EXIT")
        & (actions["from_symbol"] == "SPY")
        & (actions["to_symbol"] == "CASH")
    ]
    assert not exit_rows.empty

    first_exit_date = pd.to_datetime(exit_rows.iloc[0]["date"])
    later_actions = actions[pd.to_datetime(actions["date"]) > first_exit_date]
    reenter_rows = later_actions[
        (later_actions["action"] == "ENTER")
        & (later_actions["from_symbol"] == "CASH")
        & (later_actions["to_symbol"] == "SPY")
    ]
    assert not reenter_rows.empty
