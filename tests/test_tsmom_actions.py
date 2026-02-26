import numpy as np
import pandas as pd

from scripts.run_backtest import build_tsmom_actions, maybe_write_trades
from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.trend_tsmom import TrendTSMOM


def make_single_bars(close: pd.Series) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "volume": 1_000,
        },
        index=close.index,
    )


def make_long_only_close(index: pd.DatetimeIndex) -> pd.Series:
    returns = pd.Series(0.0, index=index, dtype=float)
    returns.iloc[40:] = 0.001

    volatile_mask = index >= pd.Timestamp("2020-04-01")
    oscillating = np.where(np.arange(int(volatile_mask.sum())) % 2 == 0, 0.03, -0.02)
    returns.loc[volatile_mask] = oscillating
    return 100.0 * (1.0 + returns).cumprod()


def test_tsmom_actions_include_enter_and_resize_with_overlay():
    idx = pd.date_range("2020-01-01", "2020-08-31", freq="B")
    bars = make_single_bars(make_long_only_close(idx))
    strategy = TrendTSMOM(lookback=10, allow_short=False)

    overlay_result = run_backtest(
        bars,
        strategy,
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.10,
        vol_max=2.0,
        vol_update="rebalance",
        rebalance_cadence="M",
    )
    assert isinstance(overlay_result.weights, pd.Series)

    overlay_actions = build_tsmom_actions(
        "SPY",
        bars,
        overlay_result.weights,
        vol_target=0.10,
        vol_update="rebalance",
        rebalance="M",
    )
    assert "ENTER" in set(overlay_actions["action"])
    assert "RESIZE" in set(overlay_actions["action"])

    plain_result = run_backtest(
        bars,
        strategy,
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=None,
    )
    assert isinstance(plain_result.weights, pd.Series)

    plain_actions = build_tsmom_actions("SPY", bars, plain_result.weights, vol_target=None)
    assert "ENTER" in set(plain_actions["action"])
    assert "RESIZE" not in set(plain_actions["action"])


def test_tsmom_trades_out_keeps_trade_schema_with_actions_present(tmp_path):
    idx = pd.date_range("2020-01-01", "2020-08-31", freq="B")
    bars = make_single_bars(make_long_only_close(idx))
    strategy = TrendTSMOM(lookback=10, allow_short=False)

    result = run_backtest(
        bars,
        strategy,
        slippage_bps=0.0,
        commission_bps=0.0,
        vol_target=0.10,
        vol_lookback=5,
        vol_min=0.10,
        vol_max=2.0,
        vol_update="rebalance",
        rebalance_cadence="M",
    )
    assert isinstance(result.weights, pd.Series)

    actions = build_tsmom_actions(
        "SPY",
        bars,
        result.weights,
        vol_target=0.10,
        vol_update="rebalance",
        rebalance="M",
    )
    assert not actions.empty

    out_path = tmp_path / "long_only_trades.csv"
    maybe_write_trades(
        "tsmom",
        str(out_path),
        "SPY",
        bars,
        result.weights,
        actions,
    )

    written = pd.read_csv(out_path)
    assert written.columns.tolist() == [
        "symbol",
        "entry_date",
        "exit_date",
        "direction",
        "entry_price",
        "exit_price",
        "pct_return",
        "holding_days",
    ]
