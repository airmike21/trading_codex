import numpy as np
import pandas as pd

from trading_codex.strategies.trend_tsmom import TrendTSMOM


def make_bars(n: int = 10) -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    price = pd.Series(np.linspace(100, 110, n), index=idx)
    return pd.DataFrame(
        {
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": 1_000,
        },
        index=idx,
    )


def test_trend_signal_uses_lagged_info():
    bars = make_bars(8)
    strat = TrendTSMOM(lookback=3)
    signals = strat.generate_signals(bars)

    rets = bars["close"].pct_change()
    rolling_mean = rets.rolling(3).mean().shift(1)

    expected = rolling_mean.apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else 0.0))
    expected = expected.rename("signal")
    pd.testing.assert_series_equal(signals["signal"], expected)

    # First few are zero due to lookback and lag
    assert signals["signal"].iloc[0] == 0.0
