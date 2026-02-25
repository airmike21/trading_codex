import numpy as np
import pandas as pd

from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.base import Strategy


class RotateWeights(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        idx = bars.index
        out = pd.DataFrame(0.0, index=idx, columns=["A", "B"])
        out.loc[idx[1:3], "A"] = 1.0
        out.loc[idx[3], "B"] = 1.0
        return out


def make_multi_bars() -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=4, freq="B")
    close_a = pd.Series([100.0, 101.0, 102.0, 103.0], index=idx)
    close_b = pd.Series([100.0, 100.0, 100.0, 100.0], index=idx)

    def ohlcv(close: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": 1_000,
            },
            index=idx,
        )

    return pd.concat({"A": ohlcv(close_a), "B": ohlcv(close_b)}, axis=1)


def test_engine_multi_asset_returns_and_turnover():
    bars = make_multi_bars()
    result = run_backtest(bars, RotateWeights(), slippage_bps=0.0, commission_bps=0.0)

    expected = pd.Series(
        [0.0, 0.01, (102.0 / 101.0) - 1.0, 0.0],
        index=bars.index,
    )
    np.testing.assert_allclose(result.returns.values, expected.values, atol=1e-12)

    assert isinstance(result.weights, pd.DataFrame)
    assert float(result.turnover.iloc[3]) == 2.0
