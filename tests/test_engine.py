import pandas as pd

from trading_codex.backtest.engine import run_backtest
from trading_codex.strategies.base import Strategy


class ZeroSignal(Strategy):
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame({"signal": 0.0}, index=bars.index)


def make_bars(n: int = 10) -> pd.DataFrame:
    idx = pd.date_range("2020-01-01", periods=n, freq="B")
    price = pd.Series(range(100, 100 + n), index=idx, dtype=float)
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


def test_engine_smoke():
    bars = make_bars()
    result = run_backtest(bars, ZeroSignal())

    assert len(result.returns) == len(bars)
    assert result.returns.isna().sum() == 0
    assert result.weights.isna().sum() == 0
    assert (result.returns == 0.0).all()
