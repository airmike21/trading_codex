"""Time-series momentum (trend) placeholder strategy."""

from __future__ import annotations

import pandas as pd

from trading_codex.strategies.base import Strategy


class TrendTSMOM(Strategy):
    def __init__(self, lookback: int = 20) -> None:
        self.lookback = lookback

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        close = bars["close"].astype(float)
        rets = close.pct_change()
        # Use only info available up to t-1 by shifting the rolling signal.
        rolling_mean = rets.rolling(self.lookback).mean().shift(1)
        signal = rolling_mean.apply(lambda x: 1.0 if x > 0 else (-1.0 if x < 0 else 0.0))
        return pd.DataFrame({"signal": signal}, index=bars.index)
