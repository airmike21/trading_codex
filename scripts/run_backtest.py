"""Run a tiny demo backtest on synthetic data."""

from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from trading_codex.backtest.engine import run_backtest
from trading_codex.backtest import metrics
from trading_codex.strategies.trend_tsmom import TrendTSMOM


def make_synthetic_bars(n: int = 252, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dates = pd.date_range("2020-01-01", periods=n, freq="B")
    daily_rets = rng.normal(loc=0.0004, scale=0.01, size=n)
    price = 100 * (1 + pd.Series(daily_rets, index=dates)).cumprod()
    bars = pd.DataFrame(
        {
            "open": price.shift(1).fillna(price.iloc[0]),
            "high": price * 1.01,
            "low": price * 0.99,
            "close": price,
            "volume": 1_000_000,
        },
        index=dates,
    )
    return bars


def main() -> None:
    bars = make_synthetic_bars()
    strat = TrendTSMOM(lookback=20)
    result = run_backtest(bars, strat, slippage_bps=1.0, commission_bps=0.5)

    print("CAGR:", round(metrics.cagr(result.returns), 4))
    print("Vol:", round(metrics.vol(result.returns), 4))
    print("Sharpe:", round(metrics.sharpe(result.returns), 4))
    print("Max DD:", round(metrics.max_drawdown(result.returns), 4))
    print("Turnover:", round(metrics.turnover(result.weights), 4))

    result.equity.plot(title="Synthetic Trend Strategy Equity")
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
