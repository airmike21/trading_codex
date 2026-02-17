"""Simple daily-bar backtest engine."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from trading_codex.backtest.costs import bps_cost
from trading_codex.data.contracts import validate_bars, validate_signals
from trading_codex.strategies.base import Strategy


@dataclass
class BacktestResult:
    returns: pd.Series
    weights: pd.Series
    turnover: pd.Series
    equity: pd.Series


def run_backtest(
    bars: pd.DataFrame,
    strategy: Strategy,
    slippage_bps: float = 1.0,
    commission_bps: float = 0.0,
) -> BacktestResult:
    validate_bars(bars)

    signals = strategy.generate_signals(bars)
    validate_signals(signals)

    signals = signals.reindex(bars.index).fillna(0.0)
    weights = signals["signal"].clip(-1.0, 1.0).astype(float)

    close = bars["close"].astype(float)
    rets = close.pct_change().fillna(0.0)

    turnover = weights.diff().abs().fillna(0.0)
    costs = bps_cost(turnover, slippage_bps=slippage_bps, commission_bps=commission_bps)

    strategy_returns = (weights * rets) - costs
    equity = (1 + strategy_returns).cumprod()

    return BacktestResult(
        returns=strategy_returns,
        weights=weights,
        turnover=turnover,
        equity=equity,
    )
