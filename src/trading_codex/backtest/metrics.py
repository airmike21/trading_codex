"""Performance metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd

ANNUALIZATION = 252


def cagr(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    equity = (1 + returns).cumprod()
    years = len(returns) / ANNUALIZATION
    if years <= 0:
        return 0.0
    return equity.iloc[-1] ** (1 / years) - 1


def vol(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    return returns.std(ddof=0) * np.sqrt(ANNUALIZATION)


def sharpe(returns: pd.Series, risk_free: float = 0.0) -> float:
    if returns.empty:
        return 0.0
    excess = returns - risk_free / ANNUALIZATION
    denom = excess.std(ddof=0)
    if denom == 0:
        return 0.0
    return excess.mean() / denom * np.sqrt(ANNUALIZATION)


def max_drawdown(returns: pd.Series) -> float:
    if returns.empty:
        return 0.0
    equity = (1 + returns).cumprod()
    peak = equity.cummax()
    drawdown = (equity / peak) - 1
    return drawdown.min()


def turnover(weights: pd.Series) -> float:
    if weights.empty:
        return 0.0
    return weights.diff().abs().sum()
