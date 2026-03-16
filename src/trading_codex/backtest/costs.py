"""Transaction cost models."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd


MODEL_PORTFOLIO_VALUE = 10_000.0
TURNOVER_EPSILON = 1e-12


@dataclass(frozen=True)
class TransactionCostEstimate:
    turnover: pd.Series
    trade_count: pd.Series
    traded_notional: pd.Series
    slippage_cost: pd.Series
    commission_cost: pd.Series
    total_cost: pd.Series
    cost_return: pd.Series


def compute_turnover(weights: pd.Series | pd.DataFrame) -> pd.Series:
    """Compute daily turnover as the absolute change in target weights."""
    if isinstance(weights, pd.DataFrame):
        return weights.diff().abs().sum(axis=1).fillna(0.0).astype(float)
    return weights.diff().abs().fillna(0.0).astype(float)


def compute_trade_count(
    weights: pd.Series | pd.DataFrame,
    epsilon: float = TURNOVER_EPSILON,
) -> pd.Series:
    """Count changed sleeves/orders per day from target-weight deltas."""
    if isinstance(weights, pd.DataFrame):
        deltas = weights.diff().abs().fillna(0.0)
        return deltas.gt(float(epsilon)).sum(axis=1).astype(int)
    deltas = weights.diff().abs().fillna(0.0)
    return deltas.gt(float(epsilon)).astype(int)


def estimate_transaction_costs(
    turnover: pd.Series,
    trade_count: pd.Series | None = None,
    *,
    slippage_bps: float = 5.0,
    commission_bps: float = 0.0,
    commission_per_trade: float = 0.0,
    portfolio_value: float = MODEL_PORTFOLIO_VALUE,
) -> TransactionCostEstimate:
    """Estimate transaction costs from turnover in a normalized $10,000 model portfolio.

    Turnover is the absolute change in portfolio weights and therefore represents
    traded notional as a fraction of the model portfolio value.
    """
    turnover_series = turnover.astype(float).fillna(0.0)
    if trade_count is None:
        trade_count_series = pd.Series(0, index=turnover_series.index, dtype=int)
    else:
        trade_count_series = trade_count.reindex(turnover_series.index).fillna(0).astype(int)

    traded_notional = turnover_series * float(portfolio_value)
    slippage_cost = traded_notional * (float(slippage_bps) / 10_000.0)
    commission_cost = (
        traded_notional * (float(commission_bps) / 10_000.0)
        + trade_count_series.astype(float) * float(commission_per_trade)
    )
    total_cost = slippage_cost + commission_cost
    cost_return = total_cost / float(portfolio_value)

    return TransactionCostEstimate(
        turnover=turnover_series,
        trade_count=trade_count_series,
        traded_notional=traded_notional,
        slippage_cost=slippage_cost,
        commission_cost=commission_cost,
        total_cost=total_cost,
        cost_return=cost_return,
    )


def bps_cost(turnover: pd.Series, slippage_bps: float = 5.0, commission_bps: float = 0.0) -> pd.Series:
    """Legacy basis-point-only cost helper retained for compatibility."""
    return estimate_transaction_costs(
        turnover,
        slippage_bps=slippage_bps,
        commission_bps=commission_bps,
    ).cost_return
