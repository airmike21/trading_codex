"""Volatility targeting overlay utilities."""

from __future__ import annotations

import numpy as np
import pandas as pd

WeightsLike = pd.Series | pd.DataFrame


def compute_portfolio_returns_1x(
    raw_weights: WeightsLike,
    asset_returns: WeightsLike,
) -> pd.Series:
    """Compute 1x portfolio returns using lagged (t-1) raw weights."""
    lagged_weights = raw_weights.shift(1).fillna(0.0)

    if isinstance(lagged_weights, pd.DataFrame):
        if not isinstance(asset_returns, pd.DataFrame):
            raise TypeError("asset_returns must be a DataFrame when raw_weights is a DataFrame.")
        aligned_returns = asset_returns.reindex(
            index=lagged_weights.index,
            columns=lagged_weights.columns,
        ).fillna(0.0)
        return lagged_weights.mul(aligned_returns).sum(axis=1).astype(float)

    if isinstance(asset_returns, pd.DataFrame):
        raise TypeError("asset_returns must be a Series when raw_weights is a Series.")
    aligned_returns = asset_returns.reindex(lagged_weights.index).fillna(0.0)
    return lagged_weights.mul(aligned_returns).astype(float)


def compute_realized_vol(portfolio_returns: pd.Series, lookback: int) -> pd.Series:
    return (
        portfolio_returns.rolling(window=lookback, min_periods=lookback).std(ddof=1)
        * np.sqrt(252.0)
    )


def compute_leverage_series(
    realized_vol: pd.Series,
    target_vol: float,
    min_leverage: float,
    max_leverage: float,
    zero_vol_epsilon: float = 1e-12,
) -> pd.Series:
    leverage = pd.Series(1.0, index=realized_vol.index, dtype=float)

    zero_vol = realized_vol.notna() & (realized_vol <= zero_vol_epsilon)
    leverage.loc[zero_vol] = float(min_leverage)

    valid = realized_vol.notna() & (realized_vol > zero_vol_epsilon)
    leverage.loc[valid] = (float(target_vol) / realized_vol.loc[valid]).clip(
        lower=float(min_leverage),
        upper=float(max_leverage),
    )
    return leverage


def apply_vol_target_overlay(
    raw_weights: WeightsLike,
    asset_returns: WeightsLike,
    target_vol: float,
    lookback: int = 63,
    min_leverage: float = 0.0,
    max_leverage: float = 1.0,
    update_mask: pd.Series | None = None,
) -> tuple[WeightsLike, pd.Series, pd.Series]:
    portfolio_returns = compute_portfolio_returns_1x(raw_weights, asset_returns)
    realized_vol = compute_realized_vol(portfolio_returns, lookback=lookback)
    leverage_daily = compute_leverage_series(
        realized_vol,
        target_vol=target_vol,
        min_leverage=min_leverage,
        max_leverage=max_leverage,
    )

    if update_mask is None:
        leverage = leverage_daily
    else:
        aligned_mask = update_mask.reindex(leverage_daily.index).fillna(False).astype(bool)
        leverage = leverage_daily.where(aligned_mask).ffill()
        if len(leverage_daily):
            leverage = leverage.fillna(float(leverage_daily.iloc[0]))
        leverage = leverage.fillna(1.0)

    if isinstance(raw_weights, pd.DataFrame):
        scaled_weights = raw_weights.mul(leverage, axis=0)
    else:
        scaled_weights = raw_weights.mul(leverage)

    return scaled_weights, leverage, realized_vol
