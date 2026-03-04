"""Inverse-volatility weighting overlay utilities."""

from __future__ import annotations

import numpy as np
import pandas as pd


def apply_inverse_vol_overlay(
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    lookback: int,
    eps: float,
) -> pd.DataFrame:
    """Scale active target weights by inverse realized volatility on update dates."""
    if lookback <= 0:
        raise ValueError("ivol lookback must be > 0.")
    if eps <= 0:
        raise ValueError("ivol eps must be > 0.")
    if not isinstance(weights, pd.DataFrame):
        raise TypeError("weights must be a DataFrame for inverse-vol overlay.")
    if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
        raise ValueError("Inverse-vol overlay expects MultiIndex bars: (symbol, field).")
    if "close" not in bars.columns.get_level_values(1):
        raise ValueError("Bars must include close field for inverse-vol overlay.")

    adjusted = pd.DataFrame(index=weights.index, columns=weights.columns, dtype=float)
    if weights.empty:
        return adjusted.fillna(0.0)

    close_panel = bars.xs("close", axis=1, level=1).astype(float)
    missing = [s for s in weights.columns if s not in close_panel.columns]
    if missing:
        raise ValueError(f"Missing close prices for inverse-vol symbols: {missing}")

    close = close_panel.reindex(index=weights.index).loc[:, weights.columns].astype(float)
    returns = close.pct_change()
    realized_vol = returns.rolling(window=lookback, min_periods=lookback).std(ddof=0)

    base_weights = weights.reindex(index=close.index, columns=close.columns).fillna(0.0).astype(float)
    active = base_weights.sum(axis=1) > 0.0
    changed = base_weights.ne(base_weights.shift(1)).any(axis=1)
    update_mask = changed & active
    active_dates = active.index[active]
    if len(active_dates):
        update_mask.loc[active_dates[0]] = True

    for dt in base_weights.index[update_mask]:
        row = base_weights.loc[dt]
        active_symbols = row.index[row > 0.0]

        if not len(active_symbols):
            adjusted.loc[dt] = row
            continue

        vol_row = realized_vol.loc[dt, active_symbols]
        inv_vol = pd.Series(0.0, index=active_symbols, dtype=float)
        valid = vol_row.notna()
        if bool(valid.any()):
            inv_vol.loc[valid] = 1.0 / np.maximum(vol_row.loc[valid].astype(float), float(eps))

        scaled = row.loc[active_symbols] * inv_vol
        scaled_sum = float(scaled.sum())

        if (not np.isfinite(scaled_sum)) or scaled_sum <= 0.0:
            adjusted.loc[dt] = row
            continue

        out_row = pd.Series(0.0, index=base_weights.columns, dtype=float)
        out_row.loc[active_symbols] = scaled / scaled_sum
        adjusted.loc[dt] = out_row

    return adjusted.ffill().fillna(0.0).astype(float)

