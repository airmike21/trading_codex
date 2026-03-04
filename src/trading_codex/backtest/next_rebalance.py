"""Next rebalance date computation helpers."""

from __future__ import annotations

import pandas as pd


def _next_trading_day_rebalance(
    index: pd.DatetimeIndex,
    current: pd.Timestamp,
    trading_days: int,
) -> str:
    if trading_days <= 0:
        raise ValueError("trading_days must be > 0.")
    if index.empty:
        return (current + pd.offsets.BDay(trading_days)).date().isoformat()

    pos = index.get_indexer([current])[0]
    if pos < 0:
        normalized = pd.Timestamp(current).normalize()
        pos = index.get_indexer([normalized])[0]
    if pos < 0:
        raise ValueError("current date is not present in index.")

    next_pos = ((int(pos) // int(trading_days)) + 1) * int(trading_days)
    if next_pos < len(index):
        return pd.Timestamp(index[next_pos]).date().isoformat()

    bars_remaining = int(trading_days) - ((int(pos) + 1) % int(trading_days))
    if bars_remaining == 0:
        bars_remaining = int(trading_days)
    return (current + pd.offsets.BDay(bars_remaining)).date().isoformat()


def _next_calendar_rebalance(current: pd.Timestamp, cadence: str) -> str | None:
    c = cadence.strip().upper()
    if c in {"W", "W-FRI", "WEEKLY"}:
        return (current + pd.offsets.Week(weekday=4)).date().isoformat()
    if c in {"M", "BM", "BME", "MONTHLY"}:
        return (current + pd.offsets.BMonthEnd(1)).date().isoformat()
    return None


def compute_next_rebalance_date(
    index: pd.DatetimeIndex,
    current: pd.Timestamp,
    *,
    trading_days: int | None = None,
    cadence: str | None = None,
) -> str | None:
    """Compute next scheduled rebalance date as ISO string."""
    if (trading_days is None and cadence is None) or (trading_days is not None and cadence is not None):
        raise ValueError("Provide exactly one of trading_days or cadence.")

    current_ts = pd.Timestamp(current)
    if trading_days is not None:
        return _next_trading_day_rebalance(index, current_ts, int(trading_days))
    return _next_calendar_rebalance(current_ts, str(cadence))
