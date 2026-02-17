"""Transaction cost models."""

from __future__ import annotations

import pandas as pd


def bps_cost(turnover: pd.Series, slippage_bps: float = 1.0, commission_bps: float = 0.0) -> pd.Series:
    """Compute transaction costs in return space from turnover.

    Turnover is absolute change in weights. Costs are in basis points.
    """
    total_bps = slippage_bps + commission_bps
    return turnover * (total_bps / 10_000.0)
