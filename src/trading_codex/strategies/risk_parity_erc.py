"""Risk parity / Equal Risk Contribution strategy."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from trading_codex.backtest.allocations import RiskParityConfig, erc_weight_series
from trading_codex.strategies.base import Strategy


class RiskParityERCStrategy(Strategy):
    """Long-only equal-risk-contribution rotation over a fixed universe."""

    def __init__(
        self,
        symbols: Iterable[str],
        lookback: int = 63,
        rebalance: str = "M",
        max_iter: int = 200,
        tol: float = 1e-8,
    ) -> None:
        self.symbols = [symbol for symbol in symbols]
        self.lookback = int(lookback)
        self.rebalance = rebalance.upper()
        self.max_iter = int(max_iter)
        self.tol = float(tol)

        if not self.symbols:
            raise ValueError("symbols must not be empty.")
        if self.lookback <= 1:
            raise ValueError("lookback must be > 1.")
        if self.max_iter <= 0:
            raise ValueError("max_iter must be > 0.")
        if self.tol <= 0.0:
            raise ValueError("tol must be > 0.")
        if self.rebalance not in {"M", "W"}:
            raise ValueError(f"Unsupported rebalance frequency: {rebalance}")

    def _decision_rebalance_mask(self, index: pd.DatetimeIndex) -> pd.Series:
        if self.rebalance == "M":
            periods = index.to_period("M")
        else:
            periods = index.to_period("W-FRI")
        period_series = pd.Series(periods, index=index)
        return period_series.ne(period_series.shift(-1)).fillna(True)

    @staticmethod
    def _apply_next_bar(mask: pd.Series) -> pd.Series:
        update_mask = pd.Series(False, index=mask.index, dtype=bool)
        for idx_pos in range(len(mask.index) - 1):
            if bool(mask.iloc[idx_pos]):
                update_mask.iloc[idx_pos + 1] = True
        return update_mask

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError("RiskParityERCStrategy expects MultiIndex bars: (symbol, field).")
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        close_panel = bars.xs("close", axis=1, level=1).astype(float)
        missing = [symbol for symbol in self.symbols if symbol not in close_panel.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")
        close = close_panel.loc[:, self.symbols]
        returns = close.pct_change().fillna(0.0)

        decision_mask = self._decision_rebalance_mask(bars.index)
        update_mask = self._apply_next_bar(decision_mask)
        cfg = RiskParityConfig(
            lookback=self.lookback,
            max_iter=self.max_iter,
            tol=self.tol,
        )
        return erc_weight_series(returns, update_mask, cfg)
