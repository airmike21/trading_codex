"""Dual momentum rotation strategy."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from trading_codex.strategies.base import Strategy


class DualMomentumStrategy(Strategy):
    """Monthly/weekly dual momentum rotation with optional defensive sleeve."""

    def __init__(
        self,
        risk_universe: Iterable[str] = ("SPY", "QQQ", "IWM", "EFA"),
        defensive: str | None = "TLT",
        lookback: int = 252,
        rebalance: str = "M",
    ) -> None:
        self.risk_universe = [sym for sym in risk_universe]
        self.defensive = defensive if defensive else None
        self.lookback = int(lookback)
        self.rebalance = rebalance.upper()
        if self.rebalance not in {"M", "W"}:
            raise ValueError(f"Unsupported rebalance frequency: {rebalance}")
        if not self.risk_universe:
            raise ValueError("risk_universe must not be empty.")

    def _rebalance_mask(self, index: pd.DatetimeIndex) -> pd.Series:
        if self.rebalance == "M":
            periods = index.to_period("M")
        else:
            periods = index.to_period("W-FRI")

        period_series = pd.Series(periods, index=index)
        return period_series.ne(period_series.shift(-1)).fillna(True)

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError("DualMomentumStrategy expects MultiIndex bars: (symbol, field).")
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        all_symbols = list(dict.fromkeys(self.risk_universe + ([self.defensive] if self.defensive else [])))
        close = bars.xs("close", axis=1, level=1)
        missing = [sym for sym in all_symbols if sym not in close.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        close = close.loc[:, all_symbols]
        momentum = close.pct_change(self.lookback).shift(1)

        rebalance_mask = self._rebalance_mask(bars.index)
        selected = pd.Series(index=bars.index, dtype=object)

        for idx_pos, (dt, is_rebalance) in enumerate(rebalance_mask.items()):
            if not is_rebalance or idx_pos + 1 >= len(bars.index):
                continue

            next_dt = bars.index[idx_pos + 1]
            risk_mom = momentum.loc[dt, self.risk_universe].dropna()
            chosen: str | None = None

            if not risk_mom.empty:
                best_symbol = risk_mom.idxmax()
                if float(risk_mom.loc[best_symbol]) > 0.0:
                    chosen = best_symbol

            if chosen is None and self.defensive:
                chosen = self.defensive

            selected.loc[next_dt] = chosen

        selected = selected.ffill()

        weights = pd.DataFrame(0.0, index=bars.index, columns=all_symbols, dtype=float)
        for sym in all_symbols:
            weights.loc[selected == sym, sym] = 1.0
        return weights
