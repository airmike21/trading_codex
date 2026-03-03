"""Multi-asset time-series momentum strategy (v1)."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from trading_codex.strategies.base import Strategy


class TimeSeriesMomentumV1Strategy(Strategy):
    """Equal-weight long-only TSMOM with optional defensive fallback."""

    def __init__(
        self,
        symbols: Iterable[str],
        lookback: int = 252,
        rebalance: str = "M",
        defensive: str | None = "TLT",
    ) -> None:
        self.symbols = [symbol for symbol in symbols]
        self.lookback = int(lookback)
        self.rebalance = rebalance.upper()
        self.defensive = defensive if defensive else None

        if not self.symbols:
            raise ValueError("symbols must not be empty.")
        if self.lookback <= 0:
            raise ValueError("lookback must be > 0.")
        if self.rebalance not in {"M", "W"}:
            raise ValueError(f"Unsupported rebalance frequency: {rebalance}")

    def _decision_rebalance_mask(self, index: pd.DatetimeIndex) -> pd.Series:
        if self.rebalance == "M":
            periods = index.to_period("M")
        else:
            periods = index.to_period("W-FRI")
        period_series = pd.Series(periods, index=index)
        return period_series.ne(period_series.shift(-1)).fillna(True)

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError("TimeSeriesMomentumV1Strategy expects MultiIndex bars: (symbol, field).")
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        all_symbols = list(dict.fromkeys(self.symbols + ([self.defensive] if self.defensive else [])))
        close_panel = bars.xs("close", axis=1, level=1).astype(float)
        missing = [symbol for symbol in all_symbols if symbol not in close_panel.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        close = close_panel.loc[:, all_symbols]
        momentum = close.loc[:, self.symbols].pct_change(self.lookback)
        decision_mask = self._decision_rebalance_mask(bars.index)

        weights = pd.DataFrame(index=bars.index, columns=all_symbols, dtype=float)
        for idx_pos, (dt, is_rebalance) in enumerate(decision_mask.items()):
            if not is_rebalance or idx_pos + 1 >= len(bars.index):
                continue
            next_dt = bars.index[idx_pos + 1]

            mom_row = momentum.loc[dt].reindex(self.symbols)
            eligible = [
                symbol
                for symbol, value in mom_row.items()
                if pd.notna(value) and float(value) > 0.0
            ]

            w = pd.Series(0.0, index=all_symbols, dtype=float)
            if eligible:
                alloc = 1.0 / float(len(eligible))
                for symbol in eligible:
                    w.loc[symbol] = alloc
            elif self.defensive:
                w.loc[self.defensive] = 1.0

            weights.loc[next_dt] = w

        weights = weights.ffill().fillna(0.0)
        return weights
