"""Dual momentum rotation strategy v1."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from trading_codex.strategies.base import Strategy


class DualMomentumV1Strategy(Strategy):
    """Top-N dual momentum with absolute filter and defensive fallback."""

    def __init__(
        self,
        symbols: Iterable[str],
        lookback: int = 252,
        top_n: int = 1,
        rebalance: int = 21,
        defensive_symbol: str = "SHY",
    ) -> None:
        self.symbols = [s for s in symbols]
        self.lookback = int(lookback)
        self.top_n = int(top_n)
        self.rebalance = int(rebalance)
        self.defensive_symbol = defensive_symbol

        if not self.symbols:
            raise ValueError("symbols must not be empty.")
        if self.lookback <= 0:
            raise ValueError("lookback must be > 0.")
        if self.top_n <= 0:
            raise ValueError("top_n must be > 0.")
        if self.rebalance <= 0:
            raise ValueError("rebalance must be > 0.")
        if not self.defensive_symbol:
            raise ValueError("defensive_symbol must not be empty.")

    def _decision_rebalance_mask(self, index: pd.DatetimeIndex) -> pd.Series:
        mask = pd.Series(False, index=index, dtype=bool)
        for i in range(self.rebalance - 1, len(index), self.rebalance):
            mask.iloc[i] = True
        return mask

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError("DualMomentumV1Strategy expects MultiIndex bars: (symbol, field).")
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        close_panel = bars.xs("close", axis=1, level=1).astype(float)
        if self.defensive_symbol not in close_panel.columns:
            raise ValueError(
                f"Missing close prices for defensive symbol: {self.defensive_symbol}"
            )

        # Strategy universe includes all configured symbols (risk + defensive).
        universe = list(dict.fromkeys(self.symbols))
        missing = [s for s in universe if s not in close_panel.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        if self.defensive_symbol not in universe:
            universe.append(self.defensive_symbol)

        risk_symbols = [s for s in universe if s != self.defensive_symbol]
        close = close_panel.loc[:, universe]
        momentum = close.loc[:, risk_symbols].pct_change(self.lookback)
        decision_mask = self._decision_rebalance_mask(bars.index)

        weights = pd.DataFrame(index=bars.index, columns=universe, dtype=float)

        for i, (dt, is_decision) in enumerate(decision_mask.items()):
            if not bool(is_decision) or i + 1 >= len(bars.index):
                continue
            next_dt = bars.index[i + 1]

            row = momentum.loc[dt].reindex(risk_symbols).dropna()
            if row.empty:
                selected: list[str] = []
            else:
                top = row.sort_values(ascending=False).head(self.top_n)
                selected = [s for s, v in top.items() if float(v) > 0.0]

            w = pd.Series(0.0, index=universe, dtype=float)
            if selected:
                alloc = 1.0 / float(len(selected))
                for s in selected:
                    w.loc[s] = alloc
            else:
                w.loc[self.defensive_symbol] = 1.0

            weights.loc[next_dt] = w

        weights = weights.ffill().fillna(0.0)
        return weights
