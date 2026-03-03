"""Cross-sectional momentum (relative strength) strategy v1."""

from __future__ import annotations

from collections.abc import Iterable

import pandas as pd

from trading_codex.strategies.base import Strategy


class CrossSectionalMomentumV1Strategy(Strategy):
    """Equal-weight top-N relative strength with optional defensive fallback."""

    def __init__(
        self,
        symbols: Iterable[str],
        lookback: int = 252,
        top_n: int = 1,
        rebalance: str = "M",
        defensive: str | None = "TLT",
    ) -> None:
        self.symbols = [s for s in symbols]
        self.lookback = int(lookback)
        self.top_n = int(top_n)
        self.rebalance = rebalance.upper()
        self.defensive = defensive if defensive else None

        if not self.symbols:
            raise ValueError("symbols must not be empty.")
        if self.lookback <= 0:
            raise ValueError("lookback must be > 0.")
        if self.top_n <= 0:
            raise ValueError("top_n must be > 0.")
        if self.rebalance not in {"M", "W"}:
            raise ValueError(f"Unsupported rebalance frequency: {rebalance}")

    def _decision_rebalance_mask(self, index: pd.DatetimeIndex) -> pd.Series:
        if self.rebalance == "M":
            periods = index.to_period("M")
        else:
            periods = index.to_period("W-FRI")
        ps = pd.Series(periods, index=index)
        # True on last bar of each period (decision date)
        return ps.ne(ps.shift(-1)).fillna(True)

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError("CrossSectionalMomentumV1Strategy expects MultiIndex bars: (symbol, field).")
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        all_symbols = list(dict.fromkeys(self.symbols + ([self.defensive] if self.defensive else [])))
        close_panel = bars.xs("close", axis=1, level=1).astype(float)

        missing = [s for s in all_symbols if s not in close_panel.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        close = close_panel.loc[:, all_symbols]
        mom = close.loc[:, self.symbols].pct_change(self.lookback)
        decision_mask = self._decision_rebalance_mask(bars.index)

        weights = pd.DataFrame(index=bars.index, columns=all_symbols, dtype=float)

        for i, (dt, is_decision) in enumerate(decision_mask.items()):
            if (not bool(is_decision)) or (i + 1 >= len(bars.index)):
                continue
            next_dt = bars.index[i + 1]

            row = mom.loc[dt].reindex(self.symbols)
            row = row.dropna()
            if row.empty:
                selected: list[str] = []
            else:
                selected = list(row.sort_values(ascending=False).head(self.top_n).index)

            # Absolute filter: require > 0
            selected = [s for s in selected if float(mom.loc[dt, s]) > 0.0] if selected else []

            w = pd.Series(0.0, index=all_symbols, dtype=float)
            if selected:
                alloc = 1.0 / float(len(selected))
                for s in selected:
                    w.loc[s] = alloc
            elif self.defensive:
                w.loc[self.defensive] = 1.0

            weights.loc[next_dt] = w

        weights = weights.ffill().fillna(0.0)
        return weights
