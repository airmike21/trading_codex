"""Value + momentum composite rotation strategy v1."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from trading_codex.strategies.base import Strategy


class ValueMomentumV1Strategy(Strategy):
    """Top-N value+momentum composite with absolute filter and defensive fallback."""

    def __init__(
        self,
        symbols: Iterable[str],
        mom_lookback: int = 252,
        val_lookback: int = 1260,
        top_n: int = 1,
        rebalance: int = 21,
        defensive_symbol: str = "SHY",
        mom_weight: float = 1.0,
        val_weight: float = 1.0,
    ) -> None:
        self.symbols = [s for s in symbols]
        self.mom_lookback = int(mom_lookback)
        self.val_lookback = int(val_lookback)
        self.top_n = int(top_n)
        self.rebalance = int(rebalance)
        self.defensive_symbol = defensive_symbol
        self.mom_weight = float(mom_weight)
        self.val_weight = float(val_weight)

        if not self.symbols:
            raise ValueError("symbols must not be empty.")
        if self.mom_lookback <= 0:
            raise ValueError("mom_lookback must be > 0.")
        if self.val_lookback <= 0:
            raise ValueError("val_lookback must be > 0.")
        if self.top_n <= 0:
            raise ValueError("top_n must be > 0.")
        if self.rebalance <= 0:
            raise ValueError("rebalance must be > 0.")
        if not self.defensive_symbol:
            raise ValueError("defensive_symbol must not be empty.")

    @staticmethod
    def _decision_rebalance_mask(index: pd.DatetimeIndex, rebalance: int) -> pd.Series:
        mask = pd.Series(False, index=index, dtype=bool)
        for i in range(rebalance - 1, len(index), rebalance):
            mask.iloc[i] = True
        return mask

    @staticmethod
    def _zscore(values: pd.Series) -> pd.Series:
        if values.empty:
            return values.astype(float)
        std = float(values.std(ddof=0))
        if (not np.isfinite(std)) or std == 0.0:
            return pd.Series(0.0, index=values.index, dtype=float)
        mean = float(values.mean())
        return ((values - mean) / std).astype(float)

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError("ValueMomentumV1Strategy expects MultiIndex bars: (symbol, field).")
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        close_panel = bars.xs("close", axis=1, level=1).astype(float)
        if self.defensive_symbol not in close_panel.columns:
            raise ValueError(
                f"Missing close prices for defensive symbol: {self.defensive_symbol}"
            )

        risk_symbols = [s for s in list(dict.fromkeys(self.symbols)) if s != self.defensive_symbol]
        if not risk_symbols:
            raise ValueError("symbols must include at least one risk symbol.")

        missing = [s for s in risk_symbols if s not in close_panel.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        universe = risk_symbols + [self.defensive_symbol]
        close = close_panel.loc[:, universe]

        mom = (close.loc[:, risk_symbols] / close.loc[:, risk_symbols].shift(self.mom_lookback)) - 1.0
        val = -((close.loc[:, risk_symbols] / close.loc[:, risk_symbols].shift(self.val_lookback)) - 1.0)
        decision_mask = self._decision_rebalance_mask(bars.index, self.rebalance)

        weights = pd.DataFrame(index=bars.index, columns=universe, dtype=float)

        for i, (dt, is_decision) in enumerate(decision_mask.items()):
            if (not bool(is_decision)) or (i + 1 >= len(bars.index)):
                continue

            mom_row = mom.loc[dt].reindex(risk_symbols)
            val_row = val.loc[dt].reindex(risk_symbols)
            valid = mom_row.notna() & val_row.notna()
            if not bool(valid.any()):
                # Warmup period: keep zeros until first valid rebalance decision.
                continue

            mom_valid = mom_row.loc[valid].astype(float)
            val_valid = val_row.loc[valid].astype(float)

            z_mom = self._zscore(mom_valid)
            z_val = self._zscore(val_valid)
            score = (self.mom_weight * z_mom) + (self.val_weight * z_val)

            eligible = mom_valid.index[mom_valid > 0.0]
            selected: list[str] = []
            if len(eligible):
                selected = list(score.loc[eligible].sort_values(ascending=False).head(self.top_n).index)

            w = pd.Series(0.0, index=universe, dtype=float)
            if selected:
                alloc = 1.0 / float(len(selected))
                for symbol in selected:
                    w.loc[symbol] = alloc
            else:
                w.loc[self.defensive_symbol] = 1.0

            next_dt = bars.index[i + 1]
            weights.loc[next_dt] = w

        weights = weights.ffill().fillna(0.0)
        return weights
