"""Volatility-managed dual momentum sleeve with defensive cash-like fallback."""

from __future__ import annotations

from collections.abc import Iterable
import math

import numpy as np
import pandas as pd

from trading_codex.strategies.base import Strategy


class DualMomentumVol10CashStrategy(Strategy):
    """Select the strongest risk asset, then cap its sleeve weight by realized vol."""

    _MIN_VALID_VOL = 1e-8

    def __init__(
        self,
        symbols: Iterable[str],
        defensive_symbol: str = "BIL",
        momentum_lookback: int = 63,
        rebalance: int = 21,
        vol_lookback: int = 20,
        target_vol: float = 0.10,
        annualization: int = 252,
    ) -> None:
        risk_symbols = [str(symbol).strip() for symbol in symbols if str(symbol).strip()]
        defensive = str(defensive_symbol).strip()

        if not risk_symbols:
            raise ValueError("symbols must not be empty.")
        if not defensive:
            raise ValueError("defensive_symbol must not be empty.")
        if momentum_lookback <= 0:
            raise ValueError("momentum_lookback must be > 0.")
        if rebalance <= 0:
            raise ValueError("rebalance must be > 0.")
        if vol_lookback <= 0:
            raise ValueError("vol_lookback must be > 0.")
        if target_vol <= 0:
            raise ValueError("target_vol must be > 0.")
        if annualization <= 0:
            raise ValueError("annualization must be > 0.")

        self.risk_symbols = [symbol for symbol in dict.fromkeys(risk_symbols) if symbol != defensive]
        self.defensive_symbol = defensive
        self.momentum_lookback = int(momentum_lookback)
        self.rebalance = int(rebalance)
        self.vol_lookback = int(vol_lookback)
        self.target_vol = float(target_vol)
        self.annualization = int(annualization)

        if not self.risk_symbols:
            raise ValueError("symbols must include at least one risk asset distinct from defensive_symbol.")

    @staticmethod
    def _decision_rebalance_mask(index: pd.DatetimeIndex, rebalance: int) -> pd.Series:
        mask = pd.Series(False, index=index, dtype=bool)
        for i in range(rebalance - 1, len(index), rebalance):
            mask.iloc[i] = True
        return mask

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError(
                "DualMomentumVol10CashStrategy expects MultiIndex bars: (symbol, field)."
            )
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        close_panel = bars.xs("close", axis=1, level=1).astype(float)
        universe = self.risk_symbols + [self.defensive_symbol]
        missing = [symbol for symbol in universe if symbol not in close_panel.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        close = close_panel.loc[:, universe]
        momentum = close.pct_change(self.momentum_lookback)
        realized_vol = close.pct_change().rolling(self.vol_lookback).std(ddof=0) * math.sqrt(
            float(self.annualization)
        )
        decision_mask = self._decision_rebalance_mask(bars.index, self.rebalance)

        weights = pd.DataFrame(index=bars.index, columns=universe, dtype=float)

        for i, (dt, is_decision) in enumerate(decision_mask.items()):
            if (not bool(is_decision)) or (i + 1 >= len(bars.index)):
                continue

            next_dt = bars.index[i + 1]
            defensive_momentum = momentum.loc[dt, self.defensive_symbol]
            if pd.isna(defensive_momentum):
                continue

            risk_row = momentum.loc[dt, self.risk_symbols].astype(float).dropna()
            selected_symbol = self.defensive_symbol
            target_weight = 1.0

            if not risk_row.empty:
                best_symbol = str(risk_row.idxmax())
                best_momentum = float(risk_row.loc[best_symbol])
                if best_momentum > float(defensive_momentum):
                    asset_vol = realized_vol.loc[dt, best_symbol]
                    asset_vol_value = float(asset_vol) if pd.notna(asset_vol) else float("nan")
                    if (
                        pd.notna(asset_vol)
                        and np.isfinite(asset_vol)
                        and asset_vol_value > self._MIN_VALID_VOL
                    ):
                        selected_symbol = best_symbol
                        target_weight = min(1.0, float(self.target_vol) / asset_vol_value)
                    else:
                        # Conservative fallback: stay defensive if the risk asset vol estimate is unusable.
                        selected_symbol = self.defensive_symbol
                        target_weight = 1.0

            row_weights = pd.Series(0.0, index=universe, dtype=float)
            row_weights.loc[selected_symbol] = float(target_weight)
            weights.loc[next_dt] = row_weights

        return weights.ffill().fillna(0.0)
