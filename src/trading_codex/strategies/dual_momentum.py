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
        regime_gate: str = "none",
        gate_symbol: str = "SPY",
        gate_sma_window: int = 200,
    ) -> None:
        self.risk_universe = [sym for sym in risk_universe]
        self.defensive = defensive if defensive else None
        self.lookback = int(lookback)
        self.rebalance = rebalance.upper()
        self.regime_gate = regime_gate.lower()
        self.gate_symbol = gate_symbol
        self.gate_sma_window = int(gate_sma_window)
        if self.rebalance not in {"M", "W"}:
            raise ValueError(f"Unsupported rebalance frequency: {rebalance}")
        if not self.risk_universe:
            raise ValueError("risk_universe must not be empty.")
        if self.regime_gate not in {"none", "sma200"}:
            raise ValueError(f"Unsupported regime_gate: {regime_gate}")
        if self.regime_gate == "sma200":
            if not self.gate_symbol:
                raise ValueError("gate_symbol must not be empty when regime_gate='sma200'.")
            if self.gate_sma_window <= 0:
                raise ValueError("gate_sma_window must be > 0 when regime_gate='sma200'.")

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
        required_symbols = list(all_symbols)
        if self.regime_gate == "sma200":
            required_symbols.append(self.gate_symbol)

        close = bars.xs("close", axis=1, level=1)
        missing = [sym for sym in required_symbols if sym not in close.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        close = close.loc[:, all_symbols]
        momentum = close.pct_change(self.lookback).shift(1)
        gate_risk_on = pd.Series(True, index=bars.index, dtype=bool)
        if self.regime_gate == "sma200":
            gate_close = bars.xs("close", axis=1, level=1)[self.gate_symbol].astype(float)
            gate_sma = gate_close.rolling(self.gate_sma_window).mean()
            gate_risk_on = (gate_close.shift(1) > gate_sma.shift(1)).fillna(False)

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

            if self.regime_gate == "sma200" and not bool(gate_risk_on.loc[dt]):
                chosen = self.defensive if self.defensive else None

            selected.loc[next_dt] = chosen

        selected = selected.ffill()

        weights = pd.DataFrame(0.0, index=bars.index, columns=all_symbols, dtype=float)
        for sym in all_symbols:
            weights.loc[selected == sym, sym] = 1.0
        return weights
