"""SMA200 regime strategy (risk-on / risk-off rotation)."""

from __future__ import annotations

import pandas as pd

from trading_codex.strategies.base import Strategy


class Sma200RegimeStrategy(Strategy):
    """Rotate between risk and defensive assets using lagged SMA regime."""

    def __init__(
        self,
        risk_symbol: str = "SPY",
        defensive: str | None = "TLT",
        sma_window: int = 200,
        rebalance: str = "M",
    ) -> None:
        self.risk_symbol = risk_symbol
        self.defensive = defensive if defensive else None
        self.sma_window = int(sma_window)
        self.rebalance = rebalance.upper()

        if not self.risk_symbol:
            raise ValueError("risk_symbol must not be empty.")
        if self.sma_window <= 0:
            raise ValueError("sma_window must be > 0.")
        if self.rebalance not in {"M", "W"}:
            raise ValueError(f"Unsupported rebalance frequency: {rebalance}")

    def _rebalance_mask(self, index: pd.DatetimeIndex) -> pd.Series:
        if self.rebalance == "M":
            periods = index.to_period("M")
        else:
            periods = index.to_period("W-FRI")
        period_series = pd.Series(periods, index=index)
        return period_series.ne(period_series.shift(-1)).fillna(True)

    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        if not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2:
            raise ValueError("Sma200RegimeStrategy expects MultiIndex bars: (symbol, field).")
        if "close" not in bars.columns.get_level_values(1):
            raise ValueError("Bars must include close field for all symbols.")

        symbols = [self.risk_symbol] + ([self.defensive] if self.defensive else [])
        close_panel = bars.xs("close", axis=1, level=1)
        missing = [sym for sym in symbols if sym not in close_panel.columns]
        if missing:
            raise ValueError(f"Missing close prices for symbols: {missing}")

        risk_close = close_panel[self.risk_symbol].astype(float)
        sma = risk_close.rolling(self.sma_window).mean()
        risk_on = (risk_close.shift(1) > sma.shift(1)).fillna(False)

        rebalance_mask = self._rebalance_mask(bars.index)
        selected = pd.Series(index=bars.index, dtype=object)
        cash_state = "CASH"

        for idx_pos, (dt, is_rebalance) in enumerate(rebalance_mask.items()):
            if not is_rebalance or idx_pos + 1 >= len(bars.index):
                continue

            next_dt = bars.index[idx_pos + 1]
            if bool(risk_on.loc[dt]):
                selected.loc[next_dt] = self.risk_symbol
            elif self.defensive:
                selected.loc[next_dt] = self.defensive
            else:
                selected.loc[next_dt] = cash_state

        selected = selected.ffill()

        weights = pd.DataFrame(0.0, index=bars.index, columns=symbols, dtype=float)
        for symbol in symbols:
            weights.loc[selected == symbol, symbol] = 1.0
        return weights
