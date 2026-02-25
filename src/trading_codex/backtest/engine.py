"""Simple daily-bar backtest engine."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from trading_codex.backtest.costs import bps_cost
from trading_codex.data.contracts import BAR_COLUMNS, validate_bars, validate_signals
from trading_codex.strategies.base import Strategy


@dataclass
class BacktestResult:
    returns: pd.Series
    weights: pd.Series | pd.DataFrame
    turnover: pd.Series
    equity: pd.Series


def _is_multi_asset_bars(bars: pd.DataFrame) -> bool:
    return isinstance(bars.columns, pd.MultiIndex) and bars.columns.nlevels == 2


def _validate_multi_asset_bars(bars: pd.DataFrame) -> None:
    if not _is_multi_asset_bars(bars):
        raise ValueError("Expected MultiIndex bars with columns (symbol, field).")

    fields = bars.columns.get_level_values(1)
    missing_fields = [field for field in BAR_COLUMNS if field not in fields]
    if missing_fields:
        raise ValueError(f"Multi-asset bars missing fields: {missing_fields}")

    symbols = bars.columns.get_level_values(0).unique().tolist()
    for sym in symbols:
        sym_fields = bars[sym].columns.tolist()
        missing_for_symbol = [field for field in BAR_COLUMNS if field not in sym_fields]
        if missing_for_symbol:
            raise ValueError(f"Bars missing fields for {sym}: {missing_for_symbol}")


def _as_weight_frame(
    signals: pd.Series | pd.DataFrame,
    index: pd.DatetimeIndex,
    symbols: list[str],
) -> pd.DataFrame:
    if isinstance(signals, pd.Series):
        signal_df = signals.to_frame(name=symbols[0])
    else:
        signal_df = signals.copy()
        if "signal" in signal_df.columns and len(symbols) == 1:
            signal_df = signal_df.rename(columns={"signal": symbols[0]})

    signal_df = signal_df.reindex(index=index)
    signal_df = signal_df.reindex(columns=symbols).fillna(0.0)
    return signal_df.clip(-1.0, 1.0).astype(float)


def run_backtest(
    bars: pd.DataFrame,
    strategy: Strategy,
    slippage_bps: float = 1.0,
    commission_bps: float = 0.0,
) -> BacktestResult:
    if _is_multi_asset_bars(bars):
        _validate_multi_asset_bars(bars)

        symbols = bars.columns.get_level_values(0).unique().tolist()
        signals = strategy.generate_signals(bars)
        weights = _as_weight_frame(signals, bars.index, symbols)

        close = bars.xs("close", axis=1, level=1).loc[:, symbols].astype(float)
        rets = close.pct_change().fillna(0.0)

        turnover = weights.diff().abs().sum(axis=1).fillna(0.0)
        costs = bps_cost(turnover, slippage_bps=slippage_bps, commission_bps=commission_bps)

        strategy_returns = (weights * rets).sum(axis=1) - costs
        equity = (1 + strategy_returns).cumprod()

        return BacktestResult(
            returns=strategy_returns,
            weights=weights,
            turnover=turnover,
            equity=equity,
        )

    validate_bars(bars)

    signals = strategy.generate_signals(bars)
    validate_signals(signals)

    signals = signals.reindex(bars.index).fillna(0.0)
    weights = signals["signal"].clip(-1.0, 1.0).astype(float)

    close = bars["close"].astype(float)
    rets = close.pct_change().fillna(0.0)

    turnover = weights.diff().abs().fillna(0.0)
    costs = bps_cost(turnover, slippage_bps=slippage_bps, commission_bps=commission_bps)

    strategy_returns = (weights * rets) - costs
    equity = (1 + strategy_returns).cumprod()

    return BacktestResult(
        returns=strategy_returns,
        weights=weights,
        turnover=turnover,
        equity=equity,
    )
