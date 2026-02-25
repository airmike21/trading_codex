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
    leverage: pd.Series | None = None
    realized_vol: pd.Series | None = None


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


def _apply_vol_target_overlay(
    base_weights: pd.Series | pd.DataFrame,
    asset_returns: pd.Series | pd.DataFrame,
    target_vol: float,
    lookback: int,
    min_lev: float,
    max_lev: float,
    update_mask: pd.Series | None = None,
) -> tuple[pd.Series | pd.DataFrame, pd.Series, pd.Series]:
    if isinstance(base_weights, pd.DataFrame):
        portfolio_returns = (base_weights * asset_returns).sum(axis=1)
    else:
        portfolio_returns = base_weights * asset_returns

    realized_vol = portfolio_returns.rolling(lookback).std().shift(1) * (252.0**0.5)
    leverage_daily = pd.Series(0.0, index=portfolio_returns.index, dtype=float)

    valid = realized_vol.notna() & (realized_vol > 0.0)
    leverage_daily.loc[valid] = (target_vol / realized_vol.loc[valid]).clip(
        lower=min_lev, upper=max_lev
    )

    if update_mask is None:
        leverage = leverage_daily
    else:
        aligned_mask = update_mask.reindex(leverage_daily.index).fillna(False).astype(bool)
        leverage = leverage_daily.where(aligned_mask).ffill().fillna(0.0)

    if isinstance(base_weights, pd.DataFrame):
        scaled_weights = base_weights.mul(leverage, axis=0)
    else:
        scaled_weights = base_weights * leverage

    return scaled_weights, leverage, realized_vol


def _calendar_rebalance_update_mask(index: pd.DatetimeIndex, rebalance_cadence: str) -> pd.Series:
    cadence = rebalance_cadence.upper()
    if cadence == "M":
        periods = pd.Series(index.to_period("M"), index=index)
        rebalance_day = periods.ne(periods.shift(-1)).fillna(False)
    elif cadence == "W":
        rebalance_day = pd.Series(index.weekday == 4, index=index, dtype=bool)
    else:
        raise ValueError("rebalance_cadence must be one of {'M', 'W'}.")

    update_mask = pd.Series(False, index=index, dtype=bool)
    for idx_pos in range(len(index) - 1):
        if bool(rebalance_day.iloc[idx_pos]):
            update_mask.iloc[idx_pos + 1] = True
    return update_mask


def run_backtest(
    bars: pd.DataFrame,
    strategy: Strategy,
    slippage_bps: float = 1.0,
    commission_bps: float = 0.0,
    vol_target: float | None = None,
    vol_lookback: int = 20,
    vol_min: float = 0.0,
    vol_max: float = 1.0,
    vol_update: str = "rebalance",
    rebalance_cadence: str = "M",
) -> BacktestResult:
    if vol_target is not None:
        if vol_target < 0:
            raise ValueError("vol_target must be >= 0 when provided.")
        if vol_lookback <= 0:
            raise ValueError("vol_lookback must be > 0.")
        if vol_min < 0:
            raise ValueError("vol_min must be >= 0.")
        if vol_max < 0:
            raise ValueError("vol_max must be >= 0.")
        if vol_min > vol_max:
            raise ValueError("vol_min must be <= vol_max.")
        if vol_update not in {"rebalance", "daily"}:
            raise ValueError("vol_update must be one of {'rebalance', 'daily'}.")
        if rebalance_cadence.upper() not in {"M", "W"}:
            raise ValueError("rebalance_cadence must be one of {'M', 'W'}.")

    if _is_multi_asset_bars(bars):
        _validate_multi_asset_bars(bars)

        symbols = bars.columns.get_level_values(0).unique().tolist()
        signals = strategy.generate_signals(bars)
        base_weights = _as_weight_frame(signals, bars.index, symbols)

        close = bars.xs("close", axis=1, level=1).loc[:, symbols].astype(float)
        rets = close.pct_change().fillna(0.0)

        leverage: pd.Series | None = None
        realized_vol: pd.Series | None = None
        if vol_target is not None:
            update_mask = (
                _calendar_rebalance_update_mask(rets.index, rebalance_cadence)
                if vol_update == "rebalance"
                else None
            )
            weights, leverage, realized_vol = _apply_vol_target_overlay(
                base_weights,
                rets,
                target_vol=float(vol_target),
                lookback=int(vol_lookback),
                min_lev=float(vol_min),
                max_lev=float(vol_max),
                update_mask=update_mask,
            )
        else:
            weights = base_weights

        turnover_weights = (
            base_weights if (vol_target is not None and vol_update == "rebalance") else weights
        )
        turnover = turnover_weights.diff().abs().sum(axis=1).fillna(0.0)
        costs = bps_cost(turnover, slippage_bps=slippage_bps, commission_bps=commission_bps)

        strategy_returns = (weights * rets).sum(axis=1) - costs
        equity = (1 + strategy_returns).cumprod()

        return BacktestResult(
            returns=strategy_returns,
            weights=weights,
            turnover=turnover,
            equity=equity,
            leverage=leverage,
            realized_vol=realized_vol,
        )

    validate_bars(bars)

    signals = strategy.generate_signals(bars)
    validate_signals(signals)

    signals = signals.reindex(bars.index).fillna(0.0)
    base_weights = signals["signal"].clip(-1.0, 1.0).astype(float)

    close = bars["close"].astype(float)
    rets = close.pct_change().fillna(0.0)

    leverage = None
    realized_vol = None
    if vol_target is not None:
        update_mask = (
            _calendar_rebalance_update_mask(rets.index, rebalance_cadence)
            if vol_update == "rebalance"
            else None
        )
        weights, leverage, realized_vol = _apply_vol_target_overlay(
            base_weights,
            rets,
            target_vol=float(vol_target),
            lookback=int(vol_lookback),
            min_lev=float(vol_min),
            max_lev=float(vol_max),
            update_mask=update_mask,
        )
    else:
        weights = base_weights

    turnover_weights = base_weights if (vol_target is not None and vol_update == "rebalance") else weights
    turnover = turnover_weights.diff().abs().fillna(0.0)
    costs = bps_cost(turnover, slippage_bps=slippage_bps, commission_bps=commission_bps)

    strategy_returns = (weights * rets) - costs
    equity = (1 + strategy_returns).cumprod()

    return BacktestResult(
        returns=strategy_returns,
        weights=weights,
        turnover=turnover,
        equity=equity,
        leverage=leverage,
        realized_vol=realized_vol,
    )
