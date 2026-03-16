"""Simple daily-bar backtest engine."""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from trading_codex.backtest.costs import compute_trade_count, compute_turnover, estimate_transaction_costs
from trading_codex.backtest.vol_overlay import apply_vol_target_overlay
from trading_codex.data.contracts import BAR_COLUMNS, validate_bars, validate_signals
from trading_codex.overlays.ivol_overlay import apply_inverse_vol_overlay
from trading_codex.strategies.base import Strategy


@dataclass
class BacktestResult:
    returns: pd.Series
    weights: pd.Series | pd.DataFrame
    turnover: pd.Series
    equity: pd.Series
    gross_returns: pd.Series | None = None
    gross_equity: pd.Series | None = None
    cost_returns: pd.Series | None = None
    estimated_costs: pd.Series | None = None
    trade_count: pd.Series | None = None
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


def _rebalance_update_mask(index: pd.DatetimeIndex, rebalance_cadence: str | int) -> pd.Series:
    if isinstance(rebalance_cadence, int):
        if rebalance_cadence <= 0:
            raise ValueError("rebalance_cadence must be > 0 when provided as trading days.")
        update_mask = pd.Series(False, index=index, dtype=bool)
        for idx_pos in range(int(rebalance_cadence) - 1, len(index) - 1, int(rebalance_cadence)):
            update_mask.iloc[idx_pos + 1] = True
        return update_mask

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
    slippage_bps: float = 5.0,
    commission_bps: float = 0.0,
    commission_per_trade: float = 0.0,
    vol_target: float | None = None,
    vol_lookback: int = 63,
    vol_min: float = 0.0,
    vol_max: float = 1.0,
    vol_update: str = "rebalance",
    rebalance_cadence: str | int = "M",
    ivol: bool = False,
    ivol_lookback: int = 63,
    ivol_eps: float = 1e-8,
) -> BacktestResult:
    if ivol:
        if ivol_lookback <= 0:
            raise ValueError("ivol_lookback must be > 0 when --ivol is enabled.")
        if ivol_eps <= 0:
            raise ValueError("ivol_eps must be > 0 when --ivol is enabled.")
    if slippage_bps < 0:
        raise ValueError("slippage_bps must be >= 0.")
    if commission_bps < 0:
        raise ValueError("commission_bps must be >= 0.")
    if commission_per_trade < 0:
        raise ValueError("commission_per_trade must be >= 0.")

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
        if isinstance(rebalance_cadence, int):
            if rebalance_cadence <= 0:
                raise ValueError("rebalance_cadence must be > 0 when provided as trading days.")
        elif rebalance_cadence.upper() not in {"M", "W"}:
            raise ValueError("rebalance_cadence must be one of {'M', 'W'} or a positive trading-day interval.")

    if _is_multi_asset_bars(bars):
        _validate_multi_asset_bars(bars)

        symbols = bars.columns.get_level_values(0).unique().tolist()
        signals = strategy.generate_signals(bars)
        base_weights = _as_weight_frame(signals, bars.index, symbols)
        if ivol:
            # Relative ivol sizing always runs before optional portfolio-level vol targeting.
            base_weights = apply_inverse_vol_overlay(
                bars,
                base_weights,
                lookback=int(ivol_lookback),
                eps=float(ivol_eps),
            )

        close = bars.xs("close", axis=1, level=1).loc[:, symbols].astype(float)
        rets = close.pct_change().fillna(0.0)

        leverage: pd.Series | None = None
        realized_vol: pd.Series | None = None
        if vol_target is not None:
            update_mask = (
                _rebalance_update_mask(rets.index, rebalance_cadence)
                if vol_update == "rebalance"
                else None
            )
            weights, leverage, realized_vol = apply_vol_target_overlay(
                base_weights,
                rets,
                target_vol=float(vol_target),
                lookback=int(vol_lookback),
                min_leverage=float(vol_min),
                max_leverage=float(vol_max),
                update_mask=update_mask,
            )
        else:
            weights = base_weights

        turnover_weights = (
            base_weights if (vol_target is not None and vol_update == "rebalance") else weights
        )
        turnover = compute_turnover(turnover_weights)
        trade_count = compute_trade_count(turnover_weights)
        gross_returns = (weights * rets).sum(axis=1).astype(float)
        cost_estimate = estimate_transaction_costs(
            turnover,
            trade_count,
            slippage_bps=slippage_bps,
            commission_bps=commission_bps,
            commission_per_trade=commission_per_trade,
        )
        strategy_returns = gross_returns - cost_estimate.cost_return
        equity = (1 + strategy_returns).cumprod()
        gross_equity = (1 + gross_returns).cumprod()

        return BacktestResult(
            returns=strategy_returns,
            weights=weights,
            turnover=turnover,
            equity=equity,
            gross_returns=gross_returns,
            gross_equity=gross_equity,
            cost_returns=cost_estimate.cost_return,
            estimated_costs=cost_estimate.total_cost,
            trade_count=cost_estimate.trade_count,
            leverage=leverage,
            realized_vol=realized_vol,
        )

    validate_bars(bars)
    if ivol:
        raise ValueError("--ivol is only supported for multi-asset strategies.")

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
            _rebalance_update_mask(rets.index, rebalance_cadence)
            if vol_update == "rebalance"
            else None
        )
        weights, leverage, realized_vol = apply_vol_target_overlay(
            base_weights,
            rets,
            target_vol=float(vol_target),
            lookback=int(vol_lookback),
            min_leverage=float(vol_min),
            max_leverage=float(vol_max),
            update_mask=update_mask,
        )
    else:
        weights = base_weights

    turnover_weights = base_weights if (vol_target is not None and vol_update == "rebalance") else weights
    turnover = compute_turnover(turnover_weights)
    trade_count = compute_trade_count(turnover_weights)
    gross_returns = (weights * rets).astype(float)
    cost_estimate = estimate_transaction_costs(
        turnover,
        trade_count,
        slippage_bps=slippage_bps,
        commission_bps=commission_bps,
        commission_per_trade=commission_per_trade,
    )

    strategy_returns = gross_returns - cost_estimate.cost_return
    equity = (1 + strategy_returns).cumprod()
    gross_equity = (1 + gross_returns).cumprod()

    return BacktestResult(
        returns=strategy_returns,
        weights=weights,
        turnover=turnover,
        equity=equity,
        gross_returns=gross_returns,
        gross_equity=gross_equity,
        cost_returns=cost_estimate.cost_return,
        estimated_costs=cost_estimate.total_cost,
        trade_count=cost_estimate.trade_count,
        leverage=leverage,
        realized_vol=realized_vol,
    )
