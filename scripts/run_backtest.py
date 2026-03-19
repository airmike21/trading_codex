"""Run backtests on locally cached daily bars."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from trading_codex.backtest import metrics
from trading_codex.backtest.engine import BacktestResult, run_backtest
from trading_codex.backtest.next_rebalance import compute_next_rebalance_date
from trading_codex.data import LocalStore
from trading_codex.strategies.dual_mom_vol10_cash import DualMomentumVol10CashStrategy
from trading_codex.strategies.dual_mom_v1 import DualMomentumV1Strategy
from trading_codex.strategies.dual_momentum import DualMomentumStrategy
from trading_codex.strategies.risk_parity_erc import RiskParityERCStrategy
from trading_codex.strategies.sma200 import Sma200RegimeStrategy
from trading_codex.strategies.tsmom_v1 import TimeSeriesMomentumV1Strategy
from trading_codex.strategies.valmom_v1 import ValueMomentumV1Strategy
from trading_codex.strategies.xsmom_v1 import CrossSectionalMomentumV1Strategy
from trading_codex.strategies.trend_tsmom import TrendTSMOM

DUAL_MOM_DEFAULT_SYMBOLS = ["SPY", "QQQ", "IWM", "EFA"]
TRACKER_COLUMNS = [
    "date",
    "action",
    "from_symbol",
    "to_symbol",
    "price",
    "shares",
    "notional",
    "cash_after",
    "equity_after",
    "notes",
]


@dataclass(frozen=True)
class RunBacktestConfig:
    rebalance_anchor_date: str | None = None


def _load_toml_dict(path: Path) -> dict[str, object]:
    try:
        import tomllib
    except ModuleNotFoundError:
        import tomli as tomllib  # type: ignore[import-not-found]

    with path.open("rb") as fh:
        payload = tomllib.load(fh)
    return payload if isinstance(payload, dict) else {}


def load_run_backtest_config(config_path: str | os.PathLike[str] | None) -> RunBacktestConfig:
    if not config_path:
        return RunBacktestConfig()

    path = Path(config_path)
    if not path.exists():
        return RunBacktestConfig()

    raw_cfg = _load_toml_dict(path)
    raw_anchor = raw_cfg.get("rebalance_anchor_date")
    if raw_anchor is None:
        return RunBacktestConfig()
    if not isinstance(raw_anchor, str):
        raise ValueError("config key 'rebalance_anchor_date' must be a string or null.")

    anchor = raw_anchor.strip()
    return RunBacktestConfig(
        rebalance_anchor_date=anchor if anchor else None,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run strategy backtests on cached daily bars.")
    parser.add_argument(
        "--strategy",
        choices=[
            "tsmom",
            "dual_mom",
            "sma200",
            "risk_parity_erc",
            "tsmom_v1",
            "xsmom_v1",
            "dual_mom_v1",
            "dual_mom_vol10_cash",
            "valmom_v1",
        ],
        default="tsmom",
    )
    parser.add_argument("--symbol", default="SPY", help="Ticker symbol for single-asset strategy.")
    parser.add_argument("--start", default=None, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=None, help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing cached parquet bars (default: data).",
    )
    parser.add_argument(
        "--config",
        default="config.toml",
        help="Optional TOML config path (default: ./config.toml if present).",
    )
    parser.add_argument(
        "--plot-out",
        default=None,
        help="Save plot to this path instead of showing it interactively.",
    )
    parser.add_argument("--no-plot", action="store_true", help="Skip plotting.")

    parser.add_argument(
        "--lookback",
        type=int,
        default=20,
        help="Trend lookback window in trading days for tsmom (default: 20).",
    )
    parser.add_argument(
        "--long-only",
        action="store_true",
        help="Disable short exposure for tsmom (negative signals become flat).",
    )
    parser.add_argument(
        "--slippage-bps",
        type=float,
        default=1.0,
        help="Slippage in basis points per unit turnover (default: 1.0).",
    )
    parser.add_argument(
        "--commission-bps",
        type=float,
        default=0.5,
        help="Commission in basis points per unit turnover (default: 0.5).",
    )

    parser.add_argument(
        "--symbols",
        nargs="+",
        default=DUAL_MOM_DEFAULT_SYMBOLS,
        help="Risk universe for dual momentum (default: SPY QQQ IWM EFA).",
    )
    parser.add_argument(
        "--defensive",
        default="TLT",
        help='Defensive ETF for dual momentum / sma200. Use "" to disable and rotate to cash.',
    )
    parser.add_argument(
        "--risk-symbol",
        default="SPY",
        help="Risk symbol for sma200 regime strategy (default: SPY).",
    )
    parser.add_argument(
        "--sma-window",
        type=int,
        default=200,
        help="SMA window for sma200 regime strategy (default: 200).",
    )
    parser.add_argument(
        "--mom-lookback",
        type=int,
        default=252,
        help="Momentum lookback in trading days for dual momentum (default: 252).",
    )
    parser.add_argument(
        "--rebalance",
        choices=["M", "W"],
        default="M",
        help="Rebalance cadence for dual momentum / sma200: monthly (M) or weekly (W).",
    )
    parser.add_argument(
        "--regime-gate",
        choices=["none", "sma200"],
        default="none",
        help="Optional regime gate for dual momentum (default: none).",
    )
    parser.add_argument(
        "--gate-symbol",
        default="SPY",
        help="Gate symbol used by regime-gate=sma200 (default: SPY).",
    )
    parser.add_argument(
        "--gate-sma-window",
        type=int,
        default=200,
        help="SMA window for regime-gate=sma200 (default: 200).",
    )
    parser.add_argument(
        "--rp-lookback",
        type=int,
        default=63,
        help="Lookback in trading days for risk parity ERC covariance (default: 63).",
    )
    parser.add_argument(
        "--rp-rebalance",
        choices=["M", "W"],
        default="M",
        help="Rebalance cadence for risk parity ERC: monthly (M) or weekly (W).",
    )
    parser.add_argument(
        "--rp-max-iter",
        type=int,
        default=200,
        help="Maximum iterations for ERC solver updates (default: 200).",
    )
    parser.add_argument(
        "--rp-tol",
        type=float,
        default=1e-8,
        help="Convergence tolerance for ERC solver updates (default: 1e-8).",
    )
    parser.add_argument(
        "--ts-lookback",
        type=int,
        default=252,
        help="Lookback in trading days for tsmom_v1 momentum filter (default: 252).",
    )
    parser.add_argument(
        "--ts-rebalance",
        choices=["M", "W"],
        default="M",
        help="Rebalance cadence for tsmom_v1: monthly (M) or weekly (W).",
    )
    parser.add_argument(
        "--xs-lookback",
        type=int,
        default=252,
        help="Lookback in trading days for xsmom_v1 relative strength (default: 252).",
    )
    parser.add_argument(
        "--xs-top-n",
        type=int,
        default=1,
        help="Top N assets to hold for xsmom_v1 (default: 1).",
    )
    parser.add_argument(
        "--xs-rebalance",
        choices=["M", "W"],
        default="M",
        help="Rebalance cadence for xsmom_v1: monthly (M) or weekly (W).",
    )
    parser.add_argument(
        "--dm-lookback",
        type=int,
        default=252,
        help="Lookback in trading days for dual_mom_v1 momentum filter (default: 252).",
    )
    parser.add_argument(
        "--dm-top-n",
        type=int,
        default=1,
        help="Top N assets to hold for dual_mom_v1 (default: 1).",
    )
    parser.add_argument(
        "--dm-rebalance",
        type=int,
        default=21,
        help="Fixed trading-day rebalance interval for dual_mom_v1 (default: 21).",
    )
    parser.add_argument(
        "--dm-defensive-symbol",
        default="SHY",
        help="Defensive symbol for dual_mom_v1 fallback (default: SHY).",
    )
    parser.add_argument(
        "--dmv-mom-lookback",
        type=int,
        default=63,
        help="Momentum lookback in trading days for dual_mom_vol10_cash (default: 63).",
    )
    parser.add_argument(
        "--dmv-rebalance",
        type=int,
        default=21,
        help="Fixed trading-day rebalance interval for dual_mom_vol10_cash (default: 21).",
    )
    parser.add_argument(
        "--dmv-defensive-symbol",
        default="BIL",
        help="Defensive symbol for dual_mom_vol10_cash fallback (default: BIL).",
    )
    parser.add_argument(
        "--dmv-vol-lookback",
        type=int,
        default=20,
        help="Realized-vol lookback in trading days for dual_mom_vol10_cash (default: 20).",
    )
    parser.add_argument(
        "--dmv-target-vol",
        type=float,
        default=0.10,
        help="Annualized target vol for dual_mom_vol10_cash sizing (default: 0.10).",
    )
    parser.add_argument(
        "--vm-mom-lookback",
        type=int,
        default=252,
        help="Momentum lookback in trading days for valmom_v1 (default: 252).",
    )
    parser.add_argument(
        "--vm-val-lookback",
        type=int,
        default=1260,
        help="Value lookback in trading days for valmom_v1 long-term reversal proxy (default: 1260).",
    )
    parser.add_argument(
        "--vm-top-n",
        type=int,
        default=1,
        help="Top N assets to hold for valmom_v1 (default: 1).",
    )
    parser.add_argument(
        "--vm-rebalance",
        type=int,
        default=21,
        help="Fixed trading-day rebalance interval for valmom_v1 (default: 21).",
    )
    parser.add_argument(
        "--vm-defensive-symbol",
        default="SHY",
        help="Defensive symbol for valmom_v1 fallback (default: SHY).",
    )
    parser.add_argument(
        "--vm-mom-weight",
        type=float,
        default=1.0,
        help="Momentum z-score weight for valmom_v1 composite score (default: 1.0).",
    )
    parser.add_argument(
        "--vm-val-weight",
        type=float,
        default=1.0,
        help="Value z-score weight for valmom_v1 composite score (default: 1.0).",
    )
    parser.add_argument(
        "--rebalance-anchor-date",
        default=None,
        help="Optional YYYY-MM-DD anchor for trading-day next_rebalance schedules.",
    )
    parser.add_argument(
        "--ivol",
        action="store_true",
        help="Enable inverse-volatility weighting overlay for multi-asset strategies.",
    )
    parser.add_argument(
        "--ivol-lookback",
        type=int,
        default=63,
        help="Lookback window for inverse-volatility overlay (default: 63).",
    )
    parser.add_argument(
        "--ivol-eps",
        type=float,
        default=1e-8,
        help="Epsilon floor for inverse-volatility overlay divide-by-zero guard (default: 1e-8).",
    )
    parser.add_argument(
        "--vol-target",
        type=float,
        nargs="?",
        const=0.10,
        default=None,
        help="Optional annualized vol target for overlay sizing. Pass without a value to enable the default 0.10 target.",
    )
    parser.add_argument(
        "--vol-lookback",
        type=int,
        default=63,
        help="Lookback window for realized vol estimate (default: 63).",
    )
    parser.add_argument(
        "--max-leverage",
        "--vol-max",
        dest="max_leverage",
        type=float,
        default=1.0,
        help="Maximum leverage for vol overlay (default: 1.0).",
    )
    parser.add_argument(
        "--min-leverage",
        "--vol-min",
        dest="min_leverage",
        type=float,
        default=0.0,
        help="Minimum leverage for vol overlay (default: 0.0).",
    )
    parser.add_argument(
        "--vol-update",
        choices=["rebalance", "daily"],
        default="rebalance",
        help="Leverage update cadence for vol overlay: rebalance changes or daily.",
    )

    parser.add_argument(
        "--trades-out",
        default=None,
        help="Optional CSV output path for trade/action log.",
    )
    parser.add_argument(
        "--actions-out",
        default=None,
        help="Optional CSV path for backtest actions with share/cash tracker fields.",
    )
    parser.add_argument(
        "--tracker-template-out",
        default=None,
        help="Optional CSV path to write an empty tracker template.",
    )
    parser.add_argument(
        "--metrics-out",
        default=None,
        help="Optional JSON output path for metrics summary.",
    )
    parser.add_argument(
        "--checklist-out",
        default=None,
        help="Optional Markdown output path for dual momentum trade checklist.",
    )
    parser.add_argument(
        "--print-latest",
        action="store_true",
        help="Print latest manual-execution status.",
    )
    next_action_group = parser.add_mutually_exclusive_group()
    next_action_group.add_argument(
        "--next-action",
        action="store_true",
        help="Print a single-line next action alert for reminders/workflow automation.",
    )
    next_action_group.add_argument(
        "--next-action-json",
        action="store_true",
        help="Print a single-line JSON next action payload for automation/parsing.",
    )
    args = parser.parse_args()
    if args.strategy == "dual_mom_vol10_cash":
        if args.vol_target is not None:
            parser.error(
                "--strategy dual_mom_vol10_cash has built-in volatility sizing; "
                "do not combine it with generic --vol-target overlay flags."
            )
        if args.ivol:
            parser.error(
                "--strategy dual_mom_vol10_cash has dedicated sleeve sizing and does not support --ivol."
            )
    return args


def _position_from_weight(weight: float) -> int:
    if weight > 0:
        return 1
    if weight < 0:
        return -1
    return 0


def _normalize_defensive_symbol(raw_defensive: str | None) -> str | None:
    if raw_defensive is None:
        return None
    defensive = raw_defensive.strip()
    return defensive if defensive else None


def has_interactive_display() -> bool:
    backend = plt.get_backend().lower()
    non_interactive_backends = ("agg", "pdf", "pgf", "ps", "svg", "template", "cairo")
    if any(name in backend for name in non_interactive_backends):
        return False
    if os.name == "nt":
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def maybe_plot_equity(
    equity: pd.Series,
    label: str,
    plot_out: str | None,
    no_plot: bool,
) -> None:
    if no_plot:
        return

    equity.plot(title=f"{label} Strategy Equity")
    plt.tight_layout()

    if plot_out:
        out_path = Path(plot_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path)
    elif has_interactive_display():
        plt.show()
    else:
        out_path = Path("outputs") / f"backtest_{label}.png"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path)

    plt.close()


def load_multi_asset_bars(
    store: LocalStore,
    symbols: list[str],
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    symbol_list = list(dict.fromkeys(symbols))
    frames: dict[str, pd.DataFrame] = {}

    for symbol in symbol_list:
        df = store.read_bars(symbol, start=start, end=end)
        if df.empty:
            raise ValueError(f"No bars available for symbol={symbol!r}.")
        frames[symbol] = df

    bars = pd.concat(frames, axis=1, join="inner").sort_index()
    if bars.empty:
        raise ValueError("No overlapping dates across requested symbols.")
    return bars


def _weights_turnover_total(weights: pd.Series | pd.DataFrame, turnover: pd.Series) -> float:
    if isinstance(weights, pd.Series):
        return float(metrics.turnover(weights))
    return float(turnover.sum())


def compute_extended_metrics(result: BacktestResult) -> dict[str, float]:
    cagr_v = float(metrics.cagr(result.returns))
    vol_v = float(metrics.vol(result.returns))
    sharpe_v = float(metrics.sharpe(result.returns))
    max_dd_v = float(metrics.max_drawdown(result.returns))
    calmar_v = cagr_v / abs(max_dd_v) if max_dd_v < 0 else 0.0

    if isinstance(result.weights, pd.Series):
        invested = result.weights.abs() > 0
    else:
        invested = result.weights.abs().sum(axis=1) > 0
    exposure_pct = float(invested.mean() * 100.0) if len(invested) else 0.0

    turnover_avg = float(result.turnover.mean()) if len(result.turnover) else 0.0
    years = len(result.returns) / 252.0
    if years > 0:
        trades_per_year = float((result.turnover > 0).sum() / years)
    else:
        trades_per_year = 0.0

    summary = {
        "cagr": cagr_v,
        "vol": vol_v,
        "sharpe": sharpe_v,
        "max_drawdown": max_dd_v,
        "calmar": calmar_v,
        "exposure_pct": exposure_pct,
        "turnover_avg_abs_change": turnover_avg,
        "trades_per_year": trades_per_year,
    }
    if result.leverage is not None:
        summary["avg_leverage"] = float(result.leverage.mean()) if len(result.leverage) else 0.0
        summary["max_leverage"] = float(result.leverage.max()) if len(result.leverage) else 0.0
    return summary


def compute_spy_benchmark(
    store: LocalStore,
    bars: pd.DataFrame,
    result_index: pd.DatetimeIndex,
    single_symbol: str | None = None,
) -> dict[str, float] | None:
    spy_close: pd.Series | None = None

    if isinstance(bars.columns, pd.MultiIndex):
        close_panel = bars.xs("close", axis=1, level=1)
        if "SPY" in close_panel.columns:
            spy_close = close_panel["SPY"]
    elif single_symbol == "SPY":
        spy_close = bars["close"]

    if spy_close is None:
        try:
            spy_df = store.read_bars(
                "SPY",
                start=result_index.min(),
                end=result_index.max(),
            )
        except FileNotFoundError:
            return None
        if spy_df.empty:
            return None
        spy_close = spy_df["close"]

    spy_returns = spy_close.astype(float).pct_change().fillna(0.0)
    spy_returns = spy_returns.reindex(result_index).fillna(0.0)
    return {
        "cagr": float(metrics.cagr(spy_returns)),
        "vol": float(metrics.vol(spy_returns)),
        "sharpe": float(metrics.sharpe(spy_returns)),
        "max_drawdown": float(metrics.max_drawdown(spy_returns)),
    }


def maybe_write_metrics_json(
    out_path: str | None,
    strategy_name: str,
    summary: dict[str, float],
    benchmark: dict[str, float] | None,
) -> None:
    if not out_path:
        return
    path = Path(out_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "strategy": strategy_name,
        "metrics": summary,
        "benchmark_spy": benchmark,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_tsmom_trade_log(
    symbol: str,
    close: pd.Series,
    weights: pd.Series,
) -> pd.DataFrame:
    columns = [
        "symbol",
        "entry_date",
        "exit_date",
        "direction",
        "entry_price",
        "exit_price",
        "pct_return",
        "holding_days",
    ]
    if close.empty:
        return pd.DataFrame(columns=columns)

    aligned_weights = weights.reindex(close.index).fillna(0.0)
    records: list[dict[str, object]] = []
    open_trade: dict[str, object] | None = None

    for dt, raw_weight in aligned_weights.items():
        pos = _position_from_weight(float(raw_weight))
        price = float(close.loc[dt])

        if open_trade is None:
            if pos != 0:
                open_trade = {
                    "entry_date": dt,
                    "entry_price": price,
                    "direction_sign": pos,
                }
            continue

        prev_pos = int(open_trade["direction_sign"])
        if pos == prev_pos:
            continue

        entry_date = pd.Timestamp(open_trade["entry_date"])
        entry_price = float(open_trade["entry_price"])
        direction = "long" if prev_pos > 0 else "short"
        if prev_pos > 0:
            pct_return = (price / entry_price) - 1.0
        else:
            pct_return = (entry_price / price) - 1.0

        records.append(
            {
                "symbol": symbol,
                "entry_date": entry_date.date().isoformat(),
                "exit_date": dt.date().isoformat(),
                "direction": direction,
                "entry_price": entry_price,
                "exit_price": price,
                "pct_return": pct_return,
                "holding_days": int((dt - entry_date).days),
            }
        )
        open_trade = None

        if pos != 0:
            open_trade = {
                "entry_date": dt,
                "entry_price": price,
                "direction_sign": pos,
            }

    if open_trade is not None:
        entry_date = pd.Timestamp(open_trade["entry_date"])
        direction_sign = int(open_trade["direction_sign"])
        direction = "long" if direction_sign > 0 else "short"
        last_dt = close.index[-1]
        records.append(
            {
                "symbol": symbol,
                "entry_date": entry_date.date().isoformat(),
                "exit_date": "",
                "direction": direction,
                "entry_price": float(open_trade["entry_price"]),
                "exit_price": "",
                "pct_return": "",
                "holding_days": int((last_dt - entry_date).days),
            }
        )

    return pd.DataFrame(records, columns=columns)


def _active_symbol_from_weights(weights: pd.DataFrame) -> pd.Series:
    abs_weights = weights.abs()
    max_abs_weight = abs_weights.max(axis=1)
    top_symbol = abs_weights.idxmax(axis=1)
    return top_symbol.where(max_abs_weight > 0.0, "CASH")


def _classify_symbol_action(from_symbol: str, to_symbol: str) -> str:
    if from_symbol == to_symbol:
        return "HOLD"
    if from_symbol == "CASH" and to_symbol != "CASH":
        return "ENTER"
    if from_symbol != "CASH" and to_symbol == "CASH":
        return "EXIT"
    return "ROTATE"


def _target_shares_for_active_symbol(
    weights: pd.DataFrame,
    close_panel: pd.DataFrame,
    capital: float = 10_000.0,
) -> pd.Series:
    active_symbol = _active_symbol_from_weights(weights)
    target_shares = pd.Series(0, index=weights.index, dtype=int)
    for dt in weights.index:
        symbol = str(active_symbol.loc[dt])
        if symbol == "CASH":
            continue
        price = float(close_panel.loc[dt, symbol])
        target_shares.loc[dt] = _target_shares_for_weight(
            float(weights.loc[dt, symbol]),
            price,
            capital=capital,
        )
    return target_shares


def _resize_mask_and_target_shares(
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    vol_target: float | None,
    vol_update: str,
    rebalance: str | int,
    capital: float = 10_000.0,
    allow_resize_without_vol_target: bool = False,
) -> tuple[pd.Series, pd.Series]:
    if weights.empty:
        empty_mask = pd.Series(False, index=weights.index, dtype=bool)
        empty_shares = pd.Series(0, index=weights.index, dtype=int)
        return empty_mask, empty_shares

    close_panel = bars.xs("close", axis=1, level=1)
    close_panel = close_panel.reindex(index=weights.index, columns=weights.columns)
    target_shares = _target_shares_for_active_symbol(weights, close_panel, capital=capital)

    if vol_target is None and not allow_resize_without_vol_target:
        return pd.Series(False, index=weights.index, dtype=bool), target_shares

    update_mask = _vol_update_mask_for_print(weights.index, vol_update, rebalance)
    active_symbol = _active_symbol_from_weights(weights)
    previous_symbol = active_symbol.shift(1).fillna("CASH")
    previous_target = target_shares.shift(1).fillna(0).astype(int)
    resize_mask = (
        update_mask
        & active_symbol.eq(previous_symbol)
        & target_shares.ne(previous_target)
    )
    return resize_mask, target_shares


def _latest_resize_details(
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    vol_target: float | None,
    vol_update: str,
    rebalance: str | int,
    up_to_date: pd.Timestamp | None = None,
    allow_resize_without_vol_target: bool = False,
) -> tuple[pd.Timestamp | None, int | None, int | None]:
    if weights.empty or (vol_target is None and not allow_resize_without_vol_target):
        return None, None, None

    resize_mask, target_shares = _resize_mask_and_target_shares(
        bars,
        weights,
        vol_target=vol_target,
        vol_update=vol_update,
        rebalance=rebalance,
        allow_resize_without_vol_target=allow_resize_without_vol_target,
    )
    if up_to_date is not None:
        resize_mask = resize_mask & (resize_mask.index <= up_to_date)

    resize_dates = resize_mask.index[resize_mask]
    if not len(resize_dates):
        return None, None, None

    resize_date = resize_dates[-1]
    previous_target = int(target_shares.shift(1).fillna(0).loc[resize_date])
    new_target = int(target_shares.loc[resize_date])
    return resize_date, previous_target, new_target


def build_dual_actions(
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    vol_target: float | None = None,
    vol_update: str = "rebalance",
    rebalance: str | int = "M",
    allow_resize_without_vol_target: bool = False,
) -> pd.DataFrame:
    columns = [
        "date",
        "action",
        "from_symbol",
        "to_symbol",
        "price_assumed(close)",
        "weight_from",
        "weight_to",
    ]
    if weights.empty:
        return pd.DataFrame(columns=columns)

    close_panel = bars.xs("close", axis=1, level=1)
    close_panel = close_panel.reindex(index=weights.index, columns=weights.columns)
    current_symbol = _active_symbol_from_weights(weights)
    previous_symbol = current_symbol.shift(1).fillna("CASH")
    previous_weights = weights.shift(1).fillna(0.0)
    resize_mask, _ = _resize_mask_and_target_shares(
        bars,
        weights,
        vol_target=vol_target,
        vol_update=vol_update,
        rebalance=rebalance,
        allow_resize_without_vol_target=allow_resize_without_vol_target,
    )

    records: list[dict[str, object]] = []
    for dt in current_symbol.index:
        from_symbol = str(previous_symbol.loc[dt])
        to_symbol = str(current_symbol.loc[dt])
        if from_symbol != to_symbol:
            action = _classify_symbol_action(from_symbol, to_symbol)
        elif bool(resize_mask.loc[dt]):
            action = "RESIZE"
            from_symbol = to_symbol
        else:
            continue
        price_symbol = to_symbol if to_symbol != "CASH" else from_symbol
        price = float(close_panel.loc[dt, price_symbol]) if price_symbol != "CASH" else ""
        weight_from = (
            float(previous_weights.loc[dt, from_symbol]) if from_symbol != "CASH" else 0.0
        )
        weight_to = float(weights.loc[dt, to_symbol]) if to_symbol != "CASH" else 0.0

        records.append(
            {
                "date": dt.date().isoformat(),
                "action": action,
                "from_symbol": from_symbol,
                "to_symbol": to_symbol,
                "price_assumed(close)": price,
                "weight_from": weight_from,
                "weight_to": weight_to,
            }
        )

    return pd.DataFrame(records, columns=columns)


def _tsmom_action_inputs(
    symbol: str,
    bars: pd.DataFrame,
    weights: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    action_bars = pd.concat({symbol: bars.copy()}, axis=1)
    action_weights = pd.DataFrame(
        {symbol: weights.reindex(bars.index).fillna(0.0).astype(float)},
        index=bars.index,
    )
    return action_bars, action_weights


def build_tsmom_actions(
    symbol: str,
    bars: pd.DataFrame,
    weights: pd.Series,
    vol_target: float | None = None,
    vol_update: str = "rebalance",
    rebalance: str | int = "M",
) -> pd.DataFrame:
    action_bars, action_weights = _tsmom_action_inputs(symbol, bars, weights)
    return build_dual_actions(
        action_bars,
        action_weights,
        vol_target=vol_target,
        vol_update=vol_update,
        rebalance=rebalance,
    )


def build_dual_tracker_actions(
    bars: pd.DataFrame,
    actions: pd.DataFrame,
    starting_equity: float = 10_000.0,
) -> pd.DataFrame:
    if actions.empty:
        return pd.DataFrame(columns=TRACKER_COLUMNS)

    close_panel = bars.xs("close", axis=1, level=1)
    cash = float(starting_equity)
    held_symbol = "CASH"
    held_shares = 0
    records: list[dict[str, object]] = []

    for _, row in actions.iterrows():
        date = pd.to_datetime(row["date"])
        action = str(row["action"])
        from_symbol = str(row["from_symbol"])
        to_symbol = str(row["to_symbol"])
        price = 0.0
        shares = 0
        notional = 0.0
        notes = ""

        if action == "ENTER":
            price = float(close_panel.loc[date, to_symbol])
            shares = int(cash // price)
            notional = float(shares * price)
            cash -= notional
            held_symbol = to_symbol
            held_shares = shares
        elif action == "EXIT":
            price = float(close_panel.loc[date, from_symbol])
            shares = int(held_shares)
            notional = float(shares * price)
            cash += notional
            held_symbol = "CASH"
            held_shares = 0
        elif action == "ROTATE":
            sell_price = float(close_panel.loc[date, from_symbol])
            cash += float(held_shares * sell_price)
            buy_price = float(close_panel.loc[date, to_symbol])
            shares = int(cash // buy_price)
            notional = float(shares * buy_price)
            cash -= notional
            price = buy_price
            held_symbol = to_symbol
            held_shares = shares
            notes = f"Sold {from_symbol} at {sell_price:.2f}"
        elif action == "RESIZE":
            price = float(close_panel.loc[date, to_symbol])
            row_loc = close_panel.index.get_loc(date)
            if isinstance(row_loc, slice):
                row_pos = int(row_loc.start or 0)
            else:
                row_pos = int(row_loc)
            prev_date = close_panel.index[row_pos - 1] if row_pos > 0 else date
            prev_price = float(close_panel.loc[prev_date, to_symbol])

            prev_weight = float(row.get("weight_from", 0.0))
            next_weight = float(row.get("weight_to", 0.0))
            prev_target = _target_shares_for_weight(
                prev_weight,
                prev_price,
                capital=starting_equity,
            )
            next_target = _target_shares_for_weight(
                next_weight,
                price,
                capital=starting_equity,
            )

            delta_shares = int(next_target - held_shares)
            shares = int(abs(delta_shares))
            notional = float(shares * price)
            cash -= float(delta_shares * price)
            held_symbol = to_symbol
            held_shares = int(next_target)
            notes = f"Target shares {prev_target}->{next_target}"
        else:
            continue

        if held_symbol == "CASH":
            equity_after = cash
        else:
            equity_after = cash + float(held_shares * close_panel.loc[date, held_symbol])

        records.append(
            {
                "date": date.date().isoformat(),
                "action": action,
                "from_symbol": from_symbol,
                "to_symbol": to_symbol,
                "price": round(price, 6) if price else "",
                "shares": shares,
                "notional": round(notional, 6),
                "cash_after": round(cash, 6),
                "equity_after": round(equity_after, 6),
                "notes": notes,
            }
        )

    return pd.DataFrame(records, columns=TRACKER_COLUMNS)


def maybe_write_tracker_template(path_str: str | None) -> None:
    if not path_str:
        return
    out_path = Path(path_str)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(columns=TRACKER_COLUMNS).to_csv(out_path, index=False)


def maybe_write_trades(
    strategy_name: str,
    trades_out: str | None,
    symbol: str,
    bars: pd.DataFrame,
    weights: pd.Series | pd.DataFrame,
    dual_actions: pd.DataFrame,
) -> None:
    if not trades_out:
        return
    out_path = Path(trades_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if strategy_name in {
        "dual_mom",
        "sma200",
        "risk_parity_erc",
        "tsmom_v1",
        "xsmom_v1",
        "dual_mom_v1",
        "valmom_v1",
    }:
        dual_actions.to_csv(out_path, index=False)
        return

    close = bars["close"].astype(float)
    trade_log = build_tsmom_trade_log(symbol, close, weights)  # type: ignore[arg-type]
    trade_log.to_csv(out_path, index=False)


def maybe_write_actions_csv(
    strategy_name: str,
    actions_out: str | None,
    bars: pd.DataFrame,
    dual_actions: pd.DataFrame,
    actions_bars: pd.DataFrame | None = None,
) -> None:
    if not actions_out:
        return
    out_path = Path(actions_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if dual_actions.empty:
        pd.DataFrame(columns=TRACKER_COLUMNS).to_csv(out_path, index=False)
        return

    tracker_source_bars = actions_bars if actions_bars is not None else bars
    tracker_actions = build_dual_tracker_actions(tracker_source_bars, dual_actions)
    tracker_actions.to_csv(out_path, index=False)


def _position_label(pos: int, allow_short: bool) -> str:
    if pos > 0:
        return "LONG"
    if pos < 0 and allow_short:
        return "SHORT"
    return "CASH"


def _target_shares_for_weight(weight: float, price: float, capital: float = 10_000.0) -> int:
    notional = capital * abs(weight)
    if price <= 0:
        return 0
    return int(notional // price)


def _latest_resize_details_tsmom(
    symbol: str,
    close: pd.Series,
    weights: pd.Series,
    vol_target: float | None,
    vol_update: str,
    rebalance: str,
) -> tuple[pd.Timestamp | None, int | None, int | None]:
    if vol_target is None or close.empty:
        return None, None, None

    aligned_weights = weights.reindex(close.index).fillna(0.0).astype(float)
    held_symbol = pd.Series(symbol, index=close.index, dtype=object).where(
        aligned_weights.abs() > 0.0,
        "CASH",
    )
    target_shares = pd.Series(0, index=close.index, dtype=int)
    for dt in close.index:
        if str(held_symbol.loc[dt]) == "CASH":
            continue
        target_shares.loc[dt] = _target_shares_for_weight(
            float(aligned_weights.loc[dt]),
            float(close.loc[dt]),
        )

    update_mask = _vol_update_mask_for_print(close.index, vol_update, rebalance)
    previous_symbol = held_symbol.shift(1).fillna("CASH")
    previous_target = target_shares.shift(1).fillna(0).astype(int)
    resize_mask = (
        update_mask
        & held_symbol.eq(previous_symbol)
        & target_shares.ne(previous_target)
    )
    resize_dates = resize_mask.index[resize_mask]
    if not len(resize_dates):
        return None, None, None

    resize_date = resize_dates[-1]
    prev_shares = int(previous_target.loc[resize_date])
    new_shares = int(target_shares.loc[resize_date])
    return resize_date, prev_shares, new_shares


def maybe_print_latest_tsmom(
    symbol: str,
    bars: pd.DataFrame,
    weights: pd.Series,
    allow_short: bool,
    print_latest: bool,
    vol_target: float | None = None,
    vol_update: str = "rebalance",
    rebalance: str = "M",
    latest_realized_vol: float | None = None,
    latest_leverage: float | None = None,
    leverage_last_update_date: str | None = None,
    realized_vol_at_last_update: float | None = None,
) -> None:
    if not print_latest:
        return

    aligned_weights = weights.reindex(bars.index).fillna(0.0)
    positions = aligned_weights.apply(_position_from_weight).astype(int)
    last_date = bars.index[-1]
    last_pos = int(positions.iloc[-1])
    latest_label = _position_label(last_pos, allow_short=allow_short)

    position_changes = positions != positions.shift(1).fillna(0).astype(int)
    changed_dates = positions.index[position_changes]
    recent_change_date = changed_dates[-1] if len(changed_dates) else None

    prev_pos = int(positions.iloc[-2]) if len(positions) >= 2 else 0
    if prev_pos == 0 and last_pos != 0:
        action = "ENTER"
    elif prev_pos != 0 and last_pos == 0:
        action = "EXIT"
    else:
        action = "HOLD"

    resize_prev_shares: int | None = None
    resize_new_shares: int | None = None
    if action == "HOLD":
        resize_date, prev_shares, new_shares = _latest_resize_details_tsmom(
            symbol,
            bars["close"].astype(float),
            aligned_weights,
            vol_target=vol_target,
            vol_update=vol_update,
            rebalance=rebalance,
        )
        if resize_date is not None and last_date >= resize_date:
            action = "RESIZE"
            resize_prev_shares = prev_shares
            resize_new_shares = new_shares

    print("Latest Date:", last_date.date().isoformat())
    print(f"Latest Position ({symbol}):", latest_label)
    if recent_change_date is not None:
        print("Most Recent Position Change:", recent_change_date.date().isoformat())
    else:
        print("Most Recent Position Change:", "N/A")
    if latest_leverage is not None:
        latest_realized_txt = (
            f"{latest_realized_vol:.4f}" if latest_realized_vol is not None and pd.notna(latest_realized_vol) else "N/A"
        )
        last_update_vol_txt = (
            f"{realized_vol_at_last_update:.4f}"
            if realized_vol_at_last_update is not None and pd.notna(realized_vol_at_last_update)
            else "N/A"
        )
        latest_price = float(bars["close"].iloc[-1]) if last_pos != 0 else 0.0
        target_shares = (
            _target_shares_for_weight(float(aligned_weights.iloc[-1]), latest_price)
            if last_pos != 0
            else 0
        )
        price_txt = f"{latest_price:.2f}" if last_pos != 0 else "N/A"
        print("Leverage last updated on:", leverage_last_update_date or "N/A")
        print("Realized vol at last update:", last_update_vol_txt)
        print("Realized Vol (ann, latest):", latest_realized_txt)
        print("Leverage (latest):", round(latest_leverage, 4))
        print(f"Target Shares ($10,000): {target_shares} @ {price_txt}")
    print("ACTION:", action)
    if action == "RESIZE" and resize_prev_shares is not None and resize_new_shares is not None:
        print("Previous Shares:", resize_prev_shares)
        print("New Shares:", resize_new_shares)


def _next_rebalance_date(last_date: pd.Timestamp, rebalance: str | int) -> pd.Timestamp:
    if isinstance(rebalance, int):
        next_date = compute_next_rebalance_date(
            pd.DatetimeIndex([]),
            last_date,
            trading_days=rebalance,
        )
    else:
        next_date = compute_next_rebalance_date(
            pd.DatetimeIndex([]),
            last_date,
            cadence=rebalance,
        )
    if next_date is None:
        raise ValueError(f"Unsupported rebalance cadence: {rebalance}")
    return pd.Timestamp(next_date)


def _next_rebalance_hint(last_date: pd.Timestamp, rebalance: str | int) -> str:
    next_rebalance = _next_rebalance_date(last_date, rebalance)
    if isinstance(rebalance, int):
        return f"next {rebalance}-trading-day rebalance ({next_rebalance.date().isoformat()})"
    if rebalance == "W":
        return f"next Friday ({next_rebalance.date().isoformat()})"
    return f"next business month-end ({next_rebalance.date().isoformat()})"


def _next_action_event_id(payload: dict[str, object]) -> str:
    def g(key: str) -> str:
        value = payload.get(key, "")
        return "" if value is None else str(value)

    parts = [
        g("date"),
        g("strategy"),
        g("action"),
        g("symbol"),
        g("target_shares"),
        g("resize_new_shares"),
        g("next_rebalance"),
    ]
    return ":".join(parts)


def _next_rebalance_value_for_payload(
    index: pd.DatetimeIndex,
    current_date: pd.Timestamp,
    next_rebalance: str | int | None,
    rebalance_anchor_date: str | None = None,
) -> str | None:
    if next_rebalance is None:
        return None
    if isinstance(next_rebalance, int):
        return compute_next_rebalance_date(
            index,
            current_date,
            trading_days=int(next_rebalance),
            anchor_date=rebalance_anchor_date,
        )
    return compute_next_rebalance_date(
        index,
        current_date,
        cadence=str(next_rebalance),
    )


def build_next_action_payload(
    strategy_label: str,
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    actions: pd.DataFrame,
    resize_rebalance: str,
    next_rebalance: str | int | None,
    rebalance_anchor_date: str | None = None,
    vol_target: float | None = None,
    vol_lookback: int | None = None,
    vol_update: str = "rebalance",
    latest_realized_vol: float | None = None,
    latest_leverage: float | None = None,
    leverage_last_update_date: str | None = None,
    allow_resize_without_vol_target: bool = False,
) -> dict[str, object]:
    realized_vol_value = (
        float(latest_realized_vol)
        if latest_realized_vol is not None and pd.notna(latest_realized_vol)
        else None
    )
    leverage_value = (
        float(latest_leverage)
        if latest_leverage is not None and pd.notna(latest_leverage)
        else None
    )
    leverage_update_value = leverage_last_update_date if vol_target is not None else None
    if weights.empty:
        today = pd.Timestamp.today().date().isoformat()
        next_rebalance_value = _next_rebalance_value_for_payload(
            pd.DatetimeIndex([]),
            pd.Timestamp(today),
            next_rebalance,
            rebalance_anchor_date=rebalance_anchor_date,
        )
        payload = {
            "schema_version": 1,
            "schema_minor": 0,
            "schema_name": "next_action",
            "date": today,
            "strategy": strategy_label,
            "action": "HOLD",
            "symbol": "CASH",
            "price": None,
            "target_shares": 0,
            "resize_prev_shares": None,
            "resize_new_shares": None,
            "next_rebalance": next_rebalance_value,
            "vol_target": float(vol_target) if vol_target is not None else None,
            "vol_lookback": int(vol_lookback) if vol_target is not None and vol_lookback is not None else None,
            "vol_update": vol_update if vol_target is not None else None,
            "realized_vol": realized_vol_value if vol_target is not None else None,
            "leverage": leverage_value if vol_target is not None else None,
            "leverage_update": leverage_update_value,
        }
        payload["event_id"] = _next_action_event_id(payload)
        return payload

    active_symbol = _active_symbol_from_weights(weights)
    last_date = weights.index[-1]
    current = str(active_symbol.iloc[-1])
    previous = str(active_symbol.iloc[-2]) if len(active_symbol) >= 2 else "CASH"
    action_last_bar = _classify_symbol_action(previous, current)

    last_action_type = "HOLD"
    last_action_date: pd.Timestamp | None = None
    if not actions.empty:
        last_action_type = str(actions.iloc[-1]["action"])
        last_action_date = pd.to_datetime(actions.iloc[-1]["date"])

    resize_prev_shares: int | None = None
    resize_new_shares: int | None = None
    if (
        action_last_bar == "HOLD"
        and last_action_type == "RESIZE"
        and last_action_date is not None
        and last_action_date.normalize() == last_date.normalize()
    ):
        action_last_bar = "RESIZE"
        _, resize_prev_shares, resize_new_shares = _latest_resize_details(
            bars,
            weights,
            vol_target=vol_target,
            vol_update=vol_update,
            rebalance=resize_rebalance,
            up_to_date=last_date,
            allow_resize_without_vol_target=allow_resize_without_vol_target,
        )

    close_panel = bars.xs("close", axis=1, level=1)
    if current == "CASH":
        target_shares = 0
        latest_price = None
    else:
        latest_weight = float(weights.loc[last_date, current])
        latest_price = float(close_panel.loc[last_date, current])
        target_shares = _target_shares_for_weight(latest_weight, latest_price)

    next_rebalance_value = _next_rebalance_value_for_payload(
        bars.index,
        last_date,
        next_rebalance,
        rebalance_anchor_date=rebalance_anchor_date,
    )

    payload = {
        "schema_version": 1,
        "schema_minor": 0,
        "schema_name": "next_action",
        "date": last_date.date().isoformat(),
        "strategy": strategy_label,
        "action": action_last_bar,
        "symbol": current,
        "price": latest_price,
        "target_shares": int(target_shares),
        "resize_prev_shares": int(resize_prev_shares)
        if action_last_bar == "RESIZE" and resize_prev_shares is not None
        else None,
        "resize_new_shares": int(resize_new_shares)
        if action_last_bar == "RESIZE" and resize_new_shares is not None
        else None,
        "next_rebalance": next_rebalance_value,
        "vol_target": float(vol_target) if vol_target is not None else None,
        "vol_lookback": int(vol_lookback) if vol_target is not None and vol_lookback is not None else None,
        "vol_update": vol_update if vol_target is not None else None,
        "realized_vol": realized_vol_value if vol_target is not None else None,
        "leverage": leverage_value if vol_target is not None else None,
        "leverage_update": leverage_update_value,
    }
    payload["event_id"] = _next_action_event_id(payload)
    return payload


def render_next_action_line(
    strategy_label: str,
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    actions: pd.DataFrame,
    resize_rebalance: str,
    next_rebalance: str | int | None,
    rebalance_anchor_date: str | None = None,
    vol_target: float | None = None,
    vol_lookback: int | None = None,
    vol_update: str = "rebalance",
    latest_realized_vol: float | None = None,
    latest_leverage: float | None = None,
    leverage_last_update_date: str | None = None,
    allow_resize_without_vol_target: bool = False,
) -> str:
    payload = build_next_action_payload(
        strategy_label=strategy_label,
        bars=bars,
        weights=weights,
        actions=actions,
        resize_rebalance=resize_rebalance,
        next_rebalance=next_rebalance,
        rebalance_anchor_date=rebalance_anchor_date,
        vol_target=vol_target,
        vol_lookback=vol_lookback,
        vol_update=vol_update,
        latest_realized_vol=latest_realized_vol,
        latest_leverage=latest_leverage,
        leverage_last_update_date=leverage_last_update_date,
        allow_resize_without_vol_target=allow_resize_without_vol_target,
    )

    if (
        payload["action"] == "RESIZE"
        and payload["resize_prev_shares"] is not None
        and payload["resize_new_shares"] is not None
        and str(payload["symbol"]) != "CASH"
    ):
        shares_txt = f"{payload['resize_prev_shares']}->{payload['resize_new_shares']}"
    else:
        shares_txt = f"sh={payload['target_shares']}"

    price_value = payload["price"]
    if price_value is not None:
        shares_txt = f"{shares_txt} px={float(price_value):.2f}"

    line_parts = [
        str(payload["date"]),
        str(payload["strategy"]),
        str(payload["action"]),
        str(payload["symbol"]),
        shares_txt,
    ]
    if payload["next_rebalance"] is not None:
        line_parts.append(f"next={payload['next_rebalance']}")
    if payload["vol_target"] is not None:
        lev_value = payload["leverage"]
        lev_txt = f"{float(lev_value):.3f}" if lev_value is not None else "N/A"
        line_parts.append(f"lev={lev_txt} upd={payload['leverage_update'] or 'N/A'}")

    return " | ".join(line_parts)


def _vol_update_mask_for_print(
    index: pd.DatetimeIndex,
    vol_update: str,
    rebalance: str | int,
) -> pd.Series:
    if vol_update == "daily":
        return pd.Series(True, index=index, dtype=bool)

    if isinstance(rebalance, int):
        if rebalance <= 0:
            raise ValueError("rebalance must be > 0 when provided as trading days.")
        update_mask = pd.Series(False, index=index, dtype=bool)
        for idx_pos in range(int(rebalance) - 1, len(index) - 1, int(rebalance)):
            update_mask.iloc[idx_pos + 1] = True
        return update_mask

    cadence = rebalance.upper()
    if cadence == "M":
        periods = pd.Series(index.to_period("M"), index=index)
        rebalance_day = periods.ne(periods.shift(-1)).fillna(False)
    elif cadence == "W":
        rebalance_day = pd.Series(index.weekday == 4, index=index, dtype=bool)
    else:
        raise ValueError("rebalance must be one of {'M', 'W'}.")

    update_mask = pd.Series(False, index=index, dtype=bool)
    for idx_pos in range(len(index) - 1):
        if bool(rebalance_day.iloc[idx_pos]):
            update_mask.iloc[idx_pos + 1] = True
    return update_mask


def maybe_write_dual_checklist(
    out_path: str | None,
    rebalance: str | int,
    risk_symbols: list[str],
    defensive: str | None,
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    vol_target: float | None = None,
    vol_update: str = "rebalance",
    allow_resize_without_vol_target: bool = False,
) -> None:
    if not out_path:
        return

    actions = build_dual_actions(
        bars,
        weights,
        vol_target=vol_target,
        vol_update=vol_update,
        rebalance=rebalance,
        allow_resize_without_vol_target=allow_resize_without_vol_target,
    )
    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])
    previous = str(active_symbol.iloc[-2]) if len(active_symbol) >= 2 else "CASH"
    action = _classify_symbol_action(previous, current)
    if actions.empty:
        last_action_date = "N/A"
        last_action = "HOLD"
    else:
        last_action_date = str(actions.iloc[-1]["date"])
        last_action = str(actions.iloc[-1]["action"])

    if action == "HOLD" and last_action == "RESIZE":
        action = "RESIZE"

    close_panel = bars.xs("close", axis=1, level=1)
    if current == "CASH":
        target_shares = 0
        price_txt = "N/A"
    else:
        latest_price = float(close_panel.loc[last_date, current])
        target_weight = float(weights.loc[last_date, current])
        target_shares = _target_shares_for_weight(target_weight, latest_price)
        price_txt = f"{latest_price:.2f}"

    if isinstance(rebalance, int):
        check_day = f"every {rebalance} trading days after close"
    elif rebalance == "W":
        check_day = "last trading day of each week after close"
    else:
        check_day = "last trading day of each month after close"
    defensive_txt = defensive if defensive else "CASH"
    next_rebalance = _next_rebalance_hint(last_date, rebalance)
    alert_symbols = ", ".join(risk_symbols + ([defensive] if defensive else []))

    content = "\n".join(
        [
            "# Dual Momentum Checklist",
            "",
            f"- Check day: {check_day}.",
            f"- Current holding: {current}.",
            f"- Last action: {last_action} on {last_action_date}.",
            f"- Next rebalance window: {next_rebalance}.",
            f"- Universe: risk={', '.join(risk_symbols)} | defensive={defensive_txt}.",
            f"- ACTION NOW: {action}.",
            f"- Target shares for $10,000: {target_shares} (price: {price_txt}).",
            f"- Alerts to place: {alert_symbols} close-check alert before {next_rebalance}.",
        ]
    )

    checklist_path = Path(out_path)
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text(content + "\n", encoding="utf-8")


def maybe_print_latest_dual(
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    rebalance: str | int,
    print_latest: bool,
    vol_target: float | None = None,
    vol_update: str = "rebalance",
    allow_resize_without_vol_target: bool = False,
    regime_gate: str = "none",
    gate_symbol: str = "SPY",
    gate_sma_window: int = 200,
    defensive_symbol: str | None = None,
    latest_realized_vol: float | None = None,
    latest_leverage: float | None = None,
    leverage_last_update_date: str | None = None,
    realized_vol_at_last_update: float | None = None,
) -> None:
    if not print_latest:
        return

    actions = build_dual_actions(
        bars,
        weights,
        vol_target=vol_target,
        vol_update=vol_update,
        rebalance=rebalance,
        allow_resize_without_vol_target=allow_resize_without_vol_target,
    )
    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])
    previous = str(active_symbol.iloc[-2]) if len(active_symbol) >= 2 else "CASH"
    action_last_bar = _classify_symbol_action(previous, current)

    if actions.empty:
        last_action_date = None
        last_action_type = "HOLD"
    else:
        last_action_date = pd.to_datetime(actions.iloc[-1]["date"])
        last_action_type = str(actions.iloc[-1]["action"])

    resize_prev_shares: int | None = None
    resize_new_shares: int | None = None
    if action_last_bar == "HOLD" and last_action_type == "RESIZE":
        action_last_bar = "RESIZE"
        _, resize_prev_shares, resize_new_shares = _latest_resize_details(
            bars,
            weights,
            vol_target=vol_target,
            vol_update=vol_update,
            rebalance=rebalance,
            up_to_date=last_date,
            allow_resize_without_vol_target=allow_resize_without_vol_target,
        )

    close_panel = bars.xs("close", axis=1, level=1)
    if current == "CASH":
        target_shares = 0
        price_txt = "N/A"
    else:
        target_weight = float(weights.loc[last_date, current])
        latest_price = float(close_panel.loc[last_date, current])
        target_shares = _target_shares_for_weight(target_weight, latest_price)
        price_txt = f"{latest_price:.2f}"

    print("Latest Date:", last_date.date().isoformat())
    print("Currently Held:", current)
    if last_action_date is None:
        print("Last Action Date:", "N/A")
    else:
        print("Last Action Date:", last_action_date.date().isoformat())
    print("Last Action Type:", last_action_type)
    print("Next Rebalance:", _next_rebalance_hint(last_date, rebalance))
    if regime_gate == "sma200":
        gate_close = close_panel[gate_symbol].astype(float)
        gate_risk_on = _sma200_risk_on_series(gate_close, gate_sma_window)
        gate_on_latest = bool(gate_risk_on.iloc[-1])
        print("Gate State:", "RISK-ON" if gate_on_latest else "RISK-OFF")
        if not gate_on_latest:
            forced_symbol = defensive_symbol if defensive_symbol else "CASH"
            print("GATE OVERRIDE:", "TRUE")
            print("Gate Forced Holding:", forced_symbol)
    if latest_leverage is not None:
        latest_realized_txt = (
            f"{latest_realized_vol:.4f}" if latest_realized_vol is not None and pd.notna(latest_realized_vol) else "N/A"
        )
        last_update_vol_txt = (
            f"{realized_vol_at_last_update:.4f}"
            if realized_vol_at_last_update is not None and pd.notna(realized_vol_at_last_update)
            else "N/A"
        )
        print("Leverage last updated on:", leverage_last_update_date or "N/A")
        print("Realized vol at last update:", last_update_vol_txt)
        print("Realized Vol (ann, latest):", latest_realized_txt)
        print("Leverage (latest):", round(latest_leverage, 4))
    print(f"Target Shares ($10,000): {target_shares} @ {price_txt}")
    print("ACTION:", action_last_bar)
    if (
        action_last_bar == "RESIZE"
        and resize_prev_shares is not None
        and resize_new_shares is not None
    ):
        print("Previous Shares:", resize_prev_shares)
        print("New Shares:", resize_new_shares)


def _sma200_risk_on_series(close: pd.Series, sma_window: int) -> pd.Series:
    sma = close.rolling(sma_window).mean()
    return (close.shift(1) > sma.shift(1)).fillna(False)


def maybe_write_sma200_checklist(
    out_path: str | None,
    rebalance: str,
    risk_symbol: str,
    defensive: str | None,
    sma_window: int,
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    vol_target: float | None = None,
    vol_update: str = "rebalance",
) -> None:
    if not out_path:
        return

    actions = build_dual_actions(
        bars,
        weights,
        vol_target=vol_target,
        vol_update=vol_update,
        rebalance=rebalance,
    )
    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])

    previous = str(active_symbol.iloc[-2]) if len(active_symbol) >= 2 else "CASH"
    action_now = _classify_symbol_action(previous, current)
    if actions.empty:
        last_action_date = "N/A"
        last_action_type = "HOLD"
    else:
        last_action_date = str(actions.iloc[-1]["date"])
        last_action_type = str(actions.iloc[-1]["action"])
    if action_now == "HOLD" and last_action_type == "RESIZE":
        action_now = "RESIZE"

    close_panel = bars.xs("close", axis=1, level=1)
    risk_close = close_panel[risk_symbol].astype(float)
    risk_on = bool(_sma200_risk_on_series(risk_close, sma_window).iloc[-1])
    risk_state = "RISK-ON" if risk_on else "RISK-OFF"

    if current == "CASH":
        target_shares = 0
        price_txt = "N/A"
    else:
        latest_price = float(close_panel.loc[last_date, current])
        target_shares = int(10_000 // latest_price)
        price_txt = f"{latest_price:.2f}"

    check_day = (
        "last trading day of each week after close"
        if rebalance == "W"
        else "month-end after close (last trading day of each month)"
    )
    next_rebalance = _next_rebalance_hint(last_date, rebalance)
    defensive_txt = defensive if defensive else "CASH"

    content = "\n".join(
        [
            "# SMA200 Regime Checklist",
            "",
            f"- Check day: {check_day}.",
            f"- Risk-On State: {risk_state}.",
            f"- Current holding: {current}.",
            f"- Last action: {last_action_type} on {last_action_date}.",
            f"- Next rebalance window: {next_rebalance}.",
            f"- Universe: risk={risk_symbol} | defensive={defensive_txt} | sma_window={sma_window}.",
            f"- ACTION NOW: {action_now}.",
            f"- Target shares for $10,000: {target_shares} (price: {price_txt}).",
        ]
    )

    checklist_path = Path(out_path)
    checklist_path.parent.mkdir(parents=True, exist_ok=True)
    checklist_path.write_text(content + "\n", encoding="utf-8")


def maybe_print_latest_sma200(
    bars: pd.DataFrame,
    weights: pd.DataFrame,
    risk_symbol: str,
    sma_window: int,
    rebalance: str,
    print_latest: bool,
    vol_target: float | None = None,
    vol_update: str = "rebalance",
    latest_realized_vol: float | None = None,
    latest_leverage: float | None = None,
    leverage_last_update_date: str | None = None,
    realized_vol_at_last_update: float | None = None,
) -> None:
    if not print_latest:
        return

    actions = build_dual_actions(
        bars,
        weights,
        vol_target=vol_target,
        vol_update=vol_update,
        rebalance=rebalance,
    )
    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])
    previous = str(active_symbol.iloc[-2]) if len(active_symbol) >= 2 else "CASH"
    action_last_bar = _classify_symbol_action(previous, current)

    if actions.empty:
        last_action_date = None
        last_action_type = "HOLD"
    else:
        last_action_date = pd.to_datetime(actions.iloc[-1]["date"])
        last_action_type = str(actions.iloc[-1]["action"])

    resize_prev_shares: int | None = None
    resize_new_shares: int | None = None
    if action_last_bar == "HOLD" and last_action_type == "RESIZE":
        action_last_bar = "RESIZE"
        _, resize_prev_shares, resize_new_shares = _latest_resize_details(
            bars,
            weights,
            vol_target=vol_target,
            vol_update=vol_update,
            rebalance=rebalance,
            up_to_date=last_date,
        )

    close_panel = bars.xs("close", axis=1, level=1)
    risk_close = close_panel[risk_symbol].astype(float)
    risk_on = bool(_sma200_risk_on_series(risk_close, sma_window).iloc[-1])
    risk_state = "RISK-ON" if risk_on else "RISK-OFF"

    if current == "CASH":
        target_shares = 0
        price_txt = "N/A"
    else:
        target_weight = float(weights.loc[last_date, current])
        latest_price = float(close_panel.loc[last_date, current])
        target_shares = _target_shares_for_weight(target_weight, latest_price)
        price_txt = f"{latest_price:.2f}"

    print("Latest Date:", last_date.date().isoformat())
    print("Risk-On State:", risk_state)
    print("Currently Held:", current)
    if last_action_date is None:
        print("Last Action Date:", "N/A")
    else:
        print("Last Action Date:", last_action_date.date().isoformat())
    print("Last Action Type:", last_action_type)
    print("Next Rebalance:", _next_rebalance_hint(last_date, rebalance))
    if latest_leverage is not None:
        latest_realized_txt = (
            f"{latest_realized_vol:.4f}" if latest_realized_vol is not None and pd.notna(latest_realized_vol) else "N/A"
        )
        last_update_vol_txt = (
            f"{realized_vol_at_last_update:.4f}"
            if realized_vol_at_last_update is not None and pd.notna(realized_vol_at_last_update)
            else "N/A"
        )
        print("Leverage last updated on:", leverage_last_update_date or "N/A")
        print("Realized vol at last update:", last_update_vol_txt)
        print("Realized Vol (ann, latest):", latest_realized_txt)
        print("Leverage (latest):", round(latest_leverage, 4))
    print(f"Target Shares ($10,000): {target_shares} @ {price_txt}")
    print("ACTION:", action_last_bar)
    if (
        action_last_bar == "RESIZE"
        and resize_prev_shares is not None
        and resize_new_shares is not None
    ):
        print("Previous Shares:", resize_prev_shares)
        print("New Shares:", resize_new_shares)


def main() -> None:
    args = parse_args()
    cfg = load_run_backtest_config(args.config)
    if args.rebalance_anchor_date is None and cfg.rebalance_anchor_date is not None:
        args.rebalance_anchor_date = cfg.rebalance_anchor_date

    store = LocalStore(base_dir=args.data_dir)
    rebalance_cadence = args.rebalance
    allow_resize_without_vol_target = False

    if args.strategy == "tsmom":
        bars = store.read_bars(args.symbol, start=args.start, end=args.end)
        if bars.empty:
            raise ValueError(
                f"No bars available for symbol={args.symbol!r} in the requested date range."
            )
        strategy = TrendTSMOM(lookback=args.lookback, allow_short=not args.long_only)
        plot_label = args.symbol
    elif args.strategy == "dual_mom":
        defensive_symbol = _normalize_defensive_symbol(args.defensive)
        gate_symbol = args.gate_symbol.strip()
        gate_symbols_to_load = [gate_symbol] if args.regime_gate == "sma200" and gate_symbol else []
        symbols_to_load = args.symbols + ([defensive_symbol] if defensive_symbol else []) + gate_symbols_to_load
        symbols_to_load = list(dict.fromkeys(symbols_to_load))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = DualMomentumStrategy(
            risk_universe=args.symbols,
            defensive=defensive_symbol,
            lookback=args.mom_lookback,
            rebalance=args.rebalance,
            regime_gate=args.regime_gate,
            gate_symbol=gate_symbol,
            gate_sma_window=args.gate_sma_window,
        )
        plot_label = "dual_momentum"
    elif args.strategy == "sma200":
        defensive_symbol = _normalize_defensive_symbol(args.defensive)
        symbols_to_load = [args.risk_symbol] + ([defensive_symbol] if defensive_symbol else [])
        symbols_to_load = list(dict.fromkeys(symbols_to_load))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = Sma200RegimeStrategy(
            risk_symbol=args.risk_symbol,
            defensive=defensive_symbol,
            sma_window=args.sma_window,
            rebalance=args.rebalance,
        )
        plot_label = "sma200_regime"
    elif args.strategy == "risk_parity_erc":
        symbols_to_load = list(dict.fromkeys(args.symbols))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = RiskParityERCStrategy(
            symbols=symbols_to_load,
            lookback=args.rp_lookback,
            rebalance=args.rp_rebalance,
            max_iter=args.rp_max_iter,
            tol=args.rp_tol,
        )
        plot_label = "risk_parity_erc"
        rebalance_cadence = args.rp_rebalance
    elif args.strategy == "tsmom_v1":
        defensive_symbol = _normalize_defensive_symbol(args.defensive)
        symbols_to_load = args.symbols + ([defensive_symbol] if defensive_symbol else [])
        symbols_to_load = list(dict.fromkeys(symbols_to_load))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = TimeSeriesMomentumV1Strategy(
            symbols=args.symbols,
            lookback=args.ts_lookback,
            rebalance=args.ts_rebalance,
            defensive=defensive_symbol,
        )
        plot_label = "tsmom_v1"
        rebalance_cadence = args.ts_rebalance
    elif args.strategy == "xsmom_v1":
        defensive_symbol = _normalize_defensive_symbol(args.defensive)
        symbols_to_load = args.symbols + ([defensive_symbol] if defensive_symbol else [])
        symbols_to_load = list(dict.fromkeys(symbols_to_load))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = CrossSectionalMomentumV1Strategy(
            symbols=args.symbols,
            lookback=args.xs_lookback,
            top_n=args.xs_top_n,
            rebalance=args.xs_rebalance,
            defensive=defensive_symbol,
        )
        plot_label = "xsmom_v1"
        rebalance_cadence = args.xs_rebalance
    elif args.strategy == "dual_mom_v1":
        risk_symbols = list(dict.fromkeys(args.symbols))
        defensive_symbol = args.dm_defensive_symbol.strip()
        if not defensive_symbol:
            raise ValueError("--dm-defensive-symbol must not be empty.")
        symbols_to_load = list(dict.fromkeys(risk_symbols + [defensive_symbol]))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = DualMomentumV1Strategy(
            symbols=risk_symbols,
            lookback=args.dm_lookback,
            top_n=args.dm_top_n,
            rebalance=args.dm_rebalance,
            defensive_symbol=defensive_symbol,
        )
        plot_label = "dual_mom_v1"
        rebalance_cadence = args.dm_rebalance
    elif args.strategy == "dual_mom_vol10_cash":
        risk_symbols = list(dict.fromkeys(args.symbols))
        defensive_symbol = args.dmv_defensive_symbol.strip()
        if not defensive_symbol:
            raise ValueError("--dmv-defensive-symbol must not be empty.")
        symbols_to_load = list(dict.fromkeys(risk_symbols + [defensive_symbol]))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = DualMomentumVol10CashStrategy(
            symbols=risk_symbols,
            defensive_symbol=defensive_symbol,
            momentum_lookback=args.dmv_mom_lookback,
            rebalance=args.dmv_rebalance,
            vol_lookback=args.dmv_vol_lookback,
            target_vol=args.dmv_target_vol,
        )
        plot_label = "dual_mom_vol10_cash"
        rebalance_cadence = args.dmv_rebalance
        allow_resize_without_vol_target = True
    elif args.strategy == "valmom_v1":
        risk_symbols = list(dict.fromkeys(args.symbols))
        defensive_symbol = args.vm_defensive_symbol.strip()
        if not defensive_symbol:
            raise ValueError("--vm-defensive-symbol must not be empty.")
        symbols_to_load = list(dict.fromkeys(risk_symbols + [defensive_symbol]))
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = ValueMomentumV1Strategy(
            symbols=risk_symbols,
            mom_lookback=args.vm_mom_lookback,
            val_lookback=args.vm_val_lookback,
            top_n=args.vm_top_n,
            rebalance=args.vm_rebalance,
            defensive_symbol=defensive_symbol,
            mom_weight=args.vm_mom_weight,
            val_weight=args.vm_val_weight,
        )
        plot_label = "valmom_v1"
        rebalance_cadence = args.vm_rebalance
    else:
        raise ValueError(f"Unsupported strategy: {args.strategy}")

    if args.ivol and (
        not isinstance(bars.columns, pd.MultiIndex) or bars.columns.nlevels != 2
    ):
        raise ValueError("--ivol is only supported for multi-asset strategies.")

    result = run_backtest(
        bars,
        strategy,
        slippage_bps=args.slippage_bps,
        commission_bps=args.commission_bps,
        vol_target=args.vol_target,
        vol_lookback=args.vol_lookback,
        vol_min=args.min_leverage,
        vol_max=args.max_leverage,
        vol_update=args.vol_update,
        rebalance_cadence=rebalance_cadence,
        ivol=args.ivol,
        ivol_lookback=args.ivol_lookback,
        ivol_eps=args.ivol_eps,
    )

    latest_realized_vol = None
    latest_leverage = None
    leverage_last_update_date = None
    realized_vol_at_last_update = None
    if result.leverage is not None and result.realized_vol is not None and len(result.returns):
        latest_dt = result.returns.index[-1]
        latest_realized_vol = float(result.realized_vol.loc[latest_dt])
        latest_leverage = float(result.leverage.loc[latest_dt])
        update_mask = _vol_update_mask_for_print(result.returns.index, args.vol_update, rebalance_cadence)
        update_dates = update_mask.index[update_mask]
        if len(update_dates):
            last_update_dt = update_dates[-1]
            leverage_last_update_date = last_update_dt.date().isoformat()
            realized_vol_at_last_update = float(result.realized_vol.loc[last_update_dt])

    dual_actions = pd.DataFrame()
    actions_bars: pd.DataFrame | None = None
    actions_weights: pd.DataFrame | None = None
    if args.strategy in {
        "dual_mom",
        "sma200",
        "risk_parity_erc",
        "tsmom_v1",
        "xsmom_v1",
        "dual_mom_v1",
        "dual_mom_vol10_cash",
        "valmom_v1",
    }:
        dual_actions = build_dual_actions(  # type: ignore[arg-type]
            bars,
            result.weights,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            rebalance=rebalance_cadence,
            allow_resize_without_vol_target=allow_resize_without_vol_target,
        )
        actions_bars = bars
        actions_weights = result.weights  # type: ignore[assignment]
    else:
        tsmom_action_bars, tsmom_action_weights = _tsmom_action_inputs(
            args.symbol,
            bars,
            result.weights,  # type: ignore[arg-type]
        )
        dual_actions = build_dual_actions(
            tsmom_action_bars,
            tsmom_action_weights,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            rebalance=rebalance_cadence,
            allow_resize_without_vol_target=allow_resize_without_vol_target,
        )
        actions_bars = tsmom_action_bars
        actions_weights = tsmom_action_weights

    if (args.next_action_json or args.next_action) and actions_bars is not None and actions_weights is not None:
        strategy_label = (
            "BASELINE" if args.strategy == "tsmom" and args.long_only else args.strategy
        )
        next_rebalance: str | int | None
        if args.strategy in {"dual_mom", "sma200"}:
            next_rebalance = args.rebalance
        elif args.strategy == "risk_parity_erc":
            next_rebalance = args.rp_rebalance
        elif args.strategy == "tsmom_v1":
            next_rebalance = args.ts_rebalance
        elif args.strategy == "xsmom_v1":
            next_rebalance = args.xs_rebalance
        elif args.strategy == "dual_mom_v1":
            next_rebalance = args.dm_rebalance
        elif args.strategy == "dual_mom_vol10_cash":
            next_rebalance = args.dmv_rebalance
        elif args.strategy == "valmom_v1":
            next_rebalance = args.vm_rebalance
        else:
            next_rebalance = None

        payload = build_next_action_payload(
            strategy_label=strategy_label,
            bars=actions_bars,
            weights=actions_weights,
            actions=dual_actions,
            resize_rebalance=rebalance_cadence,
            next_rebalance=next_rebalance,
            rebalance_anchor_date=args.rebalance_anchor_date,
            vol_target=args.vol_target,
            vol_lookback=args.vol_lookback,
            vol_update=args.vol_update,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            allow_resize_without_vol_target=allow_resize_without_vol_target,
        )
        if args.next_action_json:
            print(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
        else:
            print(
                render_next_action_line(
                    strategy_label=strategy_label,
                    bars=actions_bars,
                    weights=actions_weights,
                    actions=dual_actions,
                    resize_rebalance=rebalance_cadence,
                    next_rebalance=next_rebalance,
                    rebalance_anchor_date=args.rebalance_anchor_date,
                    vol_target=args.vol_target,
                    vol_lookback=args.vol_lookback,
                    vol_update=args.vol_update,
                    latest_realized_vol=latest_realized_vol,
                    latest_leverage=latest_leverage,
                    leverage_last_update_date=leverage_last_update_date,
                    allow_resize_without_vol_target=allow_resize_without_vol_target,
                )
            )
        return

    print("CAGR:", round(metrics.cagr(result.returns), 4))
    print("Vol:", round(metrics.vol(result.returns), 4))
    print("Sharpe:", round(metrics.sharpe(result.returns), 4))
    print("Max DD:", round(metrics.max_drawdown(result.returns), 4))
    print("Turnover:", round(_weights_turnover_total(result.weights, result.turnover), 4))

    extended = compute_extended_metrics(result)
    print("Calmar:", round(extended["calmar"], 4))
    print("Exposure %:", round(extended["exposure_pct"], 2))
    print("Turnover Avg:", round(extended["turnover_avg_abs_change"], 4))
    print("Trades/Year:", round(extended["trades_per_year"], 2))
    if "avg_leverage" in extended:
        print("Avg Leverage:", round(extended["avg_leverage"], 4))
        print("Max Leverage:", round(extended["max_leverage"], 4))

    benchmark = compute_spy_benchmark(
        store,
        bars,
        result.returns.index,
        single_symbol=args.symbol if args.strategy == "tsmom" else None,
    )
    if benchmark:
        print(
            "Benchmark SPY:",
            "CAGR",
            round(benchmark["cagr"], 4),
            "Vol",
            round(benchmark["vol"], 4),
            "Sharpe",
            round(benchmark["sharpe"], 4),
            "MaxDD",
            round(benchmark["max_drawdown"], 4),
        )
    else:
        print("Benchmark SPY: unavailable")

    maybe_write_metrics_json(args.metrics_out, args.strategy, extended, benchmark)

    print_latest_enabled = args.print_latest and (not args.next_action) and (not args.next_action_json)

    if args.strategy == "dual_mom":
        checklist_path = args.checklist_out or "outputs/dual_momentum_checklist.md"
        defensive_symbol = _normalize_defensive_symbol(args.defensive)
        maybe_write_dual_checklist(
            checklist_path,
            args.rebalance,
            args.symbols,
            defensive_symbol,
            bars,
            result.weights,  # type: ignore[arg-type]
            vol_target=args.vol_target,
            vol_update=args.vol_update,
        )
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            args.rebalance,
            print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            regime_gate=args.regime_gate,
            gate_symbol=args.gate_symbol.strip(),
            gate_sma_window=args.gate_sma_window,
            defensive_symbol=defensive_symbol,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )
    elif args.strategy == "sma200":
        checklist_path = args.checklist_out or "outputs/sma200_regime_checklist.md"
        defensive_symbol = _normalize_defensive_symbol(args.defensive)
        maybe_write_sma200_checklist(
            checklist_path,
            args.rebalance,
            args.risk_symbol,
            defensive_symbol,
            args.sma_window,
            bars,
            result.weights,  # type: ignore[arg-type]
            vol_target=args.vol_target,
            vol_update=args.vol_update,
        )
        maybe_print_latest_sma200(
            bars,
            result.weights,  # type: ignore[arg-type]
            args.risk_symbol,
            args.sma_window,
            args.rebalance,
            print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )
    elif args.strategy == "risk_parity_erc":
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            rebalance_cadence,
            print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )
    elif args.strategy == "tsmom_v1":
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            rebalance_cadence,
            print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )
    elif args.strategy == "xsmom_v1":
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            rebalance_cadence,
            print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )
    elif args.strategy == "dual_mom_v1":
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            rebalance_cadence,
            print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )
    elif args.strategy == "dual_mom_vol10_cash":
        checklist_path = args.checklist_out or "outputs/dual_mom_vol10_cash_checklist.md"
        defensive_symbol = args.dmv_defensive_symbol.strip()
        maybe_write_dual_checklist(
            checklist_path,
            rebalance_cadence,
            args.symbols,
            defensive_symbol,
            bars,
            result.weights,  # type: ignore[arg-type]
            allow_resize_without_vol_target=True,
        )
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            rebalance_cadence,
            print_latest_enabled,
            allow_resize_without_vol_target=True,
            defensive_symbol=defensive_symbol,
        )
    elif args.strategy == "valmom_v1":
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            rebalance_cadence,
            print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )
    else:
        maybe_print_latest_tsmom(
            args.symbol,
            bars,
            result.weights,  # type: ignore[arg-type]
            allow_short=not args.long_only,
            print_latest=print_latest_enabled,
            vol_target=args.vol_target,
            vol_update=args.vol_update,
            rebalance=args.rebalance,
            latest_realized_vol=latest_realized_vol,
            latest_leverage=latest_leverage,
            leverage_last_update_date=leverage_last_update_date,
            realized_vol_at_last_update=realized_vol_at_last_update,
        )

    maybe_write_trades(
        args.strategy,
        args.trades_out,
        args.symbol,
        bars,
        result.weights,
        dual_actions,
    )
    maybe_write_actions_csv(
        args.strategy,
        args.actions_out,
        bars,
        dual_actions,
        actions_bars=actions_bars,
    )
    maybe_write_tracker_template(args.tracker_template_out)
    maybe_plot_equity(result.equity, plot_label, args.plot_out, args.no_plot)


if __name__ == "__main__":
    main()
