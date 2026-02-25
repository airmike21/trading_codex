"""Run backtests on locally cached daily bars."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from trading_codex.backtest import metrics
from trading_codex.backtest.engine import BacktestResult, run_backtest
from trading_codex.data import LocalStore
from trading_codex.strategies.dual_momentum import DualMomentumStrategy
from trading_codex.strategies.sma200 import Sma200RegimeStrategy
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run strategy backtests on cached daily bars.")
    parser.add_argument("--strategy", choices=["tsmom", "dual_mom", "sma200"], default="tsmom")
    parser.add_argument("--symbol", default="SPY", help="Ticker symbol for single-asset strategy.")
    parser.add_argument("--start", default=None, help="Inclusive start date (YYYY-MM-DD).")
    parser.add_argument("--end", default=None, help="Inclusive end date (YYYY-MM-DD).")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directory containing cached parquet bars (default: data).",
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
    return parser.parse_args()


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

    return {
        "cagr": cagr_v,
        "vol": vol_v,
        "sharpe": sharpe_v,
        "max_drawdown": max_dd_v,
        "calmar": calmar_v,
        "exposure_pct": exposure_pct,
        "turnover_avg_abs_change": turnover_avg,
        "trades_per_year": trades_per_year,
    }


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
    max_weight = weights.max(axis=1)
    top_symbol = weights.idxmax(axis=1)
    return top_symbol.where(max_weight > 0.0, "CASH")


def _classify_symbol_action(from_symbol: str, to_symbol: str) -> str:
    if from_symbol == to_symbol:
        return "HOLD"
    if from_symbol == "CASH" and to_symbol != "CASH":
        return "ENTER"
    if from_symbol != "CASH" and to_symbol == "CASH":
        return "EXIT"
    return "ROTATE"


def build_dual_actions(bars: pd.DataFrame, weights: pd.DataFrame) -> pd.DataFrame:
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
    current_symbol = _active_symbol_from_weights(weights)
    previous_symbol = current_symbol.shift(1).fillna("CASH")
    previous_weights = weights.shift(1).fillna(0.0)

    records: list[dict[str, object]] = []
    for dt in current_symbol.index[current_symbol != previous_symbol]:
        from_symbol = str(previous_symbol.loc[dt])
        to_symbol = str(current_symbol.loc[dt])
        action = _classify_symbol_action(from_symbol, to_symbol)
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

    if strategy_name in {"dual_mom", "sma200"}:
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
) -> None:
    if not actions_out:
        return
    out_path = Path(actions_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)

    if strategy_name in {"dual_mom", "sma200"}:
        tracker_actions = build_dual_tracker_actions(bars, dual_actions)
        tracker_actions.to_csv(out_path, index=False)
    else:
        pd.DataFrame(columns=TRACKER_COLUMNS).to_csv(out_path, index=False)


def _position_label(pos: int, allow_short: bool) -> str:
    if pos > 0:
        return "LONG"
    if pos < 0 and allow_short:
        return "SHORT"
    return "CASH"


def maybe_print_latest_tsmom(
    symbol: str,
    bars: pd.DataFrame,
    weights: pd.Series,
    allow_short: bool,
    print_latest: bool,
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

    print("Latest Date:", last_date.date().isoformat())
    print(f"Latest Position ({symbol}):", latest_label)
    if recent_change_date is not None:
        print("Most Recent Position Change:", recent_change_date.date().isoformat())
    else:
        print("Most Recent Position Change:", "N/A")
    print("ACTION:", action)


def _next_rebalance_date(last_date: pd.Timestamp, rebalance: str) -> pd.Timestamp:
    if rebalance == "W":
        days_ahead = (4 - int(last_date.weekday())) % 7
        if days_ahead == 0:
            days_ahead = 7
        return last_date + pd.Timedelta(days=days_ahead)

    next_month_end = last_date + pd.offsets.BMonthEnd(0)
    if next_month_end <= last_date:
        next_month_end = last_date + pd.offsets.BMonthEnd(1)
    return next_month_end


def _next_rebalance_hint(last_date: pd.Timestamp, rebalance: str) -> str:
    next_rebalance = _next_rebalance_date(last_date, rebalance)
    if rebalance == "W":
        return f"next Friday ({next_rebalance.date().isoformat()})"
    return f"next business month-end ({next_rebalance.date().isoformat()})"


def maybe_write_dual_checklist(
    out_path: str | None,
    rebalance: str,
    risk_symbols: list[str],
    defensive: str | None,
    bars: pd.DataFrame,
    weights: pd.DataFrame,
) -> None:
    if not out_path:
        return

    actions = build_dual_actions(bars, weights)
    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])
    previous = str(active_symbol.iloc[-2]) if len(active_symbol) >= 2 else "CASH"
    action = _classify_symbol_action(previous, current)
    if action == "HOLD":
        if actions.empty:
            last_action_date = "N/A"
            last_action = "HOLD"
        else:
            last_action_date = str(actions.iloc[-1]["date"])
            last_action = str(actions.iloc[-1]["action"])
    else:
        last_action_date = last_date.date().isoformat()
        last_action = action

    close_panel = bars.xs("close", axis=1, level=1)
    if current == "CASH":
        target_shares = 0
        price_txt = "N/A"
    else:
        latest_price = float(close_panel.loc[last_date, current])
        target_shares = int(10_000 // latest_price)
        price_txt = f"{latest_price:.2f}"

    check_day = "last trading day of each week after close" if rebalance == "W" else "last trading day of each month after close"
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
    rebalance: str,
    print_latest: bool,
) -> None:
    if not print_latest:
        return

    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])
    previous = str(active_symbol.iloc[-2]) if len(active_symbol) >= 2 else "CASH"
    action_last_bar = _classify_symbol_action(previous, current)

    previous_symbol = active_symbol.shift(1).fillna("CASH")
    change_dates = active_symbol.index[active_symbol != previous_symbol]
    if len(change_dates):
        last_action_date = change_dates[-1]
        last_action_type = _classify_symbol_action(
            str(previous_symbol.loc[last_action_date]),
            str(active_symbol.loc[last_action_date]),
        )
    else:
        last_action_date = None
        last_action_type = "HOLD"

    close_panel = bars.xs("close", axis=1, level=1)
    if current == "CASH":
        target_shares = 0
        price_txt = "N/A"
    else:
        latest_price = float(close_panel.loc[last_date, current])
        target_shares = int(10_000 // latest_price)
        price_txt = f"{latest_price:.2f}"

    print("Latest Date:", last_date.date().isoformat())
    print("Currently Held:", current)
    if last_action_date is None:
        print("Last Action Date:", "N/A")
    else:
        print("Last Action Date:", last_action_date.date().isoformat())
    print("Last Action Type:", last_action_type)
    print("Next Rebalance:", _next_rebalance_hint(last_date, rebalance))
    print(f"Target Shares ($10,000): {target_shares} @ {price_txt}")
    print("ACTION:", action_last_bar)


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
) -> None:
    if not out_path:
        return

    actions = build_dual_actions(bars, weights)
    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])

    previous_symbol = active_symbol.shift(1).fillna("CASH")
    change_dates = active_symbol.index[active_symbol != previous_symbol]
    if len(change_dates):
        last_action_date = change_dates[-1].date().isoformat()
        last_action_type = _classify_symbol_action(
            str(previous_symbol.loc[change_dates[-1]]),
            str(active_symbol.loc[change_dates[-1]]),
        )
    elif actions.empty:
        last_action_date = "N/A"
        last_action_type = "HOLD"
    else:
        last_action_date = str(actions.iloc[-1]["date"])
        last_action_type = str(actions.iloc[-1]["action"])

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
) -> None:
    if not print_latest:
        return

    active_symbol = _active_symbol_from_weights(weights)
    last_date = bars.index[-1]
    current = str(active_symbol.iloc[-1])

    previous_symbol = active_symbol.shift(1).fillna("CASH")
    change_dates = active_symbol.index[active_symbol != previous_symbol]
    if len(change_dates):
        last_action_date = change_dates[-1]
        last_action_type = _classify_symbol_action(
            str(previous_symbol.loc[last_action_date]),
            str(active_symbol.loc[last_action_date]),
        )
    else:
        last_action_date = None
        last_action_type = "HOLD"

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

    print("Latest Date:", last_date.date().isoformat())
    print("Risk-On State:", risk_state)
    print("Currently Held:", current)
    if last_action_date is None:
        print("Last Action Date:", "N/A")
    else:
        print("Last Action Date:", last_action_date.date().isoformat())
    print("Last Action Type:", last_action_type)
    print("Next Rebalance:", _next_rebalance_hint(last_date, rebalance))
    print(f"Target Shares ($10,000): {target_shares} @ {price_txt}")


def main() -> None:
    args = parse_args()
    store = LocalStore(base_dir=args.data_dir)

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
        symbols_to_load = args.symbols + ([defensive_symbol] if defensive_symbol else [])
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = DualMomentumStrategy(
            risk_universe=args.symbols,
            defensive=defensive_symbol,
            lookback=args.mom_lookback,
            rebalance=args.rebalance,
        )
        plot_label = "dual_momentum"
    else:
        defensive_symbol = _normalize_defensive_symbol(args.defensive)
        symbols_to_load = [args.risk_symbol] + ([defensive_symbol] if defensive_symbol else [])
        bars = load_multi_asset_bars(store, symbols_to_load, args.start, args.end)
        strategy = Sma200RegimeStrategy(
            risk_symbol=args.risk_symbol,
            defensive=defensive_symbol,
            sma_window=args.sma_window,
            rebalance=args.rebalance,
        )
        plot_label = "sma200_regime"

    result = run_backtest(
        bars,
        strategy,
        slippage_bps=args.slippage_bps,
        commission_bps=args.commission_bps,
    )

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

    dual_actions = pd.DataFrame()
    if args.strategy in {"dual_mom", "sma200"}:
        dual_actions = build_dual_actions(bars, result.weights)  # type: ignore[arg-type]

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
        )
        maybe_print_latest_dual(
            bars,
            result.weights,  # type: ignore[arg-type]
            args.rebalance,
            args.print_latest,
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
        )
        maybe_print_latest_sma200(
            bars,
            result.weights,  # type: ignore[arg-type]
            args.risk_symbol,
            args.sma_window,
            args.rebalance,
            args.print_latest,
        )
    else:
        maybe_print_latest_tsmom(
            args.symbol,
            bars,
            result.weights,  # type: ignore[arg-type]
            allow_short=not args.long_only,
            print_latest=args.print_latest,
        )

    maybe_write_trades(
        args.strategy,
        args.trades_out,
        args.symbol,
        bars,
        result.weights,
        dual_actions,
    )
    maybe_write_actions_csv(args.strategy, args.actions_out, bars, dual_actions)
    maybe_write_tracker_template(args.tracker_template_out)
    maybe_plot_equity(result.equity, plot_label, args.plot_out, args.no_plot)


if __name__ == "__main__":
    main()
