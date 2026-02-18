"""Run a tiny demo backtest over locally cached daily bars."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from trading_codex.backtest.engine import run_backtest
from trading_codex.backtest import metrics
from trading_codex.data import LocalStore
from trading_codex.strategies.trend_tsmom import TrendTSMOM


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run trend backtest on locally cached daily bars."
    )
    parser.add_argument("--symbol", default="SPY", help="Ticker symbol to backtest.")
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
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Skip plotting.",
    )
    parser.add_argument(
        "--lookback",
        type=int,
        default=20,
        help="Trend lookback window in trading days (default: 20).",
    )
    parser.add_argument(
        "--long-only",
        action="store_true",
        help="Disable short exposure (negative signals become flat).",
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
        "--trades-out",
        default=None,
        help="Optional CSV path to write a trade log derived from backtest weights.",
    )
    parser.add_argument(
        "--print-latest",
        action="store_true",
        help="Print latest bar date, position, recent change date, and action.",
    )
    return parser.parse_args()


def _position_from_weight(weight: float) -> int:
    if weight > 0:
        return 1
    if weight < 0:
        return -1
    return 0


def build_trade_log(
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


def maybe_write_trade_log(
    symbol: str,
    bars: pd.DataFrame,
    weights: pd.Series,
    trades_out: str | None,
) -> None:
    if not trades_out:
        return

    close = bars["close"].astype(float)
    trades = build_trade_log(symbol, close, weights)
    out_path = Path(trades_out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    trades.to_csv(out_path, index=False)


def _position_label(pos: int, allow_short: bool) -> str:
    if pos > 0:
        return "LONG"
    if pos < 0 and allow_short:
        return "SHORT"
    return "CASH"


def maybe_print_latest(
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

    if len(positions) >= 2:
        prev_pos = int(positions.iloc[-2])
    else:
        prev_pos = 0

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
    symbol: str,
    plot_out: str | None,
    no_plot: bool,
) -> None:
    if no_plot:
        return

    equity.plot(title=f"{symbol} Trend Strategy Equity")
    plt.tight_layout()

    if plot_out:
        out_path = Path(plot_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path)
    elif has_interactive_display():
        plt.show()
    else:
        out_path = Path("outputs") / f"backtest_{symbol}.png"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        plt.savefig(out_path)

    plt.close()


def main() -> None:
    args = parse_args()
    store = LocalStore(base_dir=args.data_dir)
    bars = store.read_bars(args.symbol, start=args.start, end=args.end)
    if bars.empty:
        raise ValueError(
            f"No bars available for symbol={args.symbol!r} in the requested date range."
        )

    strat = TrendTSMOM(lookback=args.lookback, allow_short=not args.long_only)
    result = run_backtest(
        bars,
        strat,
        slippage_bps=args.slippage_bps,
        commission_bps=args.commission_bps,
    )

    print("CAGR:", round(metrics.cagr(result.returns), 4))
    print("Vol:", round(metrics.vol(result.returns), 4))
    print("Sharpe:", round(metrics.sharpe(result.returns), 4))
    print("Max DD:", round(metrics.max_drawdown(result.returns), 4))
    print("Turnover:", round(metrics.turnover(result.weights), 4))

    maybe_print_latest(
        args.symbol,
        bars,
        result.weights,
        allow_short=not args.long_only,
        print_latest=args.print_latest,
    )
    maybe_write_trade_log(args.symbol, bars, result.weights, args.trades_out)
    maybe_plot_equity(result.equity, args.symbol, args.plot_out, args.no_plot)


if __name__ == "__main__":
    main()
