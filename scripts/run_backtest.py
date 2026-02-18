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
    return parser.parse_args()


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

    maybe_plot_equity(result.equity, args.symbol, args.plot_out, args.no_plot)


if __name__ == "__main__":
    main()
