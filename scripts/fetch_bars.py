from __future__ import annotations

import argparse
from typing import Iterable

from trading_codex.data import LocalStore
from trading_codex.data.datasource import DataSource
from trading_codex.data.providers import TastytradeDataSource


def fetch_and_store(
    data_source: DataSource,
    store: LocalStore,
    symbols: Iterable[str],
    start: str,
    end: str,
) -> None:
    bars = data_source.get_daily_bars(symbols, start, end)
    for symbol in symbols:
        if symbol not in bars.columns.get_level_values(0):
            raise ValueError(f"Missing symbol in returned data: {symbol}")
        df = bars[symbol].copy()
        store.write_bars(symbol, df)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch daily bars and cache locally.")
    parser.add_argument("--provider", default="tastytrade")
    parser.add_argument("--symbols", nargs="+", required=True)
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    args = parser.parse_args()

    store = LocalStore()

    if args.provider == "tastytrade":
        data_source = TastytradeDataSource()
    else:
        raise ValueError(f"Unknown provider: {args.provider}")

    fetch_and_store(data_source, store, args.symbols, args.start, args.end)


if __name__ == "__main__":
    main()
