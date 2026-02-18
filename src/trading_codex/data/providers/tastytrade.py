from __future__ import annotations

import os
from io import StringIO
from typing import Iterable

import pandas as pd
import requests

from trading_codex.data.datasource import DataSource


class TastytradeDataSource(DataSource):
    """Scaffold for Tastytrade data access (auth and API calls are not implemented)."""

    def __init__(self) -> None:
        self.username = os.getenv("TASTYTRADE_USERNAME")
        self.password = os.getenv("TASTYTRADE_PASSWORD")
        self.account = os.getenv("TASTYTRADE_ACCOUNT")

    def _missing_creds(self) -> list[str]:
        return [
            name
            for name, value in (
                ("TASTYTRADE_USERNAME", self.username),
                ("TASTYTRADE_PASSWORD", self.password),
                ("TASTYTRADE_ACCOUNT", self.account),
            )
            if not value
        ]

    def get_daily_bars(
        self,
        symbols: Iterable[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> pd.DataFrame:
        missing = self._missing_creds()
        raise NotImplementedError(
            "TastytradeDataSource.get_daily_bars is not implemented yet. "
            f"Expected credentials in env vars: TASTYTRADE_USERNAME, "
            f"TASTYTRADE_PASSWORD, TASTYTRADE_ACCOUNT. Missing now: {missing}."
        )

    def get_latest_quotes(self, symbols: Iterable[str]) -> pd.DataFrame:
        missing = self._missing_creds()
        raise NotImplementedError(
            "TastytradeDataSource.get_latest_quotes is not implemented yet. "
            f"Expected credentials in env vars: TASTYTRADE_USERNAME, "
            f"TASTYTRADE_PASSWORD, TASTYTRADE_ACCOUNT. Missing now: {missing}."
        )


class StooqDataSource(DataSource):
    """Daily bars provider backed by Stooq CSV downloads."""

    BASE_URL = "https://stooq.com/q/d/l/"

    def __init__(self, timeout: float = 30.0) -> None:
        self.session = requests.Session()
        self.timeout = timeout

    def _fetch_symbol_daily_bars(
        self,
        symbol: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        response = self.session.get(
            self.BASE_URL,
            params={"s": f"{symbol.lower()}.us", "i": "d"},
            timeout=self.timeout,
        )
        response.raise_for_status()

        df = pd.read_csv(StringIO(response.text))
        if "Date" not in df.columns:
            raise ValueError(f"Unexpected Stooq response for symbol {symbol!r}.")
        if df.empty:
            return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

        bars = (
            df.assign(Date=pd.to_datetime(df["Date"]))
            .set_index("Date")
            .sort_index()
            .rename(columns=str.lower)
        )
        bars = bars[["open", "high", "low", "close", "volume"]]
        return bars.loc[(bars.index >= start_ts) & (bars.index <= end_ts)]

    def get_daily_bars(
        self,
        symbols: Iterable[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> pd.DataFrame:
        start_ts = pd.to_datetime(start)
        end_ts = pd.to_datetime(end)
        symbol_list = list(symbols)
        if not symbol_list:
            return pd.DataFrame()

        bars_by_symbol = {
            symbol: self._fetch_symbol_daily_bars(symbol, start_ts, end_ts)
            for symbol in symbol_list
        }
        return pd.concat(bars_by_symbol, axis=1)

    def get_latest_quotes(self, symbols: Iterable[str]) -> pd.DataFrame:
        raise NotImplementedError(
            "StooqDataSource.get_latest_quotes is not implemented; use get_daily_bars."
        )
