from __future__ import annotations

from io import StringIO
from typing import Iterable

import pandas as pd
import requests

from trading_codex.data.datasource import DataSource


class StooqDataSource(DataSource):
    """Daily bars provider backed by Stooq CSV downloads."""

    BASE_URL = "https://stooq.com/q/l/"
    EXPORT_FORMAT = "sd2t2ohlcv"

    def __init__(self, timeout: float = 30.0, symbol_suffix: str = ".us") -> None:
        self.session = requests.Session()
        self.timeout = timeout
        self.symbol_suffix = symbol_suffix

    def _parse_daily_bars_response(self, *, symbol: str, response: requests.Response) -> pd.DataFrame:
        response_url = getattr(response, "url", self.BASE_URL)
        content_type = str(response.headers.get("content-type", "")).strip() or "unknown"
        raw_text = response.text.lstrip("\ufeff")
        if not raw_text.strip():
            raise ValueError(
                f"Stooq returned an empty response body for symbol {symbol!r} "
                f"(url={response_url}, status={response.status_code}, content_type={content_type})."
            )

        try:
            df = pd.read_csv(StringIO(raw_text))
        except pd.errors.EmptyDataError as exc:
            raise ValueError(
                f"Stooq returned a non-CSV/empty payload for symbol {symbol!r} "
                f"(url={response_url}, status={response.status_code}, content_type={content_type})."
            ) from exc

        if "Date" not in df.columns:
            preview = " ".join(raw_text.splitlines()[:2])[:160]
            raise ValueError(
                f"Unexpected Stooq response for symbol {symbol!r}: missing Date column "
                f"(url={response_url}, status={response.status_code}, content_type={content_type}, preview={preview!r})."
            )
        return df

    def _fetch_symbol_daily_bars(
        self,
        symbol: str,
        start_ts: pd.Timestamp,
        end_ts: pd.Timestamp,
    ) -> pd.DataFrame:
        response = self.session.get(
            self.BASE_URL,
            params={
                "s": f"{symbol.lower()}{self.symbol_suffix}",
                "f": self.EXPORT_FORMAT,
                "h": "",
                "e": "csv",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()

        df = self._parse_daily_bars_response(symbol=symbol, response=response)
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
