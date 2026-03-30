from __future__ import annotations

import os
from typing import Iterable

import pandas as pd

from trading_codex.data.datasource import DataSource
from trading_codex.data.providers.stooq import StooqDataSource


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
