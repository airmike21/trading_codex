from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

import pandas as pd


class DataSource(ABC):
    """Abstract market data source."""

    @abstractmethod
    def get_daily_bars(
        self,
        symbols: Iterable[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
    ) -> pd.DataFrame:
        """Return daily bars with MultiIndex columns (symbol, field)."""
        raise NotImplementedError

    @abstractmethod
    def get_latest_quotes(self, symbols: Iterable[str]) -> pd.DataFrame:
        """Return latest quotes for symbols."""
        raise NotImplementedError
