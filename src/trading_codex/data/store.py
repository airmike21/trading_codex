from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd


class LocalStore:
    """Local parquet-backed store under ./data by default."""

    def __init__(self, base_dir: str | Path = "data") -> None:
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, symbol: str) -> Path:
        safe_symbol = symbol.replace("/", "_")
        return self.base_dir / f"{safe_symbol}.parquet"

    def write_bars(self, symbol: str, df: pd.DataFrame) -> None:
        if df.empty:
            raise ValueError("Cannot write empty DataFrame.")

        out = df.copy()
        if not isinstance(out.index, pd.DatetimeIndex):
            out.index = pd.to_datetime(out.index)
        out = out.sort_index()

        path = self._path_for(symbol)
        out.to_parquet(path)

    def read_bars(
        self,
        symbol: str,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ) -> pd.DataFrame:
        path = self._path_for(symbol)
        if not path.exists():
            raise FileNotFoundError(f"No data for symbol: {symbol}")

        df = pd.read_parquet(path)
        if not isinstance(df.index, pd.DatetimeIndex):
            df.index = pd.to_datetime(df.index)
        df = df.sort_index()

        start_ts = pd.to_datetime(start) if start is not None else None
        end_ts = pd.to_datetime(end) if end is not None else None
        if start_ts is not None and end_ts is not None:
            return df.loc[start_ts:end_ts]
        if start_ts is not None:
            return df.loc[start_ts:]
        if end_ts is not None:
            return df.loc[:end_ts]
        return df

    def build_panel(
        self,
        symbols: Iterable[str],
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        fields: Iterable[str] = ("open", "high", "low", "close", "volume"),
    ) -> pd.DataFrame:
        symbols_list = list(symbols)
        if not symbols_list:
            raise ValueError("No symbols provided.")

        start_ts = pd.to_datetime(start)
        end_ts = pd.to_datetime(end)
        date_index = pd.date_range(start=start_ts, end=end_ts, freq="D")

        fields_list = list(fields)
        frames = []

        for symbol in symbols_list:
            df = self.read_bars(symbol, start=start_ts, end=end_ts)
            missing = [field for field in fields_list if field not in df.columns]
            if missing:
                raise ValueError(f"Missing fields for {symbol}: {missing}")

            aligned = df.loc[:, fields_list].reindex(date_index)
            aligned.columns = pd.MultiIndex.from_product([[symbol], fields_list])
            frames.append(aligned)

        panel = pd.concat(frames, axis=1)
        panel = panel.sort_index()
        panel.index.name = "date"
        return panel
