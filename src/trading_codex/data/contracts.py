"""Shared data contracts and lightweight validators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

BAR_COLUMNS = ["open", "high", "low", "close", "volume"]
SIGNAL_COLUMNS = ["signal"]
WEIGHT_COLUMNS = ["weight"]


@dataclass(frozen=True)
class OHLCVSchema:
    columns: Iterable[str] = tuple(BAR_COLUMNS)


@dataclass(frozen=True)
class SignalSchema:
    columns: Iterable[str] = tuple(SIGNAL_COLUMNS)


@dataclass(frozen=True)
class WeightSchema:
    columns: Iterable[str] = tuple(WEIGHT_COLUMNS)


def _require_columns(df: pd.DataFrame, columns: Iterable[str], name: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise ValueError(f"{name} missing columns: {missing}")


def validate_bars(df: pd.DataFrame) -> None:
    _require_columns(df, BAR_COLUMNS, "bars")


def validate_signals(df: pd.DataFrame) -> None:
    _require_columns(df, SIGNAL_COLUMNS, "signals")


def validate_weights(df: pd.DataFrame) -> None:
    _require_columns(df, WEIGHT_COLUMNS, "weights")
