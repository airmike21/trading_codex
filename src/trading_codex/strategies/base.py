"""Strategy interface."""

from __future__ import annotations

from abc import ABC, abstractmethod
import pandas as pd


class Strategy(ABC):
    """Base strategy interface.

    Contract: signals at time t must only use information available
    up to time t-1 to avoid lookahead bias.
    """

    @abstractmethod
    def generate_signals(self, bars: pd.DataFrame) -> pd.DataFrame:
        """Return a DataFrame with a `signal` column aligned to `bars` index."""
        raise NotImplementedError
