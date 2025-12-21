"""Data models for FinETL."""

from dataclasses import dataclass

import pandas as pd


@dataclass
class ExtractedData:
    """Container for extracted financial data.

    Attributes:
        ohlcv: Combined OHLCV data for all tickers with columns:
            ticker, date, open, high, low, close, volume
        financials: Combined financial statements for all tickers with columns:
            ticker, period, and one column per financial metric
    """

    ohlcv: pd.DataFrame | None = None
    financials: pd.DataFrame | None = None

    def __bool__(self) -> bool:
        """Return True if any data is present."""
        has_ohlcv = self.ohlcv is not None and not self.ohlcv.empty
        has_financials = self.financials is not None and not self.financials.empty
        return has_ohlcv or has_financials
