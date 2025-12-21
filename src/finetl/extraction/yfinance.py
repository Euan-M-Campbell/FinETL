"""Yahoo Finance data extractor."""

import logging
from typing import Any

import pandas as pd
import yfinance as yf

from finetl.base import BaseExtractor
from finetl.config.schema import (
    ExtractionConfig,
    Frequency,
    StatementType,
)
from finetl.exceptions import ExtractionError
from finetl.models import ExtractedData

logger = logging.getLogger(__name__)


class YFinanceExtractor(BaseExtractor):
    """Extractor for Yahoo Finance data."""

    def __init__(self, config: ExtractionConfig) -> None:
        self.config = config
        self.tickers = config.tickers

    def extract(self) -> ExtractedData:
        """Extract all configured data types."""
        ohlcv_df = None
        financials_df = None

        data_types = self.config.data_types

        if data_types.ohlcv and data_types.ohlcv.enabled:
            logger.info("Extracting OHLCV data for %d tickers", len(self.tickers))
            ohlcv_df = self._extract_ohlcv()

        if data_types.financials and data_types.financials.enabled:
            logger.info("Extracting financial statements for %d tickers", len(self.tickers))
            financials_df = self._extract_financials()

        return ExtractedData(ohlcv=ohlcv_df, financials=financials_df)

    def _extract_ohlcv(self) -> pd.DataFrame:
        """Extract OHLCV data for all tickers."""
        ohlcv_config = self.config.data_types.ohlcv
        if ohlcv_config is None:
            raise ExtractionError("OHLCV config is not set")

        try:
            data = yf.download(
                tickers=self.tickers,
                start=ohlcv_config.start_date.isoformat(),
                end=ohlcv_config.end_date.isoformat(),
                interval=ohlcv_config.interval.value,
                group_by="ticker",
                auto_adjust=True,
                progress=False,
            )
        except Exception as e:
            raise ExtractionError(f"Failed to download OHLCV data: {e}") from e

        if data.empty:
            logger.warning("No OHLCV data returned")
            return pd.DataFrame()

        # Handle single vs multiple tickers
        if len(self.tickers) == 1:
            # Single ticker: simple DataFrame with OHLCV columns
            df = data.reset_index()
            df["ticker"] = self.tickers[0]
        else:
            # Multiple tickers: MultiIndex columns (ticker, price_type)
            frames = []
            for ticker in self.tickers:
                if ticker in data.columns.get_level_values(0):
                    ticker_data = data[ticker].copy()
                    ticker_data = ticker_data.reset_index()
                    ticker_data["ticker"] = ticker
                    frames.append(ticker_data)
            if not frames:
                return pd.DataFrame()
            df = pd.concat(frames, ignore_index=True)

        # Standardize column names
        df.columns = df.columns.str.lower()

        # Handle various date column names from yfinance
        date_columns = ["date", "datetime", "index"]
        for col in date_columns:
            if col in df.columns and col != "date":
                df = df.rename(columns={col: "date"})
                break

        # Reorder columns
        cols = ["ticker", "date", "open", "high", "low", "close", "volume"]
        available_cols = [c for c in cols if c in df.columns]
        df = df[available_cols]

        return df

    def _extract_financials(self) -> pd.DataFrame:
        """Extract and merge financial statements for all tickers."""
        financials_config = self.config.data_types.financials
        if financials_config is None:
            raise ExtractionError("Financials config is not set")

        quarterly = financials_config.frequency == Frequency.QUARTERLY
        statements = financials_config.statements

        all_ticker_data = []

        for ticker_symbol in self.tickers:
            logger.debug("Extracting financials for %s", ticker_symbol)
            try:
                ticker = yf.Ticker(ticker_symbol)
                ticker_financials = self._get_ticker_financials(
                    ticker, statements, quarterly
                )
                if ticker_financials is not None and not ticker_financials.empty:
                    ticker_financials["ticker"] = ticker_symbol
                    all_ticker_data.append(ticker_financials)
            except Exception as e:
                logger.warning("Failed to extract financials for %s: %s", ticker_symbol, e)

        if not all_ticker_data:
            logger.warning("No financial data extracted")
            return pd.DataFrame()

        # Combine all tickers
        df = pd.concat(all_ticker_data, ignore_index=True)

        # Reorder columns to put ticker and period first
        cols = df.columns.tolist()
        cols.remove("ticker")
        cols.remove("period")
        df = df[["ticker", "period"] + cols]

        return df

    def _get_ticker_financials(
        self,
        ticker: Any,
        statements: list[StatementType],
        quarterly: bool,
    ) -> pd.DataFrame | None:
        """Get and merge financial statements for a single ticker."""
        statement_dfs = []

        for statement_type in statements:
            df = self._get_statement(ticker, statement_type, quarterly)
            if df is not None and not df.empty:
                statement_dfs.append(df)

        if not statement_dfs:
            return None

        # Merge all statements on period (outer join to keep all periods)
        result = statement_dfs[0]
        for df in statement_dfs[1:]:
            result = result.merge(df, on="period", how="outer")

        return result

    def _get_statement(
        self,
        ticker: Any,
        statement_type: StatementType,
        quarterly: bool,
    ) -> pd.DataFrame | None:
        """Get a single financial statement and transpose it."""
        try:
            if statement_type == StatementType.BALANCE_SHEET:
                raw = ticker.quarterly_balance_sheet if quarterly else ticker.balance_sheet
            elif statement_type == StatementType.INCOME_STATEMENT:
                raw = ticker.quarterly_financials if quarterly else ticker.financials
            elif statement_type == StatementType.CASH_FLOW:
                raw = ticker.quarterly_cashflow if quarterly else ticker.cashflow
            else:
                return None

            if raw is None or raw.empty:
                return None

            # yfinance returns: rows=metrics, columns=periods (dates)
            # We want: rows=periods, columns=metrics
            df = raw.T.reset_index()
            df = df.rename(columns={"index": "period"})

            # Ensure period is datetime
            df["period"] = pd.to_datetime(df["period"])

            return df

        except Exception as e:
            logger.warning("Failed to get %s: %s", statement_type.value, e)
            return None
