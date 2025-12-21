"""Tests for data extraction."""

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from finetl.config.schema import (
    DataTypesConfig,
    ExtractionConfig,
    FinancialsConfig,
    Frequency,
    OHLCVConfig,
    StatementType,
)
from finetl.extraction import YFinanceExtractor


@pytest.fixture
def mock_ohlcv_data() -> pd.DataFrame:
    """Mock OHLCV data from yfinance."""
    dates = pd.date_range("2024-01-02", periods=3, freq="D", name="Date")
    return pd.DataFrame(
        {
            "Open": [185.50, 185.90, 186.50],
            "High": [186.20, 187.10, 187.50],
            "Low": [184.80, 185.50, 186.00],
            "Close": [185.90, 186.80, 187.20],
            "Volume": [45000000, 42000000, 43000000],
        },
        index=dates,
    )


@pytest.fixture
def mock_balance_sheet() -> pd.DataFrame:
    """Mock balance sheet data from yfinance."""
    periods = pd.to_datetime(["2024-09-30", "2024-06-30"])
    return pd.DataFrame(
        {
            periods[0]: [352583000000, 308030000000],
            periods[1]: [348000000000, 305000000000],
        },
        index=["Total Assets", "Total Liabilities Net Minority Interest"],
    )


@pytest.fixture
def mock_income_statement() -> pd.DataFrame:
    """Mock income statement data from yfinance."""
    periods = pd.to_datetime(["2024-09-30", "2024-06-30"])
    return pd.DataFrame(
        {
            periods[0]: [394328000000, 94680000000],
            periods[1]: [385000000000, 92000000000],
        },
        index=["Total Revenue", "Net Income"],
    )


@pytest.fixture
def mock_cashflow() -> pd.DataFrame:
    """Mock cash flow data from yfinance."""
    periods = pd.to_datetime(["2024-09-30", "2024-06-30"])
    return pd.DataFrame(
        {
            periods[0]: [118254000000, -22000000000],
            periods[1]: [115000000000, -20000000000],
        },
        index=["Operating Cash Flow", "Capital Expenditure"],
    )


class TestYFinanceExtractor:
    """Tests for YFinanceExtractor."""

    def test_extract_ohlcv_single_ticker(self, mock_ohlcv_data: pd.DataFrame):
        config = ExtractionConfig(
            source="yfinance",
            tickers=["AAPL"],
            data_types=DataTypesConfig(
                ohlcv=OHLCVConfig(
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 12, 1),
                )
            ),
        )
        extractor = YFinanceExtractor(config)

        with patch("finetl.extraction.yfinance.yf.download") as mock_download:
            mock_download.return_value = mock_ohlcv_data

            result = extractor.extract()

        assert result.ohlcv is not None
        assert "ticker" in result.ohlcv.columns
        assert "date" in result.ohlcv.columns
        assert "open" in result.ohlcv.columns
        assert len(result.ohlcv) == 3
        assert result.ohlcv["ticker"].iloc[0] == "AAPL"

    def test_extract_ohlcv_multiple_tickers(self, mock_ohlcv_data: pd.DataFrame):
        config = ExtractionConfig(
            source="yfinance",
            tickers=["AAPL", "MSFT"],
            data_types=DataTypesConfig(
                ohlcv=OHLCVConfig(
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 12, 1),
                )
            ),
        )
        extractor = YFinanceExtractor(config)

        # Create multi-ticker response
        multi_data = pd.concat(
            {"AAPL": mock_ohlcv_data, "MSFT": mock_ohlcv_data},
            axis=1,
        )

        with patch("finetl.extraction.yfinance.yf.download") as mock_download:
            mock_download.return_value = multi_data

            result = extractor.extract()

        assert result.ohlcv is not None
        assert set(result.ohlcv["ticker"].unique()) == {"AAPL", "MSFT"}

    def test_extract_financials(
        self,
        mock_balance_sheet: pd.DataFrame,
        mock_income_statement: pd.DataFrame,
        mock_cashflow: pd.DataFrame,
    ):
        config = ExtractionConfig(
            source="yfinance",
            tickers=["AAPL"],
            data_types=DataTypesConfig(
                financials=FinancialsConfig(
                    frequency=Frequency.QUARTERLY,
                    statements=[
                        StatementType.BALANCE_SHEET,
                        StatementType.INCOME_STATEMENT,
                        StatementType.CASH_FLOW,
                    ],
                )
            ),
        )
        extractor = YFinanceExtractor(config)

        mock_ticker = MagicMock()
        mock_ticker.quarterly_balance_sheet = mock_balance_sheet
        mock_ticker.quarterly_financials = mock_income_statement
        mock_ticker.quarterly_cashflow = mock_cashflow

        with patch("finetl.extraction.yfinance.yf.Ticker") as mock_yf_ticker:
            mock_yf_ticker.return_value = mock_ticker

            result = extractor.extract()

        assert result.financials is not None
        assert "ticker" in result.financials.columns
        assert "period" in result.financials.columns
        assert "Total Assets" in result.financials.columns
        assert result.financials["ticker"].iloc[0] == "AAPL"

    def test_extract_empty_ohlcv(self):
        config = ExtractionConfig(
            source="yfinance",
            tickers=["INVALID"],
            data_types=DataTypesConfig(
                ohlcv=OHLCVConfig(
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 12, 1),
                )
            ),
        )
        extractor = YFinanceExtractor(config)

        with patch("finetl.extraction.yfinance.yf.download") as mock_download:
            mock_download.return_value = pd.DataFrame()

            result = extractor.extract()

        assert result.ohlcv is not None
        assert result.ohlcv.empty

    def test_extract_both_data_types(
        self,
        mock_ohlcv_data: pd.DataFrame,
        mock_balance_sheet: pd.DataFrame,
        mock_income_statement: pd.DataFrame,
        mock_cashflow: pd.DataFrame,
    ):
        config = ExtractionConfig(
            source="yfinance",
            tickers=["AAPL"],
            data_types=DataTypesConfig(
                ohlcv=OHLCVConfig(
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 12, 1),
                ),
                financials=FinancialsConfig(
                    frequency=Frequency.QUARTERLY,
                ),
            ),
        )
        extractor = YFinanceExtractor(config)

        mock_ticker = MagicMock()
        mock_ticker.quarterly_balance_sheet = mock_balance_sheet
        mock_ticker.quarterly_financials = mock_income_statement
        mock_ticker.quarterly_cashflow = mock_cashflow

        with (
            patch("finetl.extraction.yfinance.yf.download") as mock_download,
            patch("finetl.extraction.yfinance.yf.Ticker") as mock_yf_ticker,
        ):
            mock_download.return_value = mock_ohlcv_data
            mock_yf_ticker.return_value = mock_ticker

            result = extractor.extract()

        assert result.ohlcv is not None
        assert result.financials is not None
        assert not result.ohlcv.empty
        assert not result.financials.empty
