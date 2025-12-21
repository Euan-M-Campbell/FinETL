"""Shared test fixtures."""

from datetime import date

import pandas as pd
import pytest

from finetl.config.schema import (
    DataTypesConfig,
    ExtractionConfig,
    FinancialsConfig,
    FinETLConfig,
    Frequency,
    LoadingConfig,
    OHLCVConfig,
    StatementType,
)
from finetl.models import ExtractedData


@pytest.fixture
def sample_ohlcv_config() -> OHLCVConfig:
    """Sample OHLCV configuration."""
    return OHLCVConfig(
        enabled=True,
        start_date=date(2024, 1, 1),
        end_date=date(2024, 12, 1),
    )


@pytest.fixture
def sample_financials_config() -> FinancialsConfig:
    """Sample financials configuration."""
    return FinancialsConfig(
        enabled=True,
        frequency=Frequency.QUARTERLY,
        statements=[
            StatementType.BALANCE_SHEET,
            StatementType.INCOME_STATEMENT,
            StatementType.CASH_FLOW,
        ],
    )


@pytest.fixture
def sample_extraction_config(
    sample_ohlcv_config: OHLCVConfig,
    sample_financials_config: FinancialsConfig,
) -> ExtractionConfig:
    """Sample extraction configuration."""
    return ExtractionConfig(
        source="yfinance",
        tickers=["AAPL", "MSFT"],
        data_types=DataTypesConfig(
            ohlcv=sample_ohlcv_config,
            financials=sample_financials_config,
        ),
    )


@pytest.fixture
def sample_loading_config(tmp_path) -> LoadingConfig:
    """Sample loading configuration."""
    return LoadingConfig(
        destination="csv",
        path=str(tmp_path / "output"),
    )


@pytest.fixture
def sample_config(
    sample_extraction_config: ExtractionConfig,
    sample_loading_config: LoadingConfig,
) -> FinETLConfig:
    """Sample full configuration."""
    return FinETLConfig(
        name="test-pipeline",
        extraction=sample_extraction_config,
        loading=sample_loading_config,
    )


@pytest.fixture
def sample_ohlcv_df() -> pd.DataFrame:
    """Sample OHLCV DataFrame."""
    return pd.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "MSFT", "MSFT"],
            "date": pd.to_datetime(
                ["2024-01-02", "2024-01-03", "2024-01-02", "2024-01-03"]
            ),
            "open": [185.50, 185.90, 372.00, 374.00],
            "high": [186.20, 187.10, 375.50, 376.00],
            "low": [184.80, 185.50, 371.20, 373.00],
            "close": [185.90, 186.80, 374.30, 375.50],
            "volume": [45000000, 42000000, 22000000, 21000000],
        }
    )


@pytest.fixture
def sample_financials_df() -> pd.DataFrame:
    """Sample financials DataFrame."""
    return pd.DataFrame(
        {
            "ticker": ["AAPL", "AAPL", "MSFT"],
            "period": pd.to_datetime(["2024-09-30", "2024-06-30", "2024-06-30"]),
            "Total Assets": [352583000000, 348000000000, 512163000000],
            "Total Liabilities": [308030000000, 305000000000, 243686000000],
            "Total Revenue": [394328000000, 385000000000, 245122000000],
            "Operating Cash Flow": [118254000000, 115000000000, 118548000000],
        }
    )


@pytest.fixture
def sample_extracted_data(
    sample_ohlcv_df: pd.DataFrame,
    sample_financials_df: pd.DataFrame,
) -> ExtractedData:
    """Sample extracted data."""
    return ExtractedData(ohlcv=sample_ohlcv_df, financials=sample_financials_df)


@pytest.fixture
def sample_config_dict() -> dict:
    """Sample configuration as a dictionary."""
    return {
        "name": "test-pipeline",
        "extraction": {
            "source": "yfinance",
            "tickers": ["AAPL", "MSFT"],
            "data_types": {
                "ohlcv": {
                    "enabled": True,
                    "start_date": "2024-01-01",
                    "end_date": "2024-12-01",
                    "interval": "1d",
                },
                "financials": {
                    "enabled": True,
                    "frequency": "quarterly",
                    "statements": ["balance_sheet", "income_statement", "cash_flow"],
                },
            },
        },
        "loading": {
            "destination": "csv",
            "path": "./output",
        },
    }
