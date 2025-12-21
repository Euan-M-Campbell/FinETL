"""Pydantic models for FinETL configuration validation."""

from datetime import date
from enum import Enum
from typing import Literal

from pydantic import BaseModel, Field, model_validator


class Interval(str, Enum):
    """Valid intervals for OHLCV data."""

    ONE_MINUTE = "1m"
    TWO_MINUTES = "2m"
    FIVE_MINUTES = "5m"
    FIFTEEN_MINUTES = "15m"
    THIRTY_MINUTES = "30m"
    SIXTY_MINUTES = "60m"
    NINETY_MINUTES = "90m"
    ONE_HOUR = "1h"
    ONE_DAY = "1d"
    FIVE_DAYS = "5d"
    ONE_WEEK = "1wk"
    ONE_MONTH = "1mo"
    THREE_MONTHS = "3mo"


class Frequency(str, Enum):
    """Frequency for financial statements."""

    ANNUAL = "annual"
    QUARTERLY = "quarterly"


class StatementType(str, Enum):
    """Types of financial statements."""

    BALANCE_SHEET = "balance_sheet"
    INCOME_STATEMENT = "income_statement"
    CASH_FLOW = "cash_flow"


class OHLCVConfig(BaseModel):
    """Configuration for OHLCV data extraction."""

    enabled: bool = True
    start_date: date
    end_date: date
    interval: Interval = Interval.ONE_DAY

    @model_validator(mode="after")
    def validate_dates(self) -> "OHLCVConfig":
        if self.start_date >= self.end_date:
            raise ValueError("start_date must be before end_date")
        return self


class FinancialsConfig(BaseModel):
    """Configuration for financial statements extraction."""

    enabled: bool = True
    frequency: Frequency = Frequency.ANNUAL
    statements: list[StatementType] = Field(
        default_factory=lambda: [
            StatementType.BALANCE_SHEET,
            StatementType.INCOME_STATEMENT,
            StatementType.CASH_FLOW,
        ]
    )


class DataTypesConfig(BaseModel):
    """Configuration for data types to extract."""

    ohlcv: OHLCVConfig | None = None
    financials: FinancialsConfig | None = None

    @model_validator(mode="after")
    def at_least_one_enabled(self) -> "DataTypesConfig":
        ohlcv_enabled = self.ohlcv is not None and self.ohlcv.enabled
        financials_enabled = self.financials is not None and self.financials.enabled
        if not ohlcv_enabled and not financials_enabled:
            raise ValueError("At least one data type must be enabled")
        return self


class ExtractionConfig(BaseModel):
    """Configuration for data extraction."""

    source: Literal["yfinance"] = "yfinance"
    tickers: list[str] = Field(..., min_length=1)
    data_types: DataTypesConfig


class IfExistsType(str, Enum):
    """Behavior when table already exists."""

    FAIL = "fail"
    REPLACE = "replace"
    APPEND = "append"


class LoadingConfig(BaseModel):
    """Configuration for data loading."""

    destination: Literal["csv", "parquet", "huggingface", "postgresql"] = "csv"
    path: str = "./output"

    # HuggingFace-specific options
    repo_id: str | None = None
    private: bool = False

    # PostgreSQL-specific options
    host: str | None = None
    port: int = 5432
    database: str | None = None
    schema_name: str = "public"
    user: str | None = None
    password: str | None = None
    if_exists: IfExistsType = IfExistsType.APPEND

    @model_validator(mode="after")
    def validate_destination_config(self) -> "LoadingConfig":
        if self.destination == "huggingface" and not self.repo_id:
            raise ValueError("repo_id is required for huggingface destination")
        if self.destination == "postgresql":
            missing = []
            if not self.host:
                missing.append("host")
            if not self.database:
                missing.append("database")
            if not self.user:
                missing.append("user")
            if not self.password:
                missing.append("password")
            if missing:
                raise ValueError(f"PostgreSQL requires: {', '.join(missing)}")
        return self


class FinETLConfig(BaseModel):
    """Root configuration for FinETL pipeline."""

    name: str
    extraction: ExtractionConfig
    loading: LoadingConfig
