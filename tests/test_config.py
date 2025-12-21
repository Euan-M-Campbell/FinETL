"""Tests for configuration parsing and validation."""

from datetime import date
from pathlib import Path

import pytest
import yaml

from finetl.config import (
    DataTypesConfig,
    ExtractionConfig,
    FinancialsConfig,
    FinETLConfig,
    Frequency,
    Interval,
    OHLCVConfig,
    StatementType,
    load_config,
    parse_config,
)
from finetl.exceptions import ConfigurationError


class TestOHLCVConfig:
    """Tests for OHLCVConfig."""

    def test_valid_config(self):
        config = OHLCVConfig(
            enabled=True,
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 1),
            interval=Interval.ONE_DAY,
        )
        assert config.enabled is True
        assert config.start_date == date(2024, 1, 1)
        assert config.end_date == date(2024, 12, 1)
        assert config.interval == Interval.ONE_DAY

    def test_default_interval(self):
        config = OHLCVConfig(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 12, 1),
        )
        assert config.interval == Interval.ONE_DAY

    def test_invalid_date_range(self):
        with pytest.raises(ValueError, match="start_date must be before end_date"):
            OHLCVConfig(
                start_date=date(2024, 12, 1),
                end_date=date(2024, 1, 1),
            )

    def test_same_date(self):
        with pytest.raises(ValueError, match="start_date must be before end_date"):
            OHLCVConfig(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 1, 1),
            )


class TestFinancialsConfig:
    """Tests for FinancialsConfig."""

    def test_valid_config(self):
        config = FinancialsConfig(
            enabled=True,
            frequency=Frequency.QUARTERLY,
            statements=[StatementType.BALANCE_SHEET],
        )
        assert config.enabled is True
        assert config.frequency == Frequency.QUARTERLY
        assert config.statements == [StatementType.BALANCE_SHEET]

    def test_default_values(self):
        config = FinancialsConfig()
        assert config.enabled is True
        assert config.frequency == Frequency.ANNUAL
        assert len(config.statements) == 3


class TestDataTypesConfig:
    """Tests for DataTypesConfig."""

    def test_ohlcv_only(self):
        config = DataTypesConfig(
            ohlcv=OHLCVConfig(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 1),
            )
        )
        assert config.ohlcv is not None
        assert config.financials is None

    def test_financials_only(self):
        config = DataTypesConfig(financials=FinancialsConfig())
        assert config.ohlcv is None
        assert config.financials is not None

    def test_both_enabled(self):
        config = DataTypesConfig(
            ohlcv=OHLCVConfig(
                start_date=date(2024, 1, 1),
                end_date=date(2024, 12, 1),
            ),
            financials=FinancialsConfig(),
        )
        assert config.ohlcv is not None
        assert config.financials is not None

    def test_none_enabled_raises(self):
        with pytest.raises(ValueError, match="At least one data type must be enabled"):
            DataTypesConfig()

    def test_disabled_ohlcv_only(self):
        with pytest.raises(ValueError, match="At least one data type must be enabled"):
            DataTypesConfig(
                ohlcv=OHLCVConfig(
                    enabled=False,
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 12, 1),
                )
            )


class TestExtractionConfig:
    """Tests for ExtractionConfig."""

    def test_valid_config(self):
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
        assert config.source == "yfinance"
        assert config.tickers == ["AAPL", "MSFT"]

    def test_empty_tickers_raises(self):
        with pytest.raises(ValueError):
            ExtractionConfig(
                source="yfinance",
                tickers=[],
                data_types=DataTypesConfig(
                    ohlcv=OHLCVConfig(
                        start_date=date(2024, 1, 1),
                        end_date=date(2024, 12, 1),
                    )
                ),
            )


class TestFinETLConfig:
    """Tests for FinETLConfig."""

    def test_valid_config(self, sample_config: FinETLConfig):
        assert sample_config.name == "test-pipeline"
        assert sample_config.extraction.source == "yfinance"
        assert sample_config.loading.destination == "csv"


class TestParseConfig:
    """Tests for parse_config function."""

    def test_parse_valid_dict(self, sample_config_dict: dict):
        config = parse_config(sample_config_dict)
        assert config.name == "test-pipeline"
        assert config.extraction.tickers == ["AAPL", "MSFT"]

    def test_parse_invalid_dict(self):
        with pytest.raises(ConfigurationError, match="Invalid configuration"):
            parse_config({"name": "test"})  # Missing required fields


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_valid_yaml(self, tmp_path: Path, sample_config_dict: dict):
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(sample_config_dict, f)

        config = load_config(config_path)
        assert config.name == "test-pipeline"

    def test_load_nonexistent_file(self, tmp_path: Path):
        with pytest.raises(ConfigurationError, match="Config file not found"):
            load_config(tmp_path / "nonexistent.yaml")

    def test_load_invalid_yaml(self, tmp_path: Path):
        config_path = tmp_path / "invalid.yaml"
        with open(config_path, "w") as f:
            f.write("invalid: yaml: content: [")

        with pytest.raises(ConfigurationError, match="Invalid YAML syntax"):
            load_config(config_path)

    def test_load_non_dict_yaml(self, tmp_path: Path):
        config_path = tmp_path / "list.yaml"
        with open(config_path, "w") as f:
            yaml.dump(["item1", "item2"], f)

        with pytest.raises(ConfigurationError, match="must contain a YAML mapping"):
            load_config(config_path)
