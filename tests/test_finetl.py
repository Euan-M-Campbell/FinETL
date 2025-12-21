"""Tests for main FinETL class."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import yaml

from finetl import FinETL
from finetl.exceptions import ConfigurationError


class TestFinETL:
    """Tests for FinETL class."""

    def test_from_yaml(self, tmp_path: Path, sample_config_dict: dict):
        config_path = tmp_path / "config.yaml"
        with open(config_path, "w") as f:
            yaml.dump(sample_config_dict, f)

        etl = FinETL.from_yaml(config_path)

        assert etl.config.name == "test-pipeline"
        assert etl.config.extraction.tickers == ["AAPL", "MSFT"]

    def test_from_dict(self, sample_config_dict: dict):
        etl = FinETL.from_dict(sample_config_dict)

        assert etl.config.name == "test-pipeline"
        assert etl.config.extraction.tickers == ["AAPL", "MSFT"]

    def test_from_yaml_invalid_file(self, tmp_path: Path):
        with pytest.raises(ConfigurationError):
            FinETL.from_yaml(tmp_path / "nonexistent.yaml")

    def test_from_dict_invalid_config(self):
        with pytest.raises(ConfigurationError):
            FinETL.from_dict({"name": "incomplete"})

    def test_run_pipeline(
        self,
        tmp_path: Path,
        sample_config_dict: dict,
    ):
        # Update output path to tmp_path
        sample_config_dict["loading"]["path"] = str(tmp_path / "output")

        etl = FinETL.from_dict(sample_config_dict)

        # Create mock OHLCV data (simulating yfinance download return)
        dates = pd.date_range("2024-01-02", periods=2, freq="D", name="Date")
        mock_ohlcv = pd.DataFrame(
            {
                "Open": [185.50, 185.90],
                "High": [186.20, 187.10],
                "Low": [184.80, 185.50],
                "Close": [185.90, 186.80],
                "Volume": [45000000, 42000000],
            },
            index=dates,
        )

        mock_balance_sheet = pd.DataFrame(
            {
                pd.Timestamp("2024-09-30"): [352583000000],
            },
            index=["Total Assets"],
        )

        mock_ticker = MagicMock()
        mock_ticker.quarterly_balance_sheet = mock_balance_sheet
        mock_ticker.quarterly_financials = mock_balance_sheet
        mock_ticker.quarterly_cashflow = mock_balance_sheet

        # Create multi-ticker response format
        multi_data = pd.concat(
            {"AAPL": mock_ohlcv, "MSFT": mock_ohlcv},
            axis=1,
        )

        with (
            patch("finetl.extraction.yfinance.yf.download") as mock_download,
            patch("finetl.extraction.yfinance.yf.Ticker") as mock_yf_ticker,
        ):
            mock_download.return_value = multi_data
            mock_yf_ticker.return_value = mock_ticker

            etl.run()

        # Check output files exist
        output_dir = tmp_path / "output"
        assert output_dir.exists()
        assert (output_dir / "ohlcv.csv").exists()
        assert (output_dir / "financials.csv").exists()


class TestFinETLIntegration:
    """Integration tests for FinETL (requires network access)."""

    @pytest.mark.skip(reason="Requires network access - run manually")
    def test_real_yfinance_extraction(self, tmp_path: Path):
        config = {
            "name": "integration-test",
            "extraction": {
                "source": "yfinance",
                "tickers": ["AAPL"],
                "data_types": {
                    "ohlcv": {
                        "enabled": True,
                        "start_date": "2024-01-01",
                        "end_date": "2024-01-10",
                        "interval": "1d",
                    },
                },
            },
            "loading": {
                "destination": "csv",
                "path": str(tmp_path / "output"),
            },
        }

        etl = FinETL.from_dict(config)
        etl.run()

        ohlcv_path = tmp_path / "output" / "ohlcv.csv"
        assert ohlcv_path.exists()

        df = pd.read_csv(ohlcv_path)
        assert "ticker" in df.columns
        assert "AAPL" in df["ticker"].values
