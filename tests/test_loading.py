"""Tests for data loading."""

from pathlib import Path

import pandas as pd
import pytest

from finetl.config.schema import LoadingConfig
from finetl.loading import CSVLoader, get_loader, register_loader
from finetl.base import BaseLoader
from finetl.exceptions import ConfigurationError
from finetl.models import ExtractedData


class TestCSVLoader:
    """Tests for CSVLoader."""

    def test_load_ohlcv_only(
        self,
        tmp_path: Path,
        sample_ohlcv_df: pd.DataFrame,
    ):
        config = LoadingConfig(destination="csv", path=str(tmp_path / "output"))
        loader = CSVLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)
        loader.load(data)

        ohlcv_path = tmp_path / "output" / "ohlcv.csv"
        assert ohlcv_path.exists()

        loaded = pd.read_csv(ohlcv_path)
        assert len(loaded) == len(sample_ohlcv_df)
        assert list(loaded.columns) == list(sample_ohlcv_df.columns)

    def test_load_financials_only(
        self,
        tmp_path: Path,
        sample_financials_df: pd.DataFrame,
    ):
        config = LoadingConfig(destination="csv", path=str(tmp_path / "output"))
        loader = CSVLoader(config)

        data = ExtractedData(ohlcv=None, financials=sample_financials_df)
        loader.load(data)

        financials_path = tmp_path / "output" / "financials.csv"
        assert financials_path.exists()

        loaded = pd.read_csv(financials_path)
        assert len(loaded) == len(sample_financials_df)

    def test_load_both(
        self,
        tmp_path: Path,
        sample_extracted_data: ExtractedData,
    ):
        config = LoadingConfig(destination="csv", path=str(tmp_path / "output"))
        loader = CSVLoader(config)

        loader.load(sample_extracted_data)

        ohlcv_path = tmp_path / "output" / "ohlcv.csv"
        financials_path = tmp_path / "output" / "financials.csv"
        assert ohlcv_path.exists()
        assert financials_path.exists()

    def test_load_empty_data(self, tmp_path: Path):
        config = LoadingConfig(destination="csv", path=str(tmp_path / "output"))
        loader = CSVLoader(config)

        data = ExtractedData(ohlcv=None, financials=None)
        loader.load(data)

        # No files should be created
        output_dir = tmp_path / "output"
        assert not output_dir.exists() or not any(output_dir.iterdir())

    def test_load_creates_directory(
        self,
        tmp_path: Path,
        sample_ohlcv_df: pd.DataFrame,
    ):
        nested_path = tmp_path / "deep" / "nested" / "output"
        config = LoadingConfig(destination="csv", path=str(nested_path))
        loader = CSVLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)
        loader.load(data)

        assert nested_path.exists()
        assert (nested_path / "ohlcv.csv").exists()


class TestLoaderRegistry:
    """Tests for loader registry."""

    def test_get_csv_loader(self):
        loader_class = get_loader("csv")
        assert loader_class == CSVLoader

    def test_get_unknown_loader(self):
        with pytest.raises(ConfigurationError, match="Unsupported destination type"):
            get_loader("unknown")

    def test_register_custom_loader(self):
        class CustomLoader(BaseLoader):
            def load(self, data: ExtractedData) -> None:
                pass

        register_loader("custom", CustomLoader)
        assert get_loader("custom") == CustomLoader
