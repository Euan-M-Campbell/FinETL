"""Tests for data loading."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from finetl.base import BaseLoader
from finetl.config.schema import LoadingConfig
from finetl.exceptions import ConfigurationError, LoadingError
from finetl.loading import (
    CSVLoader,
    HuggingFaceLoader,
    ParquetLoader,
    PostgreSQLLoader,
    get_loader,
    register_loader,
)
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


class TestParquetLoader:
    """Tests for ParquetLoader."""

    def test_load_ohlcv_only(
        self,
        tmp_path: Path,
        sample_ohlcv_df: pd.DataFrame,
    ):
        config = LoadingConfig(destination="parquet", path=str(tmp_path / "output"))
        loader = ParquetLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)
        loader.load(data)

        ohlcv_path = tmp_path / "output" / "ohlcv.parquet"
        assert ohlcv_path.exists()

        loaded = pd.read_parquet(ohlcv_path)
        assert len(loaded) == len(sample_ohlcv_df)
        assert list(loaded.columns) == list(sample_ohlcv_df.columns)

    def test_load_financials_only(
        self,
        tmp_path: Path,
        sample_financials_df: pd.DataFrame,
    ):
        config = LoadingConfig(destination="parquet", path=str(tmp_path / "output"))
        loader = ParquetLoader(config)

        data = ExtractedData(ohlcv=None, financials=sample_financials_df)
        loader.load(data)

        financials_path = tmp_path / "output" / "financials.parquet"
        assert financials_path.exists()

        loaded = pd.read_parquet(financials_path)
        assert len(loaded) == len(sample_financials_df)

    def test_load_both(
        self,
        tmp_path: Path,
        sample_extracted_data: ExtractedData,
    ):
        config = LoadingConfig(destination="parquet", path=str(tmp_path / "output"))
        loader = ParquetLoader(config)

        loader.load(sample_extracted_data)

        ohlcv_path = tmp_path / "output" / "ohlcv.parquet"
        financials_path = tmp_path / "output" / "financials.parquet"
        assert ohlcv_path.exists()
        assert financials_path.exists()

    def test_load_empty_data(self, tmp_path: Path):
        config = LoadingConfig(destination="parquet", path=str(tmp_path / "output"))
        loader = ParquetLoader(config)

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
        config = LoadingConfig(destination="parquet", path=str(nested_path))
        loader = ParquetLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)
        loader.load(data)

        assert nested_path.exists()
        assert (nested_path / "ohlcv.parquet").exists()

    def test_parquet_data_integrity(
        self,
        tmp_path: Path,
        sample_ohlcv_df: pd.DataFrame,
    ):
        """Verify data matches after round-trip through Parquet."""
        config = LoadingConfig(destination="parquet", path=str(tmp_path / "output"))
        loader = ParquetLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)
        loader.load(data)

        loaded = pd.read_parquet(tmp_path / "output" / "ohlcv.parquet")
        pd.testing.assert_frame_equal(loaded, sample_ohlcv_df)


class TestHuggingFaceLoader:
    """Tests for HuggingFaceLoader."""

    def test_load_ohlcv_only(
        self,
        sample_ohlcv_df: pd.DataFrame,
    ):
        config = LoadingConfig(
            destination="huggingface",
            repo_id="test-user/test-repo",
        )
        loader = HuggingFaceLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)

        with patch("finetl.loading.huggingface.DatasetDict") as mock_dataset_dict:
            mock_instance = MagicMock()
            mock_dataset_dict.return_value = mock_instance

            loader.load(data)

            # Verify DatasetDict was created with ohlcv key
            call_args = mock_dataset_dict.call_args[0][0]
            assert "ohlcv" in call_args
            assert "financials" not in call_args

            # Verify push_to_hub was called correctly
            mock_instance.push_to_hub.assert_called_once_with(
                "test-user/test-repo", private=False
            )

    def test_load_financials_only(
        self,
        sample_financials_df: pd.DataFrame,
    ):
        config = LoadingConfig(
            destination="huggingface",
            repo_id="test-user/test-repo",
        )
        loader = HuggingFaceLoader(config)

        data = ExtractedData(ohlcv=None, financials=sample_financials_df)

        with patch("finetl.loading.huggingface.DatasetDict") as mock_dataset_dict:
            mock_instance = MagicMock()
            mock_dataset_dict.return_value = mock_instance

            loader.load(data)

            # Verify DatasetDict was created with financials key
            call_args = mock_dataset_dict.call_args[0][0]
            assert "financials" in call_args
            assert "ohlcv" not in call_args

            mock_instance.push_to_hub.assert_called_once()

    def test_load_both(
        self,
        sample_extracted_data: ExtractedData,
    ):
        config = LoadingConfig(
            destination="huggingface",
            repo_id="test-user/test-repo",
        )
        loader = HuggingFaceLoader(config)

        with patch("finetl.loading.huggingface.DatasetDict") as mock_dataset_dict:
            mock_instance = MagicMock()
            mock_dataset_dict.return_value = mock_instance

            loader.load(sample_extracted_data)

            # Verify DatasetDict was created with both keys
            call_args = mock_dataset_dict.call_args[0][0]
            assert "ohlcv" in call_args
            assert "financials" in call_args

            mock_instance.push_to_hub.assert_called_once()

    def test_load_empty_data(self):
        config = LoadingConfig(
            destination="huggingface",
            repo_id="test-user/test-repo",
        )
        loader = HuggingFaceLoader(config)

        data = ExtractedData(ohlcv=None, financials=None)

        with patch("finetl.loading.huggingface.DatasetDict") as mock_dataset_dict:
            loader.load(data)

            # push_to_hub should not be called for empty data
            mock_dataset_dict.return_value.push_to_hub.assert_not_called()

    def test_private_repo(
        self,
        sample_ohlcv_df: pd.DataFrame,
    ):
        config = LoadingConfig(
            destination="huggingface",
            repo_id="test-user/private-repo",
            private=True,
        )
        loader = HuggingFaceLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)

        with patch("finetl.loading.huggingface.DatasetDict") as mock_dataset_dict:
            mock_instance = MagicMock()
            mock_dataset_dict.return_value = mock_instance

            loader.load(data)

            # Verify private flag is passed
            mock_instance.push_to_hub.assert_called_once_with(
                "test-user/private-repo", private=True
            )

    def test_push_to_hub_failure(
        self,
        sample_ohlcv_df: pd.DataFrame,
    ):
        config = LoadingConfig(
            destination="huggingface",
            repo_id="test-user/test-repo",
        )
        loader = HuggingFaceLoader(config)

        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)

        with patch("finetl.loading.huggingface.DatasetDict") as mock_dataset_dict:
            mock_instance = MagicMock()
            mock_instance.push_to_hub.side_effect = Exception("Upload failed")
            mock_dataset_dict.return_value = mock_instance

            with pytest.raises(LoadingError, match="Failed to upload to HuggingFace"):
                loader.load(data)

    def test_config_requires_repo_id(self):
        with pytest.raises(ValueError, match="repo_id is required"):
            LoadingConfig(destination="huggingface")


class TestPostgreSQLLoader:
    """Tests for PostgreSQLLoader."""

    @pytest.fixture
    def pg_config(self) -> LoadingConfig:
        """Sample PostgreSQL configuration."""
        return LoadingConfig(
            destination="postgresql",
            host="localhost",
            port=5432,
            database="testdb",
            schema_name="public",
            user="testuser",
            password="testpass",
        )

    def test_load_ohlcv_only(
        self,
        pg_config: LoadingConfig,
        sample_ohlcv_df: pd.DataFrame,
    ):
        loader = PostgreSQLLoader(pg_config)
        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)

        with patch.object(loader, "_create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            with patch.object(sample_ohlcv_df, "to_sql") as mock_to_sql:
                loader.load(data)

                mock_to_sql.assert_called_once_with(
                    name="ohlcv",
                    con=mock_engine,
                    schema="public",
                    if_exists="append",
                    index=False,
                )
                mock_engine.dispose.assert_called_once()

    def test_load_financials_only(
        self,
        pg_config: LoadingConfig,
        sample_financials_df: pd.DataFrame,
    ):
        loader = PostgreSQLLoader(pg_config)
        data = ExtractedData(ohlcv=None, financials=sample_financials_df)

        with patch.object(loader, "_create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            with patch.object(sample_financials_df, "to_sql") as mock_to_sql:
                loader.load(data)

                mock_to_sql.assert_called_once_with(
                    name="financials",
                    con=mock_engine,
                    schema="public",
                    if_exists="append",
                    index=False,
                )

    def test_load_both(
        self,
        pg_config: LoadingConfig,
        sample_extracted_data: ExtractedData,
    ):
        loader = PostgreSQLLoader(pg_config)

        with patch.object(loader, "_create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            with (
                patch.object(sample_extracted_data.ohlcv, "to_sql") as mock_ohlcv_sql,
                patch.object(
                    sample_extracted_data.financials, "to_sql"
                ) as mock_fin_sql,
            ):
                loader.load(sample_extracted_data)

                mock_ohlcv_sql.assert_called_once()
                mock_fin_sql.assert_called_once()

    def test_load_empty_data(self, pg_config: LoadingConfig):
        loader = PostgreSQLLoader(pg_config)
        data = ExtractedData(ohlcv=None, financials=None)

        with patch.object(loader, "_create_engine") as mock_create_engine:
            loader.load(data)
            mock_create_engine.assert_not_called()

    def test_if_exists_replace(
        self,
        sample_ohlcv_df: pd.DataFrame,
    ):
        config = LoadingConfig(
            destination="postgresql",
            host="localhost",
            database="testdb",
            user="testuser",
            password="testpass",
            if_exists="replace",
        )
        loader = PostgreSQLLoader(config)
        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)

        with patch.object(loader, "_create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            with patch.object(sample_ohlcv_df, "to_sql") as mock_to_sql:
                loader.load(data)

                mock_to_sql.assert_called_once_with(
                    name="ohlcv",
                    con=mock_engine,
                    schema="public",
                    if_exists="replace",
                    index=False,
                )

    def test_custom_schema(
        self,
        sample_ohlcv_df: pd.DataFrame,
    ):
        config = LoadingConfig(
            destination="postgresql",
            host="localhost",
            database="testdb",
            schema_name="finance",
            user="testuser",
            password="testpass",
        )
        loader = PostgreSQLLoader(config)
        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)

        with patch.object(loader, "_create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            with patch.object(sample_ohlcv_df, "to_sql") as mock_to_sql:
                loader.load(data)

                mock_to_sql.assert_called_once_with(
                    name="ohlcv",
                    con=mock_engine,
                    schema="finance",
                    if_exists="append",
                    index=False,
                )

    def test_write_failure(
        self,
        pg_config: LoadingConfig,
        sample_ohlcv_df: pd.DataFrame,
    ):
        loader = PostgreSQLLoader(pg_config)
        data = ExtractedData(ohlcv=sample_ohlcv_df, financials=None)

        with patch.object(loader, "_create_engine") as mock_create_engine:
            mock_engine = MagicMock()
            mock_create_engine.return_value = mock_engine

            with patch.object(sample_ohlcv_df, "to_sql") as mock_to_sql:
                mock_to_sql.side_effect = Exception("Connection failed")

                with pytest.raises(LoadingError, match="Failed to write OHLCV data"):
                    loader.load(data)

    def test_config_requires_host(self):
        with pytest.raises(ValueError, match="host"):
            LoadingConfig(
                destination="postgresql",
                database="testdb",
                user="testuser",
                password="testpass",
            )

    def test_config_requires_database(self):
        with pytest.raises(ValueError, match="database"):
            LoadingConfig(
                destination="postgresql",
                host="localhost",
                user="testuser",
                password="testpass",
            )

    def test_config_requires_user(self):
        with pytest.raises(ValueError, match="user"):
            LoadingConfig(
                destination="postgresql",
                host="localhost",
                database="testdb",
                password="testpass",
            )

    def test_config_requires_password(self):
        with pytest.raises(ValueError, match="password"):
            LoadingConfig(
                destination="postgresql",
                host="localhost",
                database="testdb",
                user="testuser",
            )


class TestLoaderRegistry:
    """Tests for loader registry."""

    def test_get_csv_loader(self):
        loader_class = get_loader("csv")
        assert loader_class == CSVLoader

    def test_get_parquet_loader(self):
        loader_class = get_loader("parquet")
        assert loader_class == ParquetLoader

    def test_get_huggingface_loader(self):
        loader_class = get_loader("huggingface")
        assert loader_class == HuggingFaceLoader

    def test_get_postgresql_loader(self):
        loader_class = get_loader("postgresql")
        assert loader_class == PostgreSQLLoader

    def test_get_unknown_loader(self):
        with pytest.raises(ConfigurationError, match="Unsupported destination type"):
            get_loader("unknown")

    def test_register_custom_loader(self):
        class CustomLoader(BaseLoader):
            def load(self, data: ExtractedData) -> None:
                pass

        register_loader("custom", CustomLoader)
        assert get_loader("custom") == CustomLoader
