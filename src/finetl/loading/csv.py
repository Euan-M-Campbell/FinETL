"""CSV data loader."""

import logging
from pathlib import Path

from finetl.base import BaseLoader
from finetl.config.schema import LoadingConfig
from finetl.exceptions import LoadingError
from finetl.models import ExtractedData

logger = logging.getLogger(__name__)


class CSVLoader(BaseLoader):
    """Loader for writing data to CSV files."""

    def __init__(self, config: LoadingConfig) -> None:
        super().__init__(config)
        self.output_path = Path(config.path)

    def load(self, data: ExtractedData) -> None:
        """Write extracted data to CSV files."""
        if not data:
            logger.warning("No data to load")
            return

        # Create output directory if it doesn't exist
        try:
            self.output_path.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            raise LoadingError(f"Failed to create output directory: {e}") from e

        # Write OHLCV data
        if data.ohlcv is not None and not data.ohlcv.empty:
            ohlcv_path = self.output_path / "ohlcv.csv"
            logger.info("Writing OHLCV data to %s", ohlcv_path)
            try:
                data.ohlcv.to_csv(ohlcv_path, index=False)
            except Exception as e:
                raise LoadingError(f"Failed to write OHLCV data: {e}") from e

        # Write financials data
        if data.financials is not None and not data.financials.empty:
            financials_path = self.output_path / "financials.csv"
            logger.info("Writing financials data to %s", financials_path)
            try:
                data.financials.to_csv(financials_path, index=False)
            except Exception as e:
                raise LoadingError(f"Failed to write financials data: {e}") from e
