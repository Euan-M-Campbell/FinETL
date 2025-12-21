"""HuggingFace Hub data loader."""

import logging

from datasets import Dataset, DatasetDict

from finetl.base import BaseLoader
from finetl.config.schema import LoadingConfig
from finetl.exceptions import LoadingError
from finetl.models import ExtractedData

logger = logging.getLogger(__name__)


class HuggingFaceLoader(BaseLoader):
    """Loader for uploading data to HuggingFace Hub as datasets."""

    def __init__(self, config: LoadingConfig) -> None:
        super().__init__(config)
        self.repo_id = config.repo_id
        self.private = config.private

    def load(self, data: ExtractedData) -> None:
        """Upload extracted data to HuggingFace Hub."""
        if not data:
            logger.warning("No data to load")
            return

        datasets: dict[str, Dataset] = {}

        # Convert OHLCV DataFrame to Dataset
        if data.ohlcv is not None and not data.ohlcv.empty:
            logger.info("Converting OHLCV data to HuggingFace Dataset")
            datasets["ohlcv"] = Dataset.from_pandas(data.ohlcv)

        # Convert financials DataFrame to Dataset
        if data.financials is not None and not data.financials.empty:
            logger.info("Converting financials data to HuggingFace Dataset")
            datasets["financials"] = Dataset.from_pandas(data.financials)

        if not datasets:
            logger.warning("No datasets to upload")
            return

        # Create DatasetDict and push to Hub
        dataset_dict = DatasetDict(datasets)
        logger.info("Pushing dataset to HuggingFace Hub: %s", self.repo_id)

        try:
            dataset_dict.push_to_hub(self.repo_id, private=self.private)
            logger.info("Successfully uploaded dataset to %s", self.repo_id)
        except Exception as e:
            raise LoadingError(f"Failed to upload to HuggingFace Hub: {e}") from e
