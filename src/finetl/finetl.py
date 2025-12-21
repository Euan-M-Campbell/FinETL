"""Main FinETL class for orchestrating ETL pipelines."""

import logging
from pathlib import Path
from typing import Any

from finetl.config import FinETLConfig, load_config, parse_config
from finetl.extraction import YFinanceExtractor
from finetl.loading import get_loader

logger = logging.getLogger(__name__)


class FinETL:
    """Main class for running FinETL pipelines.

    A FinETL object is instantiated from a YAML configuration file or
    a configuration dictionary. It orchestrates the extraction and loading
    of financial data.

    Example:
        >>> etl = FinETL.from_yaml("config.yaml")
        >>> etl.run()
    """

    def __init__(self, config: FinETLConfig) -> None:
        """Initialize a FinETL pipeline.

        Args:
            config: A validated FinETLConfig object
        """
        self.config = config
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Set up logging for the pipeline."""
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    @classmethod
    def from_yaml(cls, path: str | Path) -> "FinETL":
        """Create a FinETL instance from a YAML configuration file.

        Args:
            path: Path to the YAML configuration file

        Returns:
            A configured FinETL instance

        Raises:
            ConfigurationError: If the configuration is invalid
        """
        config = load_config(path)
        return cls(config)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "FinETL":
        """Create a FinETL instance from a configuration dictionary.

        Args:
            data: Configuration dictionary

        Returns:
            A configured FinETL instance

        Raises:
            ConfigurationError: If the configuration is invalid
        """
        config = parse_config(data)
        return cls(config)

    def run(self) -> None:
        """Run the ETL pipeline.

        This method executes the full ETL pipeline:
        1. Extract data from the configured source
        2. Load data to the configured destination
        """
        logger.info("Starting pipeline: %s", self.config.name)

        # Extract
        logger.info("Extracting data from %s", self.config.extraction.source)
        extractor = self._get_extractor()
        data = extractor.extract()

        if not data:
            logger.warning("No data extracted, skipping load step")
            return

        # Load
        logger.info("Loading data to %s", self.config.loading.destination)
        loader_class = get_loader(self.config.loading.destination)
        loader = loader_class(self.config.loading)
        loader.load(data)

        logger.info("Pipeline completed: %s", self.config.name)

    def _get_extractor(self) -> YFinanceExtractor:
        """Get the appropriate extractor based on configuration."""
        source = self.config.extraction.source
        if source == "yfinance":
            return YFinanceExtractor(self.config.extraction)
        else:
            # This shouldn't happen due to Pydantic validation
            raise ValueError(f"Unsupported source: {source}")
