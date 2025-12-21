"""PostgreSQL data loader."""

import logging
from urllib.parse import quote_plus

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from finetl.base import BaseLoader
from finetl.config.schema import LoadingConfig
from finetl.exceptions import LoadingError
from finetl.models import ExtractedData

logger = logging.getLogger(__name__)


class PostgreSQLLoader(BaseLoader):
    """Loader for writing data to PostgreSQL database."""

    def __init__(self, config: LoadingConfig) -> None:
        super().__init__(config)
        self.host = config.host
        self.port = config.port
        self.database = config.database
        self.schema_name = config.schema_name
        self.user = config.user
        self.password = config.password
        self.if_exists = config.if_exists.value

    def _create_engine(self) -> Engine:
        """Create SQLAlchemy engine for PostgreSQL connection."""
        # URL-encode password to handle special characters
        encoded_password = quote_plus(self.password)
        connection_string = (
            f"postgresql://{self.user}:{encoded_password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(connection_string)

    def load(self, data: ExtractedData) -> None:
        """Write extracted data to PostgreSQL tables."""
        if not data:
            logger.warning("No data to load")
            return

        has_ohlcv = data.ohlcv is not None and not data.ohlcv.empty
        has_financials = data.financials is not None and not data.financials.empty

        if not has_ohlcv and not has_financials:
            logger.warning("No data to load")
            return

        try:
            engine = self._create_engine()
        except Exception as e:
            raise LoadingError(f"Failed to create database connection: {e}") from e

        # Write OHLCV data
        if has_ohlcv:
            logger.info(
                "Writing OHLCV data to PostgreSQL table %s.ohlcv",
                self.schema_name,
            )
            try:
                data.ohlcv.to_sql(
                    name="ohlcv",
                    con=engine,
                    schema=self.schema_name,
                    if_exists=self.if_exists,
                    index=False,
                )
            except Exception as e:
                raise LoadingError(f"Failed to write OHLCV data: {e}") from e

        # Write financials data
        if has_financials:
            logger.info(
                "Writing financials data to PostgreSQL table %s.financials",
                self.schema_name,
            )
            try:
                data.financials.to_sql(
                    name="financials",
                    con=engine,
                    schema=self.schema_name,
                    if_exists=self.if_exists,
                    index=False,
                )
            except Exception as e:
                raise LoadingError(f"Failed to write financials data: {e}") from e

        engine.dispose()
        logger.info("Successfully wrote data to PostgreSQL")
