"""Abstract base classes for FinETL components."""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from finetl.config.schema import ExtractionConfig, LoadingConfig
    from finetl.models import ExtractedData


class BaseExtractor(ABC):
    """Abstract base class for data extractors."""

    @abstractmethod
    def extract(self) -> "ExtractedData":
        """Extract data and return an ExtractedData object."""
        pass


class BaseLoader(ABC):
    """Abstract base class for data loaders."""

    def __init__(self, config: "LoadingConfig") -> None:
        self.config = config

    @abstractmethod
    def load(self, data: "ExtractedData") -> None:
        """Load extracted data to the destination."""
        pass
