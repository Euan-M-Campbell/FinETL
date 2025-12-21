"""Loader registry for mapping destination types to loader classes."""

from finetl.base import BaseLoader
from finetl.exceptions import ConfigurationError
from finetl.loading.csv import CSVLoader
from finetl.loading.huggingface import HuggingFaceLoader
from finetl.loading.parquet import ParquetLoader
from finetl.loading.postgresql import PostgreSQLLoader

# Registry mapping destination type strings to loader classes
_LOADER_REGISTRY: dict[str, type[BaseLoader]] = {
    "csv": CSVLoader,
    "parquet": ParquetLoader,
    "huggingface": HuggingFaceLoader,
    "postgresql": PostgreSQLLoader,
}


def get_loader(destination: str) -> type[BaseLoader]:
    """Get a loader class by destination type.

    Args:
        destination: The destination type string (e.g., "csv")

    Returns:
        The loader class for the given destination type

    Raises:
        ConfigurationError: If the destination type is not supported
    """
    loader_class = _LOADER_REGISTRY.get(destination)
    if loader_class is None:
        supported = ", ".join(_LOADER_REGISTRY.keys())
        raise ConfigurationError(
            f"Unsupported destination type: {destination}. Supported: {supported}"
        )
    return loader_class


def register_loader(destination: str, loader_class: type[BaseLoader]) -> None:
    """Register a new loader class for a destination type.

    Args:
        destination: The destination type string
        loader_class: The loader class to register
    """
    _LOADER_REGISTRY[destination] = loader_class
