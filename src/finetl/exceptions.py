"""Custom exceptions for FinETL."""


class FinETLError(Exception):
    """Base exception for FinETL."""

    pass


class ConfigurationError(FinETLError):
    """Raised when configuration is invalid."""

    pass


class ExtractionError(FinETLError):
    """Raised when data extraction fails."""

    pass


class LoadingError(FinETLError):
    """Raised when data loading fails."""

    pass
