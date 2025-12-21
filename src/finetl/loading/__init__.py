"""Loading module for FinETL."""

from finetl.loading.csv import CSVLoader
from finetl.loading.parquet import ParquetLoader
from finetl.loading.registry import get_loader, register_loader

__all__ = ["CSVLoader", "ParquetLoader", "get_loader", "register_loader"]
