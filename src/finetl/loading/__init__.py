"""Loading module for FinETL."""

from finetl.loading.csv import CSVLoader
from finetl.loading.huggingface import HuggingFaceLoader
from finetl.loading.parquet import ParquetLoader
from finetl.loading.postgresql import PostgreSQLLoader
from finetl.loading.registry import get_loader, register_loader

__all__ = [
    "CSVLoader",
    "HuggingFaceLoader",
    "ParquetLoader",
    "PostgreSQLLoader",
    "get_loader",
    "register_loader",
]
