"""YAML configuration loading and validation."""

from pathlib import Path
from typing import Any

import yaml
from pydantic import ValidationError

from finetl.config.schema import FinETLConfig
from finetl.exceptions import ConfigurationError


def load_yaml(path: str | Path) -> dict[str, Any]:
    """Load a YAML file and return its contents as a dictionary."""
    path = Path(path)
    if not path.exists():
        raise ConfigurationError(f"Config file not found: {path}")
    if not path.is_file():
        raise ConfigurationError(f"Config path is not a file: {path}")

    try:
        with open(path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as e:
        raise ConfigurationError(f"Invalid YAML syntax: {e}") from e

    if not isinstance(data, dict):
        raise ConfigurationError("Config file must contain a YAML mapping")

    return data


def load_config(path: str | Path) -> FinETLConfig:
    """Load and validate a FinETL configuration from a YAML file."""
    data = load_yaml(path)
    return parse_config(data)


def parse_config(data: dict[str, Any]) -> FinETLConfig:
    """Parse and validate a configuration dictionary."""
    try:
        return FinETLConfig.model_validate(data)
    except ValidationError as e:
        raise ConfigurationError(f"Invalid configuration: {e}") from e
