# FinETL

A config-driven Python library for extracting financial data from Yahoo Finance and loading it to various destinations.

## Features

- **YAML Configuration** - Define your ETL pipeline in a simple config file
- **OHLCV Data** - Extract Open, High, Low, Close, Volume price data
- **Financial Statements** - Extract Balance Sheet, Income Statement, and Cash Flow data
- **Multiple Tickers** - Fetch data for multiple stocks in a single run
- **Extensible** - Easy to add new data sources and destinations

## Installation

Requires Python 3.12+

```bash
# Clone the repository
git clone https://github.com/yourusername/finetl.git
cd finetl

# Install with Poetry
poetry install
```

## Quick Start

1. Create a config file:

```yaml
# my_config.yaml
name: "my-pipeline"

extraction:
  source: yfinance
  tickers:
    - AAPL
    - MSFT
    - GOOGL

  data_types:
    ohlcv:
      enabled: true
      start_date: "2024-01-01"
      end_date: "2024-12-01"
      interval: "1d"

    financials:
      enabled: true
      frequency: "quarterly"
      statements:
        - balance_sheet
        - income_statement
        - cash_flow

loading:
  destination: csv
  path: "./output"
```

2. Run the pipeline:

```python
from finetl import FinETL

etl = FinETL.from_yaml("my_config.yaml")
etl.run()
```

Or use the example script:

```bash
poetry run python scripts/run_example.py
```

## Configuration Reference

### Root

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Pipeline name for logging |
| `extraction` | object | Yes | Extraction configuration |
| `loading` | object | Yes | Loading configuration |

### Extraction

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source` | string | Yes | Data source (currently only `yfinance`) |
| `tickers` | list[string] | Yes | List of stock ticker symbols |
| `data_types` | object | Yes | Data types to extract |

### Data Types

At least one of `ohlcv` or `financials` must be enabled.

#### OHLCV

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | `true` | Enable OHLCV extraction |
| `start_date` | string | Yes | - | Start date (YYYY-MM-DD) |
| `end_date` | string | Yes | - | End date (YYYY-MM-DD) |
| `interval` | string | No | `1d` | Data interval |

Valid intervals: `1m`, `2m`, `5m`, `15m`, `30m`, `60m`, `90m`, `1h`, `1d`, `5d`, `1wk`, `1mo`, `3mo`

#### Financials

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `enabled` | boolean | No | `true` | Enable financials extraction |
| `frequency` | string | No | `annual` | `annual` or `quarterly` |
| `statements` | list[string] | No | all | Statements to extract |

Valid statements: `balance_sheet`, `income_statement`, `cash_flow`

### Loading

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `destination` | string | Yes | - | Destination type (currently only `csv`) |
| `path` | string | No | `./output` | Output directory path |

## Output Format

### OHLCV (ohlcv.csv)

All tickers combined into a single file with a `ticker` column:

| ticker | date | open | high | low | close | volume |
|--------|------|------|------|-----|-------|--------|
| AAPL | 2024-01-02 | 185.50 | 186.20 | 184.80 | 185.90 | 45000000 |
| AAPL | 2024-01-03 | 185.90 | 187.10 | 185.50 | 186.80 | 42000000 |
| MSFT | 2024-01-02 | 372.00 | 375.50 | 371.20 | 374.30 | 22000000 |

### Financials (financials.csv)

All tickers and statement types merged into a single file. One row per ticker per period, with all metrics as columns:

| ticker | period | Total Assets | Total Liabilities | Total Revenue | Operating Cash Flow | ... |
|--------|--------|--------------|-------------------|---------------|---------------------|-----|
| AAPL | 2024-09-30 | 352583000000 | 308030000000 | 394328000000 | 118254000000 | ... |
| AAPL | 2024-06-30 | 348000000000 | 305000000000 | 385000000000 | 115000000000 | ... |
| MSFT | 2024-06-30 | 512163000000 | 243686000000 | 245122000000 | 118548000000 | ... |

## Programmatic Usage

You can also create pipelines programmatically:

```python
from finetl import FinETL

config = {
    "name": "my-pipeline",
    "extraction": {
        "source": "yfinance",
        "tickers": ["AAPL", "MSFT"],
        "data_types": {
            "ohlcv": {
                "enabled": True,
                "start_date": "2024-01-01",
                "end_date": "2024-12-01",
                "interval": "1d",
            },
        },
    },
    "loading": {
        "destination": "csv",
        "path": "./output",
    },
}

etl = FinETL.from_dict(config)
etl.run()
```

## Development

### Running Tests

```bash
poetry run pytest
```

With coverage:

```bash
poetry run pytest --cov=finetl
```

### Project Structure

```
src/finetl/
├── __init__.py          # Package exports
├── finetl.py            # Main FinETL class
├── base.py              # Abstract base classes
├── exceptions.py        # Custom exceptions
├── models.py            # Data models
├── config/
│   ├── schema.py        # Pydantic config models
│   └── loader.py        # YAML loading/validation
├── extraction/
│   └── yfinance.py      # Yahoo Finance extractor
└── loading/
    ├── csv.py           # CSV loader
    └── registry.py      # Loader registry
```

## Dependencies

- **yfinance** - Yahoo Finance data API
- **pydantic** - Configuration validation
- **pyyaml** - YAML parsing
- **pandas** - Data manipulation

## License

MIT
