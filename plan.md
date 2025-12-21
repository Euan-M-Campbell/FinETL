# FinETL Implementation Plan

## Overview

Build a config-driven financial data ETL library. A `FinETL` object is instantiated from a YAML configuration file that defines data sources (tickers from yfinance) and destinations (CSV files initially, extensible to other formats).

---

## Data Types Supported

### Price Data
- **OHLCV** - Open, High, Low, Close, Volume (time-series)

### Financial Statements (merged into single table)
- **Balance Sheet** - Assets, liabilities, equity (`ticker.balance_sheet`)
- **Income Statement** - Revenue, expenses, profit (`ticker.financials`)
- **Cash Flow Statement** - Operating, investing, financing activities (`ticker.cashflow`)

Each financial statement supports:
- Annual frequency (default)
- Quarterly frequency

---

## Output Structure

All tickers are combined into single DataFrames:

```
output/
├── ohlcv.csv        # All tickers, with 'ticker' column
└── financials.csv   # All tickers, one row per ticker/period, metrics as columns
```

### OHLCV DataFrame Schema

| ticker | date       | open   | high   | low    | close  | volume   |
|--------|------------|--------|--------|--------|--------|----------|
| AAPL   | 2024-01-02 | 185.50 | 186.20 | 184.80 | 185.90 | 45000000 |
| AAPL   | 2024-01-03 | 185.90 | 187.10 | 185.50 | 186.80 | 42000000 |
| MSFT   | 2024-01-02 | 372.00 | 375.50 | 371.20 | 374.30 | 22000000 |
| ...    | ...        | ...    | ...    | ...    | ...    | ...      |

### Financials DataFrame Schema

One row per ticker per period. All metrics from all statement types become columns:

| ticker | period     | Total Assets  | Total Liabilities | Total Revenue | Operating Cash Flow | ... |
|--------|------------|---------------|-------------------|---------------|---------------------|-----|
| AAPL   | 2024-09-30 | 352583000000  | 308030000000      | 394328000000  | 118254000000        | ... |
| AAPL   | 2024-06-30 | 348000000000  | 305000000000      | 385000000000  | 115000000000        | ... |
| MSFT   | 2024-06-30 | 512163000000  | 243686000000      | 245122000000  | 118548000000        | ... |
| ...    | ...        | ...           | ...               | ...           | ...                 | ... |

---

## YAML Configuration Schema

```yaml
# config.yaml
name: "my-stock-pipeline"

extraction:
  source: yfinance
  tickers:
    - AAPL
    - GOOGL
    - MSFT

  # Data types to extract (at least one required)
  data_types:
    ohlcv:
      enabled: true
      start_date: "2020-01-01"
      end_date: "2024-01-01"
      interval: "1d"  # 1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo

    financials:
      enabled: true
      frequency: "annual"  # annual | quarterly
      statements:
        - balance_sheet
        - income_statement
        - cash_flow

loading:
  destination: csv
  path: "./data/output"
```

### Minimal Config (OHLCV only)

```yaml
name: "simple-ohlcv"

extraction:
  source: yfinance
  tickers:
    - AAPL
  data_types:
    ohlcv:
      enabled: true
      start_date: "2024-01-01"
      end_date: "2024-12-01"

loading:
  destination: csv
  path: "./output"
```

### Financials Only Config

```yaml
name: "quarterly-financials"

extraction:
  source: yfinance
  tickers:
    - AAPL
    - MSFT
  data_types:
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

---

## Phase 1: Core Infrastructure

### 1.1 Config Module (`src/finetl/config/`)

- `schema.py` - Pydantic models for config validation
  - `OHLCVConfig` - start_date, end_date, interval, enabled
  - `FinancialsConfig` - frequency, statements list, enabled
  - `DataTypesConfig` - ohlcv, financials (both optional, at least one required)
  - `ExtractionConfig` - source, tickers, data_types
  - `LoadingConfig` - destination type, path, options
  - `FinETLConfig` - root config combining all sections
- `loader.py` - YAML parsing and validation
  - Load YAML file to dict
  - Validate against Pydantic schema
  - Return typed config object

### 1.2 Base Classes (`src/finetl/base.py`)

- `BaseExtractor` - Abstract extractor interface
- `BaseLoader` - Abstract loader interface

### 1.3 Exceptions (`src/finetl/exceptions.py`)

- `ConfigurationError` - Invalid config file
- `ExtractionError` - Data fetch failures
- `LoadingError` - Write failures

---

## Phase 2: Extraction (`src/finetl/extraction/`)

### 2.1 Yahoo Finance Extractor

- `yfinance.py`
  - `YFinanceExtractor` class
  - Accepts `ExtractionConfig`
  - Methods:
    - `extract_ohlcv() -> DataFrame` - All tickers combined with 'ticker' column
    - `extract_financials() -> DataFrame` - All tickers merged, one row per ticker/period, metrics as columns
    - `extract_all() -> ExtractedData`
  - Returns structured `ExtractedData` object
  - Handles rate limiting and retries

### 2.2 Extracted Data Model

```python
@dataclass
class ExtractedData:
    ohlcv: DataFrame | None        # Combined OHLCV for all tickers
    financials: DataFrame | None   # Combined financials (all statements, all tickers)
```

---

## Phase 3: Loading (`src/finetl/loading/`)

### 3.1 CSV Loader

- `csv.py`
  - `CSVLoader` class
  - Accepts `LoadingConfig` and `ExtractedData`
  - Writes:
    - `ohlcv.csv` - if OHLCV data present
    - `financials.csv` - if financials data present

### 3.2 Loader Registry

- `registry.py`
  - Maps destination type strings to loader classes
  - `get_loader(destination: str) -> Type[BaseLoader]`
  - Extensible for future formats (parquet, database, etc.)

---

## Phase 4: Main FinETL Class (`src/finetl/finetl.py`)

### 4.1 FinETL Class

```python
from finetl import FinETL

# Instantiate from YAML config
etl = FinETL.from_yaml("config.yaml")

# Or from dict
etl = FinETL.from_dict({...})

# Run the pipeline
etl.run()
```

### 4.2 Implementation

- `from_yaml(path: str) -> FinETL` - Class method to load from file
- `from_dict(config: dict) -> FinETL` - Class method for programmatic use
- `run()` - Execute extraction and loading
- Progress logging during execution

---

## Phase 5: Package Structure

```
src/finetl/
├── __init__.py          # Exports FinETL
├── finetl.py            # Main FinETL class
├── base.py              # Abstract base classes
├── exceptions.py        # Custom exceptions
├── models.py            # ExtractedData dataclass
├── config/
│   ├── __init__.py
│   ├── schema.py        # Pydantic config models
│   └── loader.py        # YAML loading/validation
├── extraction/
│   ├── __init__.py
│   └── yfinance.py      # YFinance extractor
└── loading/
    ├── __init__.py
    ├── csv.py           # CSV loader
    └── registry.py      # Loader registry
```

---

## Phase 6: Testing (`tests/`)

### 6.1 Unit Tests

- `test_config.py` - Config parsing and validation
- `test_extraction.py` - Mock yfinance responses, verify DataFrame structure
- `test_loading.py` - CSV output verification
- `test_finetl.py` - End-to-end integration

### 6.2 Test Fixtures

- Sample YAML configs (valid and invalid)
- Mock DataFrames for OHLCV and financials

---

## Phase 7: Future Extensions

### Additional Loaders (not in initial scope)

- Parquet loader
- SQLite/PostgreSQL loader
- HuggingFace Hub loader

### Additional Extractors (not in initial scope)

- Alpha Vantage
- Polygon.io
- CSV file source

### Additional Data Types (not in initial scope)

- Dividends and splits
- Company info/metadata
- Analyst recommendations
- Institutional holders

### Transformation Layer (not in initial scope)

- Technical indicators
- Data cleaning
- Feature engineering

---

## Implementation Order

1. **Config** - YAML schema and loading (including data_types)
2. **Base classes** - Extractor/Loader interfaces
3. **Models** - ExtractedData dataclass
4. **Extraction** - YFinance extractor (OHLCV + financials with merging logic)
5. **Loading** - CSV loader
6. **FinETL** - Main class tying it together
7. **Tests** - Add with each phase

---

## Dependencies to Add

```bash
poetry add pyyaml pydantic
```

---

## Example Usage

```python
from finetl import FinETL

# Load and run from config file
etl = FinETL.from_yaml("pipeline.yaml")
etl.run()
```

Where `pipeline.yaml` contains:

```yaml
name: "full-financial-data"

extraction:
  source: yfinance
  tickers:
    - AAPL
    - TSLA
    - NVDA

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

This produces:
```
./output/
├── ohlcv.csv       # AAPL, TSLA, NVDA price data combined
└── financials.csv  # All 3 tickers, one row per ticker/period, all metrics as columns
```
