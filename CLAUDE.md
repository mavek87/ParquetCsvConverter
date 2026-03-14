# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A Python library for bidirectional Parquet ↔ CSV conversion with column mapping and date-format rules. Supports both fast (Polars lazy) and low-RAM (streaming batch) modes.

## Running

```bash
# Install / sync deps
uv sync

# CLI — parquet → csv
uv run -m src -pc data.parquet [out.csv]

# CLI — csv → parquet
uv run -m src -cp data.csv [out.parquet]

# Schema inspection (reads only footer, no data load)
uv run -m src -s data.parquet

# With column rules via flags
uv run -m src -pc data.parquet \
    --rename isin:etf_isin \
    --date-format date:iso \
    --select isin,date,close,adj_close,dividends

# With rules JSON file
uv run -m src -pc data.parquet --rules rules_example.json

# Streaming mode (low RAM, for >10 GB files)
uv run -m src -pc data.parquet --mode streaming --chunk-size 50000
```

```bash
# Run tests
uv run pytest tests/ -q

# Run a single test file
uv run pytest tests/test_converter.py -v
```

## Package structure

```
src/
├── __init__.py      # Public API exports
├── __main__.py      # python -m entry point
├── models.py        # DateFormat, ColumnRule, ConversionConfig, ConversionResult
├── converter.py     # Core logic: parquet_to_csv, csv_to_parquet, inspect_schema
└── cli.py           # argparse CLI, JSON rules loader
```

## Architecture

**models.py** defines the three key types:
- `DateFormat` enum: `INSTANT` (epoch int64), `ISO` (datetime string), `DATE` (date string)
- `ColumnRule(parquet_name, csv_name, date_format)` — per-column rule
- `ConversionConfig` — collects all rules plus `mode`, `delimiter`, `chunk_size`

**converter.py** implements two conversion directions and two modes each:

| Function | lazy mode | streaming mode |
|---|---|---|
| `parquet_to_csv` | `pl.scan_parquet` + `sink_csv` | PyArrow `iter_batches` |
| `csv_to_parquet` | `pl.scan_csv` + `sink_parquet` | `pl.read_csv_batched` + PyArrow `ParquetWriter` |

Transform pipeline for **parquet→csv**: select columns → apply date formats → rename.
Transform pipeline for **csv→parquet**: rename → apply date parsing.

Both pipelines operate on `pl.LazyFrame` so they compose with either mode (lazy collects the full result; streaming applies them eagerly per batch).

**cli.py** builds config from either `--rules FILE.json` or individual flags (`--rename`, `--date-format`, `--select`). `--rules` takes precedence over flags.

## Programmatic usage

```python
from src import parquet_to_csv, csv_to_parquet, ColumnRule, ConversionConfig, DateFormat

config = ConversionConfig(
    column_rules=[
        ColumnRule(parquet_name="isin", csv_name="etf_isin"),
        ColumnRule(parquet_name="date", csv_name="date", date_format=DateFormat.ISO),
    ],
    select_columns=["isin", "date", "close", "adj_close", "dividends"],
    mode="lazy",
)
result = parquet_to_csv("data.parquet", "data.csv", config)
print(result)  # "10549175 rows | 76.3 MB → 609.1 MB | 4.21s"
```

## Rules JSON format

```json
{
  "column_rules": [
    { "parquet_name": "isin", "csv_name": "etf_isin" },
    { "parquet_name": "date", "csv_name": "date", "date_format": "iso" }
  ],
  "select_columns": ["isin", "date", "close", "adj_close", "dividends"],
  "delimiter": ",",
  "mode": "lazy",
  "chunk_size": 100000
}
```

`date_format` values: `"instant"` (epoch µs int64), `"iso"` (ISO 8601 datetime string), `"date"` (YYYY-MM-DD string).
