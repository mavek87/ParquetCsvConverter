# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

A Python library for bidirectional Parquet ↔ CSV conversion with column mapping and date-format rules. Pure Polars — no PyArrow dependency.

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
- `ConversionConfig` — collects all rules plus `delimiter`

**converter.py** implements two conversion directions using Polars lazy evaluation:

| Function | Engine |
|---|---|
| `parquet_to_csv` | `pl.scan_parquet` + `sink_csv` |
| `csv_to_parquet` | `pl.scan_csv` + `sink_parquet` |

Both `sink_csv` and `sink_parquet` stream data internally, so memory usage stays low regardless of file size.

Transform pipeline for **parquet→csv**: select columns → apply date formats → rename.
Transform pipeline for **csv→parquet**: rename → apply date parsing.

Both pipelines operate on `pl.LazyFrame` and are composed before the sink call.

Row count for `parquet_to_csv` is read cheaply from the Parquet footer metadata via
`pl.scan_parquet(path).select(pl.len()).collect().item()` after writing.
Row count for `csv_to_parquet` is `None` (not cheaply available for CSV input).

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
)
result = parquet_to_csv("data.parquet", "data.csv", config)
print(result)  # "10,549,175 rows | 76.3 MB → 609.1 MB | 4.21s"
```

## Rules JSON format

```json
{
  "column_rules": [
    { "parquet_name": "isin", "csv_name": "etf_isin" },
    { "parquet_name": "date", "csv_name": "date", "date_format": "iso" }
  ],
  "select_columns": ["isin", "date", "close", "adj_close", "dividends"],
  "delimiter": ","
}
```

`date_format` values: `"instant"` (epoch µs int64), `"iso"` (ISO 8601 datetime string), `"date"` (YYYY-MM-DD string).
