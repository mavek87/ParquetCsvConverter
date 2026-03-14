# Parquet Csv Converter

Python library for bidirectional **Parquet ↔ CSV** conversion with column renaming, date handling, and custom format strings.

---

## Docker

### Build

```bash
docker build -t parquet-csv .
```

### File ownership

By default Docker runs as root (UID 0), so output files written to a host-mounted
volume are root-owned. Pass `--user $(id -u):$(id -g)` to run as the current user
and avoid this.

### Usage

Mount the directory containing your files with `-v $(pwd):/data` and pass paths inside the container.

```bash
# Parquet → CSV
docker run --rm --user "$(id -u):$(id -g)" -v $(pwd):/data parquet-csv \
    -pc /data/data.parquet -o /data/output.csv

# CSV → Parquet
docker run --rm --user "$(id -u):$(id -g)" -v $(pwd):/data parquet-csv \
    -cp /data/data.csv -o /data/output.parquet

# Schema inspection
docker run --rm --user "$(id -u):$(id -g)" -v $(pwd):/data parquet-csv \
    -s /data/data.parquet
```

With additional options:

```bash
# Rename + date format + column selection
docker run --rm --user "$(id -u):$(id -g)" -v $(pwd):/data parquet-csv \
    -pc /data/data.parquet -o /data/output.csv \
    --rename isin:etf_isin \
    --date-format date:iso \
    --select isin,date,close

# Custom date format
docker run --rm --user "$(id -u):$(id -g)" -v $(pwd):/data parquet-csv \
    -pc /data/data.parquet -o /data/output.csv \
    --date-format date:%d/%m/%Y

# JSON rules file (the file must be inside the mounted directory)
docker run --rm --user "$(id -u):$(id -g)" -v $(pwd):/data parquet-csv \
    -pc /data/data.parquet -o /data/output.csv \
    --rules /data/rules.json

# Streaming mode for very large files
docker run --rm --user "$(id -u):$(id -g)" -v $(pwd):/data parquet-csv \
    -pc /data/data.parquet -o /data/output.csv \
    --mode streaming --chunk-size 50000
```

---

## Local installation

```bash
# Runtime dependencies only
uv sync

# Runtime + dev dependencies (includes pytest)
uv sync --group dev
```

---

## Quick start

```bash
# Parquet → CSV, output defaults to same name with .csv extension
uv run -m src -pc data.parquet

# CSV → Parquet, explicit output path
uv run -m src -cp data.csv -o data.parquet

# Schema inspection (reads only the footer, no data loaded)
uv run -m src -s data.parquet
```

---

## CLI

### Parquet → CSV

```bash
# Default: output = same name with .csv extension
uv run -m src -pc data.parquet

# Explicit output path
uv run -m src -pc data.parquet -o output.csv

# Select specific columns (parquet names, comma-separated)
uv run -m src -pc data.parquet -o output.csv \
    --select isin,date,close,adj_close,dividends

# Rename a column: "isin" in parquet → "etf_isin" in CSV
uv run -m src -pc data.parquet -o output.csv \
    --rename isin:etf_isin

# Multiple renames in one call (--rename is repeatable)
uv run -m src -pc data.parquet -o output.csv \
    --rename isin:etf_isin \
    --rename date:trade_date

# Date as ISO 8601 string  →  "2021-01-15T00:00:00.000000"
uv run -m src -pc data.parquet -o output.csv \
    --date-format date:iso

# Date as day-only string  →  "2021-01-15"
uv run -m src -pc data.parquet -o output.csv \
    --date-format date:date

# Date as epoch microseconds (int64)  →  1610668800000000
uv run -m src -pc data.parquet -o output.csv \
    --date-format date:instant

# Custom strptime format  →  "15/01/2021"
uv run -m src -pc data.parquet -o output.csv \
    --date-format date:%d/%m/%Y

# Custom format with time  →  "2021-01-15 10:30"
uv run -m src -pc data.parquet -o output.csv \
    --date-format date:"%Y-%m-%d %H:%M"

# Multiple date columns with different formats (--date-format is repeatable)
uv run -m src -pc data.parquet -o output.csv \
    --date-format date:iso \
    --date-format last_updated:date

# Everything combined: selection + rename + date format
uv run -m src -pc data.parquet -o output.csv \
    --select isin,date,close,adj_close,dividends \
    --rename isin:etf_isin \
    --date-format date:iso

# Custom delimiter (semicolon)
uv run -m src -pc data.parquet -o output.csv \
    --delimiter ";"

# Streaming mode (low RAM, recommended for files >10 GB)
uv run -m src -pc data.parquet -o output.csv \
    --mode streaming --chunk-size 50000

# Rules from a JSON file (see dedicated section below)
uv run -m src -pc data.parquet -o output.csv \
    --rules rules.json
```

### CSV → Parquet

> `--rename` always takes `parquet_name:csv_name`.
> If the CSV column is named `etf_isin` and you want `isin` in the parquet, write `--rename isin:etf_isin`.
> The library automatically inverts the rename direction based on the conversion direction.

```bash
# Default
uv run -m src -cp data.csv

# Explicit output path
uv run -m src -cp data.csv -o output.parquet

# Rename: "etf_isin" in CSV → "isin" in parquet
uv run -m src -cp data.csv -o output.parquet \
    --rename isin:etf_isin

# Parse date: ISO 8601 string → Datetime(us)
uv run -m src -cp data.csv -o output.parquet \
    --date-format date:iso

# Parse date: "YYYY-MM-DD" string → Date type
uv run -m src -cp data.csv -o output.parquet \
    --date-format date:date

# Parse date: epoch µs integer → Datetime(us)
uv run -m src -cp data.csv -o output.parquet \
    --date-format date:instant

# Parse with custom format: "15/01/2021" → Datetime(us)
uv run -m src -cp data.csv -o output.parquet \
    --date-format date:%d/%m/%Y

# Rename + date
uv run -m src -cp data.csv -o output.parquet \
    --rename isin:etf_isin \
    --date-format date:iso

# Semicolon delimiter + streaming
uv run -m src -cp data.csv -o output.parquet \
    --delimiter ";" --mode streaming --chunk-size 100000

# With a JSON rules file
uv run -m src -cp data.csv -o output.parquet \
    --rules rules.json
```

### Schema inspection

```bash
# Print schema and metadata (reads only the footer, no data loaded)
uv run -m src -s data.parquet
```

Example output:

```
File   : data.parquet
Size   : 75.1 MB
Rows   : 10,549,175
Groups : 11
Columns: 5

#     Column                           Type
------------------------------------------------------------
0     close                            double
1     date                             timestamp[us]
2     adj_close                        double
3     isin                             string
4     dividends                        double
```

---

## Date handling (`--date-format`)

`--date-format` controls how a date/timestamp column is serialized or parsed. It always takes the **parquet column name**:

```
--date-format <parquet_col>:<format>
```

### Named formats

| Value | Parquet → CSV | CSV → Parquet |
|---|---|---|
| `instant` | `Datetime` → epoch µs integer (e.g. `1610668800000000`) | `int64` → `Datetime(us)` |
| `iso` | `Datetime` → `"2021-01-15T00:00:00.000000"` | ISO 8601 string → `Datetime(us)` |
| `date` | `Datetime` / `Date` → `"2021-01-15"` | `YYYY-MM-DD` string → `Date` |

### Custom strptime format

Any other string is treated as a Python strptime format string:

| Format | Parquet → CSV | CSV → Parquet |
|---|---|---|
| `%d/%m/%Y` | `Datetime` → `"15/01/2021"` | `"15/01/2021"` string → `Datetime(us)` |
| `%Y%m%d` | `Datetime` → `"20210115"` | `"20210115"` string → `Datetime(us)` |
| `%d/%m/%Y %H:%M` | `Datetime` → `"15/01/2021 10:30"` | `"15/01/2021 10:30"` string → `Datetime(us)` |

```bash
# CLI
uv run -m src -pc data.parquet -o output.csv --date-format date:%d/%m/%Y

# JSON rules
{ "date_format": "%d/%m/%Y" }

# Python API
ColumnRule(parquet_name="date", csv_name="date", date_format="%d/%m/%Y")
```

### Round-trip with the same rules

The same `--rename` and `--date-format` options work in both directions: the library automatically inverts the rename and selects the correct parsing.

```bash
# Export
uv run -m src -pc data.parquet -o data.csv \
    --rename isin:etf_isin \
    --date-format date:iso

# Re-import with the exact same rules
uv run -m src -cp data.csv -o data_restored.parquet \
    --rename isin:etf_isin \
    --date-format date:iso
```

---

## Lazy vs streaming mode

| | `lazy` (default) | `streaming` |
|---|---|---|
| **Engine parquet→csv** | Polars `scan_parquet` + `sink_csv` | PyArrow `iter_batches` |
| **Engine csv→parquet** | Polars `scan_csv` + `sink_parquet` | PyArrow `open_csv` + `ParquetWriter` |
| **When to use** | Files up to a few GB | Very large files (>10 GB) |
| **`--chunk-size`** | Ignored | Parquet→CSV: exact row count per batch; CSV→Parquet: approximate (bytes × 512) |
| **Row count in result** | Parquet→CSV: from metadata; CSV→Parquet: `None` | Always available |
| **Progress output** | No | Yes, one line updated per chunk |

```bash
# Streaming with 50,000-row chunks
uv run -m src -pc data.parquet -o output.csv \
    --mode streaming --chunk-size 50000
```

---

## JSON rules file (`--rules`)

Collects all options in a reusable file. Takes precedence over `--rename`, `--date-format`, and `--select`.

```json
{
  "column_rules": [
    {
      "parquet_name": "isin",
      "csv_name": "etf_isin"
    },
    {
      "parquet_name": "date",
      "csv_name": "date",
      "date_format": "iso"
    },
    {
      "parquet_name": "last_updated",
      "csv_name": "last_updated",
      "date_format": "%d/%m/%Y"
    }
  ],
  "select_columns": ["isin", "date", "close", "adj_close", "dividends"],
  "delimiter": ",",
  "mode": "lazy",
  "chunk_size": 100000
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `column_rules` | array | `[]` | Per-column rules |
| `column_rules[].parquet_name` | string | — | Column name in the Parquet file |
| `column_rules[].csv_name` | string | — | Column name in the CSV file |
| `column_rules[].date_format` | string \| null | `null` | `"instant"`, `"iso"`, `"date"`, strptime format string, or omitted |
| `select_columns` | array \| null | `null` | Parquet columns to include; `null` = all |
| `delimiter` | string | `","` | CSV field separator |
| `mode` | string | `"lazy"` | `"lazy"` or `"streaming"` |
| `chunk_size` | integer | `100000` | Batch size in streaming mode |

Fields absent from the JSON fall back to the CLI flag values (e.g. `--delimiter`, `--mode`).

```bash
uv run -m src -pc data.parquet -o output.csv --rules rules.json
uv run -m src -cp data.csv -o output.parquet --rules rules.json
```

---

## Python API

### Basic usage

```python
from src import parquet_to_csv, csv_to_parquet, inspect_schema
from src import ColumnRule, ConversionConfig, DateFormat

# No config: direct conversion, no transformations
parquet_to_csv("data.parquet")               # output: data.csv
parquet_to_csv("data.parquet", "output.csv")
csv_to_parquet("data.csv")                   # output: data.parquet
csv_to_parquet("data.csv", "output.parquet")
```

### With conversion rules

```python
config = ConversionConfig(
    column_rules=[
        ColumnRule(parquet_name="isin",    csv_name="etf_isin"),
        ColumnRule(parquet_name="date",    csv_name="date",         date_format=DateFormat.ISO),
        ColumnRule(parquet_name="updated", csv_name="last_updated", date_format=DateFormat.DATE),
    ],
    select_columns=["isin", "date", "close", "adj_close", "dividends"],
    delimiter=",",
    mode="lazy",
    verbose=True,
)

result = parquet_to_csv("data.parquet", "output.csv", config)
print(result)
# "10,549,175 rows | 75.1 MB → 608.6 MB | 0.82s"
```

### Custom format string

```python
config = ConversionConfig(
    column_rules=[
        ColumnRule(parquet_name="date", csv_name="date", date_format="%d/%m/%Y"),
    ],
)

# Parquet → CSV: the "date" column is written as "15/01/2021"
parquet_to_csv("data.parquet", "output.csv", config)

# CSV → Parquet: the string "15/01/2021" is parsed back as Datetime(us)
csv_to_parquet("output.csv", "restored.parquet", config)
```

### Streaming mode

```python
config = ConversionConfig(
    column_rules=[
        ColumnRule(parquet_name="isin", csv_name="etf_isin"),
        ColumnRule(parquet_name="date", csv_name="date", date_format=DateFormat.ISO),
    ],
    mode="streaming",
    chunk_size=50_000,
    verbose=True,
)

result = parquet_to_csv("large_file.parquet", "output.csv", config)
```

### Schema inspection

```python
inspect_schema("data.parquet")
# Prints schema, row count, row groups, and column types.
# Reads only the file footer: fast even on files tens of GB in size.
```

### `ConversionResult`

All conversion functions return a `ConversionResult`:

```python
result = parquet_to_csv("data.parquet", "output.csv", config)

result.input_path       # Path("data.parquet")
result.output_path      # Path("output.csv")
result.rows_converted   # 10_549_175  (None in lazy csv→parquet mode)
result.elapsed_seconds  # 0.82
result.input_size_mb    # 75.1
result.output_size_mb   # 608.6
print(result)           # "10,549,175 rows | 75.1 MB → 608.6 MB | 0.82s"
```

### `DateFormat`

```python
DateFormat.INSTANT  # epoch microseconds as int64  →  1610668800000000
DateFormat.ISO      # ISO 8601 string              →  "2021-01-15T00:00:00.000000"
DateFormat.DATE     # day-only string              →  "2021-01-15"
# or any strptime format string:
"%d/%m/%Y"          # →  "15/01/2021"
"%Y%m%d"            # →  "20210115"
```

---

## Tests

```bash
# All tests
uv run pytest tests/ -q

# Verbose output
uv run pytest tests/ -v

# Single module
uv run pytest tests/test_converter.py -v

# Single test
uv run pytest tests/test_converter.py::TestParquetToCsvLazy::test_date_iso -v
```
