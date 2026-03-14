"""Core conversion logic: parquet ↔ csv."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import polars as pl
import pyarrow.parquet as pq

from .models import ConversionConfig, ConversionResult, DateFormat


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


def _log(config: ConversionConfig, msg: str, end: str = "\n") -> None:
    if config.verbose:
        print(msg, end=end, flush=True)


# ---------------------------------------------------------------------------
# Polars transform helpers (work on LazyFrame)
# ---------------------------------------------------------------------------

def _apply_parquet_to_csv_transforms(lf: pl.LazyFrame, config: ConversionConfig) -> pl.LazyFrame:
    """Column selection → date formatting → rename (parquet→csv direction)."""
    if config.select_columns:
        lf = lf.select(config.select_columns)

    date_rules = config.date_rules_by_parquet_name()
    if date_rules:
        exprs = []
        for col_name, fmt in date_rules.items():
            if fmt == DateFormat.INSTANT:
                exprs.append(pl.col(col_name).cast(pl.Int64))
            elif fmt == DateFormat.ISO:
                exprs.append(pl.col(col_name).dt.to_string("%Y-%m-%dT%H:%M:%S%.6f"))
            elif fmt == DateFormat.DATE:
                exprs.append(pl.col(col_name).dt.to_string("%Y-%m-%d"))
            else:
                # custom strptime format string → dt.to_string(fmt)
                exprs.append(pl.col(col_name).dt.to_string(fmt))
        lf = lf.with_columns(exprs)

    rename_map = config.parquet_to_csv_rename()
    if rename_map:
        lf = lf.rename(rename_map)

    return lf


def _apply_csv_to_parquet_transforms(lf: pl.LazyFrame, config: ConversionConfig) -> pl.LazyFrame:
    """Rename (csv→parquet) → date parsing direction.

    Works for both sources:
    - polars scan_csv (lazy): date columns arrive as String
    - pyarrow open_csv (streaming): date columns may arrive pre-parsed as Datetime
    The schema is inspected after rename to choose the right expression per column.
    """
    rename_map = config.csv_to_parquet_rename()
    if rename_map:
        lf = lf.rename(rename_map)

    date_rules = config.date_rules_by_parquet_name()
    if date_rules:
        schema = lf.collect_schema()
        exprs = []
        for col_name, fmt in date_rules.items():
            col = pl.col(col_name)
            dtype = schema.get(col_name)
            already_temporal = dtype is not None and type(dtype).__name__ in (
                "Datetime", "Date", "Time", "Duration"
            )

            if fmt == DateFormat.INSTANT:
                # CSV stores epoch-microseconds as int64 (or integer-string)
                exprs.append(col.cast(pl.Int64).cast(pl.Datetime("us")))
            elif fmt == DateFormat.ISO:
                if already_temporal:
                    exprs.append(col.cast(pl.Datetime("us")))
                else:
                    exprs.append(col.str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f", strict=False))
            elif fmt == DateFormat.DATE:
                if already_temporal:
                    exprs.append(col.cast(pl.Date))
                else:
                    exprs.append(col.str.to_date(format="%Y-%m-%d", strict=False))
            else:
                # custom format → str.to_datetime(format=fmt) → Datetime(us)
                exprs.append(col.str.to_datetime(format=fmt, strict=False))
        lf = lf.with_columns(exprs)

    return lf


# ---------------------------------------------------------------------------
# Parquet → CSV
# ---------------------------------------------------------------------------

def _parquet_to_csv_lazy(input_path: Path, output_path: Path, config: ConversionConfig) -> Optional[int]:
    """Polars lazy sink — fast, handles most file sizes with low memory."""
    lf = pl.scan_parquet(input_path)
    lf = _apply_parquet_to_csv_transforms(lf, config)
    lf.sink_csv(output_path, separator=config.delimiter)
    # Row count is available cheaply from parquet metadata
    return pq.read_metadata(input_path).num_rows


def _parquet_to_csv_streaming(input_path: Path, output_path: Path, config: ConversionConfig) -> int:
    """PyArrow iter_batches — explicit chunking for very large files (>10 GB)."""
    parquet_file = pq.ParquetFile(input_path)
    n_groups = parquet_file.metadata.num_row_groups
    _log(config, f"  Row groups: {n_groups} | Chunk size: {config.chunk_size:,} rows")

    total_rows = 0
    first_chunk = True
    start = time.time()

    with open(output_path, "wb") as f_out:
        for i, batch in enumerate(parquet_file.iter_batches(batch_size=config.chunk_size)):
            df = pl.from_arrow(batch)
            df = _apply_parquet_to_csv_transforms(df.lazy(), config).collect()

            rows = len(df)
            total_rows += rows

            csv_bytes = df.write_csv(separator=config.delimiter, include_header=first_chunk).encode()
            f_out.write(csv_bytes)
            first_chunk = False

            elapsed = time.time() - start
            speed = total_rows / elapsed if elapsed > 0 else 0
            _log(
                config,
                f"  chunk {i + 1:>4} | rows: {rows:>8,} | total: {total_rows:>10,} | {speed:>10,.0f} rows/s",
                end="\r",
            )

    _log(config, "")
    return total_rows


def parquet_to_csv(
    input_path: Path | str,
    output_path: Path | str | None = None,
    config: Optional[ConversionConfig] = None,
) -> ConversionResult:
    """Convert a Parquet file to CSV.

    Args:
        input_path:  Path to the source .parquet file.
        output_path: Destination .csv path. Defaults to the input stem + '.csv'.
        config:      Column rules, delimiter, mode, etc. Defaults to no transformations.

    Returns:
        ConversionResult with timing and size statistics.
    """
    input_path = Path(input_path)
    output_path = Path(output_path) if output_path else input_path.with_suffix(".csv")
    if config is None:
        config = ConversionConfig()

    _log(config, f"[parquet→csv] {input_path.name} → {output_path.name}  (mode={config.mode})")

    start = time.time()
    in_mb = _size_mb(input_path)

    if config.mode == "streaming":
        rows = _parquet_to_csv_streaming(input_path, output_path, config)
    else:
        rows = _parquet_to_csv_lazy(input_path, output_path, config)

    result = ConversionResult(
        input_path=input_path,
        output_path=output_path,
        rows_converted=rows,
        elapsed_seconds=time.time() - start,
        input_size_mb=in_mb,
        output_size_mb=_size_mb(output_path),
    )
    _log(config, f"✓ {result}")
    return result


# ---------------------------------------------------------------------------
# CSV → Parquet
# ---------------------------------------------------------------------------

def _csv_to_parquet_lazy(input_path: Path, output_path: Path, config: ConversionConfig) -> Optional[int]:
    """Polars lazy sink — fast, handles most file sizes."""
    lf = pl.scan_csv(input_path, separator=config.delimiter)
    lf = _apply_csv_to_parquet_transforms(lf, config)
    lf.sink_parquet(output_path)
    return None  # row count not cheaply available for CSV


def _csv_to_parquet_streaming(input_path: Path, output_path: Path, config: ConversionConfig) -> int:
    """PyArrow open_csv — explicit batching for very large CSV files.

    chunk_size is mapped to pyarrow block_size in bytes (chunk_size * 512),
    so it remains an approximate row-count control rather than an exact value.
    """
    import pyarrow.csv as pa_csv

    reader = pa_csv.open_csv(
        input_path,
        read_options=pa_csv.ReadOptions(block_size=config.chunk_size * 512),
        parse_options=pa_csv.ParseOptions(delimiter=config.delimiter),
        # Disable timestamp auto-inference so date columns arrive as strings,
        # letting _apply_csv_to_parquet_transforms handle parsing explicitly.
        convert_options=pa_csv.ConvertOptions(timestamp_parsers=[]),
    )

    writer = None
    total_rows = 0
    chunk_idx = 0
    start = time.time()

    for batch in reader:
        df = pl.from_arrow(batch)
        df = _apply_csv_to_parquet_transforms(df.lazy(), config).collect()

        rows = len(df)
        total_rows += rows

        arrow_table = df.to_arrow()
        if writer is None:
            writer = pq.ParquetWriter(output_path, arrow_table.schema)
        writer.write_table(arrow_table)

        elapsed = time.time() - start
        speed = total_rows / elapsed if elapsed > 0 else 0
        _log(
            config,
            f"  chunk {chunk_idx + 1:>4} | rows: {rows:>8,} | total: {total_rows:>10,} | {speed:>10,.0f} rows/s",
            end="\r",
        )
        chunk_idx += 1

    if writer:
        writer.close()

    _log(config, "")
    return total_rows


def csv_to_parquet(
    input_path: Path | str,
    output_path: Path | str | None = None,
    config: Optional[ConversionConfig] = None,
) -> ConversionResult:
    """Convert a CSV file to Parquet.

    Args:
        input_path:  Path to the source .csv file.
        output_path: Destination .parquet path. Defaults to the input stem + '.parquet'.
        config:      Column rules, delimiter, mode, etc. Defaults to no transformations.

    Returns:
        ConversionResult with timing and size statistics.
    """
    input_path = Path(input_path)
    output_path = Path(output_path) if output_path else input_path.with_suffix(".parquet")
    if config is None:
        config = ConversionConfig()

    _log(config, f"[csv→parquet] {input_path.name} → {output_path.name}  (mode={config.mode})")

    start = time.time()
    in_mb = _size_mb(input_path)

    if config.mode == "streaming":
        rows = _csv_to_parquet_streaming(input_path, output_path, config)
    else:
        rows = _csv_to_parquet_lazy(input_path, output_path, config)

    result = ConversionResult(
        input_path=input_path,
        output_path=output_path,
        rows_converted=rows,
        elapsed_seconds=time.time() - start,
        input_size_mb=in_mb,
        output_size_mb=_size_mb(output_path),
    )
    _log(config, f"✓ {result}")
    return result


# ---------------------------------------------------------------------------
# Schema inspection
# ---------------------------------------------------------------------------

def inspect_schema(parquet_path: Path | str) -> None:
    """Print the schema and metadata of a Parquet file (reads only the footer).

    Args:
        parquet_path: Path to the .parquet file to inspect.
    """
    parquet_path = Path(parquet_path)
    meta = pq.read_metadata(parquet_path)
    schema = pq.read_schema(parquet_path)

    size_mb = _size_mb(parquet_path)
    print(f"File   : {parquet_path}")
    print(f"Size   : {size_mb:.1f} MB")
    print(f"Rows   : {meta.num_rows:,}")
    print(f"Groups : {meta.num_row_groups}")
    print(f"Columns: {meta.num_columns}")
    print()
    print(f"{'#':<5} {'Column':<32} {'Type'}")
    print("-" * 60)
    for i, name in enumerate(schema.names):
        arrow_type = schema.field(name).type
        print(f"{i:<5} {name:<32} {arrow_type}")
