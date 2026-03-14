"""Core conversion logic: parquet ↔ csv."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Optional

import polars as pl

from .models import ConversionConfig, ConversionResult, DateFormat


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _size_mb(path: Path) -> float:
    return path.stat().st_size / (1024 * 1024)


def _log(config: ConversionConfig, msg: str, end: str = "\n") -> None:
    if config.verbose:
        print(msg, end=end, flush=True)


def _validate_compression_level(
    level: Optional[int], config: ConversionConfig
) -> Optional[int]:
    """Validate compression_level is in range 1-22, else use default and warn."""
    if level is None:
        return None
    if not (1 <= level <= 22):
        _log(
            config,
            f"Warning: compression_level {level} is out of range (1-22), using default",
        )
        return None
    return level

# ---------------------------------------------------------------------------
# Polars transform helpers (work on LazyFrame)
# ---------------------------------------------------------------------------


def _apply_parquet_to_csv_transforms(
    lf: pl.LazyFrame, config: ConversionConfig
) -> pl.LazyFrame:
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


def _apply_csv_to_parquet_transforms(
    lf: pl.LazyFrame, config: ConversionConfig
) -> pl.LazyFrame:
    """Rename (csv→parquet) → date parsing direction."""
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
                "Datetime",
                "Date",
                "Time",
                "Duration",
            )

            if fmt == DateFormat.INSTANT:
                exprs.append(col.cast(pl.Int64).cast(pl.Datetime("us")))
            elif fmt == DateFormat.ISO:
                if already_temporal:
                    exprs.append(col.cast(pl.Datetime("us")))
                else:
                    exprs.append(
                        col.str.to_datetime(format="%Y-%m-%dT%H:%M:%S%.f", strict=False)
                    )
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

def parquet_to_csv(
    input_path: Path | str,
    output_path: Path | str | None = None,
    config: Optional[ConversionConfig] = None,
) -> ConversionResult:
    """Convert a Parquet file to CSV.

    Args:
        input_path:  Path to the source .parquet file.
        output_path: Destination .csv path. Defaults to the input stem + '.csv'.
        config:      Column rules, delimiter, etc. Defaults to no transformations.

    Returns:
        ConversionResult with timing and size statistics.
    """
    input_path = Path(input_path)
    output_path = Path(output_path) if output_path else input_path.with_suffix(".csv")
    if config is None:
        config = ConversionConfig()

    _log(config, f"[parquet→csv] {input_path.name} → {output_path.name}")

    start = time.time()
    in_mb = _size_mb(input_path)

    lf = pl.scan_parquet(input_path)
    lf = _apply_parquet_to_csv_transforms(lf, config)
    lf.sink_csv(output_path, separator=config.delimiter)

    # Row count is available cheaply from parquet footer metadata.
    rows = pl.scan_parquet(input_path).select(pl.len()).collect().item()

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

def csv_to_parquet(
    input_path: Path | str,
    output_path: Path | str | None = None,
    config: Optional[ConversionConfig] = None,
) -> ConversionResult:
    """Convert a CSV file to Parquet.

    Args:
        input_path:  Path to the source .csv file.
        output_path: Destination .parquet path. Defaults to the input stem + '.parquet'.
        config:      Column rules, delimiter, etc. Defaults to no transformations.

    Returns:
        ConversionResult with timing and size statistics.
    """
    input_path = Path(input_path)
    output_path = (
        Path(output_path) if output_path else input_path.with_suffix(".parquet")
    )
    if config is None:
        config = ConversionConfig()

    _log(config, f"[csv→parquet] {input_path.name} → {output_path.name}")

    start = time.time()
    in_mb = _size_mb(input_path)

    lf = pl.scan_csv(input_path, separator=config.delimiter)
    lf = _apply_csv_to_parquet_transforms(lf, config)

    compression_level = _validate_compression_level(config.compression_level, config)
    lf.sink_parquet(output_path, compression_level=compression_level)

    result = ConversionResult(
        input_path=input_path,
        output_path=output_path,
        rows_converted=None,  # not cheaply available for CSV input
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
    schema = pl.read_parquet_schema(parquet_path)
    row_count = pl.scan_parquet(parquet_path).select(pl.len()).collect().item()

    size_mb = _size_mb(parquet_path)
    print(f"File   : {parquet_path}")
    print(f"Size   : {size_mb:.1f} MB")
    print(f"Rows   : {row_count:,}")
    print(f"Columns: {len(schema)}")
    print()
    print(f"{'#':<5} {'Column':<32} {'Type'}")
    print("-" * 60)
    for i, (name, dtype) in enumerate(schema.items()):
        print(f"{i:<5} {name:<32} {dtype}")
