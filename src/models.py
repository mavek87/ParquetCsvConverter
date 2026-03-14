"""Data models for conversion configuration and results."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, Union


class DateFormat(str, Enum):
    """Specifies how a date/timestamp column is serialized to or from CSV.

    INSTANT  – epoch microseconds stored as int64.
                parquet→csv: cast Datetime → Int64
                csv→parquet: cast Int64 → Datetime(us)

    ISO      – ISO 8601 datetime string, e.g. "2021-01-15T10:30:00.000000".
                parquet→csv: dt.to_string(...)
                csv→parquet: str.to_datetime(...)

    DATE     – date-only string, e.g. "2021-01-15".
                parquet→csv: dt.to_string("%Y-%m-%d")
                csv→parquet: str.to_date("%Y-%m-%d")
    """

    INSTANT = "instant"
    ISO = "iso"
    DATE = "date"


@dataclass
class ColumnRule:
    """Rename and date-format rule for a single column.

    Args:
        parquet_name: Column name as it appears in the Parquet file.
        csv_name:     Column name as it appears in the CSV file.
        date_format:  How to serialize/parse date or timestamp values.
                      - None: no special handling (native Polars default).
                      - DateFormat.INSTANT: epoch microseconds as int64.
                      - DateFormat.ISO: ISO 8601 datetime string.
                      - DateFormat.DATE: YYYY-MM-DD date string.
                      - Any other str: treated as a strptime format string
                        (e.g. "%d/%m/%Y"). parquet→csv uses dt.to_string(fmt);
                        csv→parquet uses str.to_datetime(format=fmt) → Datetime(us).
    """

    parquet_name: str
    csv_name: str
    date_format: Union[DateFormat, str, None] = None


@dataclass
class ConversionConfig:
    """Full configuration for a parquet ↔ csv conversion job.

    Args:
        column_rules:   Per-column rename and date-format rules.
        select_columns: Parquet column names to include; None means all columns.
        delimiter:      CSV field delimiter (default: comma).
        compression_level: Parquet compression level (1-22 for zstd/gzip/brotli).
                          None uses the library default (zstd level 3).
        verbose:        Print progress info to stdout.
    """

    column_rules: list[ColumnRule] = field(default_factory=list)
    select_columns: Optional[list[str]] = None
    delimiter: str = ","
    compression_level: Optional[int] = None
    verbose: bool = True

    # ------------------------------------------------------------------ helpers

    def parquet_to_csv_rename(self) -> dict[str, str]:
        """parquet_name → csv_name (only for columns that are actually renamed)."""
        return {
            r.parquet_name: r.csv_name
            for r in self.column_rules
            if r.parquet_name != r.csv_name
        }

    def csv_to_parquet_rename(self) -> dict[str, str]:
        """csv_name → parquet_name (only for columns that are actually renamed)."""
        return {
            r.csv_name: r.parquet_name
            for r in self.column_rules
            if r.parquet_name != r.csv_name
        }

    def date_rules_by_parquet_name(self) -> dict[str, Union[DateFormat, str]]:
        """parquet_name → DateFormat or custom format string (only for columns with an explicit date_format)."""
        return {
            r.parquet_name: r.date_format
            for r in self.column_rules
            if r.date_format is not None
        }


@dataclass
class ConversionResult:
    """Outcome of a completed conversion."""

    input_path: Path
    output_path: Path
    rows_converted: Optional[int]
    elapsed_seconds: float
    input_size_mb: float
    output_size_mb: float

    def __str__(self) -> str:
        rows = f"{self.rows_converted:,}" if self.rows_converted is not None else "?"
        return (
            f"{rows} rows | "
            f"{self.input_size_mb:.1f} MB → {self.output_size_mb:.1f} MB | "
            f"{self.elapsed_seconds:.2f}s"
        )
