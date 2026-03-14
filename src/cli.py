"""Command-line interface for parquet-csv-converter."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .converter import csv_to_parquet, inspect_schema, parquet_to_csv
from .models import ColumnRule, ConversionConfig, DateFormat

_DATE_FORMAT_VALUES = [f.value for f in DateFormat]


# ---------------------------------------------------------------------------
# Config builders
# ---------------------------------------------------------------------------

def _config_from_json(path: str, args: argparse.Namespace) -> ConversionConfig:
    with open(path) as f:
        data = json.load(f)

    def _parse_date_format(raw):
        if not raw:
            return None
        try:
            return DateFormat(raw)
        except ValueError:
            return raw  # custom format string

    column_rules = [
        ColumnRule(
            parquet_name=r["parquet_name"],
            csv_name=r["csv_name"],
            date_format=_parse_date_format(r.get("date_format")),
        )
        for r in data.get("column_rules", [])
    ]
    return ConversionConfig(
        column_rules=column_rules,
        select_columns=data.get("select_columns"),
        delimiter=data.get("delimiter", args.delimiter),
        chunk_size=data.get("chunk_size", args.chunk_size),
        mode=data.get("mode", args.mode),
        verbose=True,
    )


def _config_from_flags(args: argparse.Namespace) -> ConversionConfig:
    rename_map: dict[str, str] = {}
    for r in args.rename or []:
        parts = r.split(":", 1)
        if len(parts) != 2:
            sys.exit(f"Error: --rename expects 'parquet_col:csv_col', got '{r}'")
        rename_map[parts[0]] = parts[1]

    date_map: dict[str, object] = {}
    for d in args.date_format or []:
        parts = d.split(":", 1)
        if len(parts) != 2:
            sys.exit(f"Error: --date-format expects 'col:format', got '{d}'")
        try:
            date_map[parts[0]] = DateFormat(parts[1])
        except ValueError:
            date_map[parts[0]] = parts[1]  # custom format string

    all_parquet_cols = set(rename_map) | set(date_map)
    column_rules = [
        ColumnRule(
            parquet_name=col,
            csv_name=rename_map.get(col, col),
            date_format=date_map.get(col),
        )
        for col in all_parquet_cols
    ]

    select_columns = [c.strip() for c in args.select.split(",")] if args.select else None

    return ConversionConfig(
        column_rules=column_rules,
        select_columns=select_columns,
        delimiter=args.delimiter,
        chunk_size=args.chunk_size,
        mode=args.mode,
        verbose=True,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(
        prog="parquet-csv-converter",
        description="Bidirectional Parquet ↔ CSV converter with column mapping and date rules.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
examples:
  # Parquet → CSV (lazy mode, default)
  parquet-csv-converter -pc data.parquet

  # Parquet → CSV with column rename and date as ISO string
  parquet-csv-converter -pc data.parquet out.csv \\
      --rename isin:etf_isin --date-format date:iso

  # CSV → Parquet with streaming mode and semicolon delimiter
  parquet-csv-converter -cp data.csv out.parquet \\
      --mode streaming --delimiter ";"

  # Print Parquet schema
  parquet-csv-converter -s data.parquet

  # Use a JSON rules file
  parquet-csv-converter -pc data.parquet -o out.csv --rules rules.json
""",
    )

    direction = ap.add_mutually_exclusive_group(required=True)
    direction.add_argument(
        "-pc", "--parquet2csv",
        metavar="INPUT.parquet",
        help="Convert Parquet → CSV",
    )
    direction.add_argument(
        "-cp", "--csv2parquet",
        metavar="INPUT.csv",
        help="Convert CSV → Parquet",
    )
    direction.add_argument(
        "-s", "--schema",
        metavar="INPUT.parquet",
        help="Print schema of a Parquet file (no conversion)",
    )

    ap.add_argument(
        "-o", "--output",
        metavar="OUTPUT",
        help="Output file path (default: input stem with new extension)",
    )
    ap.add_argument(
        "-d", "--delimiter",
        default=",",
        metavar="CHAR",
        help="CSV field delimiter (default: ',')",
    )
    ap.add_argument(
        "--mode",
        choices=["lazy", "streaming"],
        default="lazy",
        help="'lazy' = Polars sink, fast (default); 'streaming' = explicit batching, low RAM",
    )
    ap.add_argument(
        "--chunk-size",
        type=int,
        default=100_000,
        metavar="N",
        help="Rows per chunk in streaming mode (default: 100000)",
    )
    ap.add_argument(
        "--rename",
        action="append",
        metavar="PARQUET_COL:CSV_COL",
        help="Rename a column. Use the parquet column name on the left. Repeatable.",
    )
    ap.add_argument(
        "--date-format",
        action="append",
        metavar="COL:FORMAT",
        help=(
            f"Date handling for a column (parquet name). "
            f"Named formats: {_DATE_FORMAT_VALUES}. "
            f"Any other value is treated as a strptime format string (e.g. '%%d/%%m/%%Y'). "
            f"Repeatable."
        ),
    )
    ap.add_argument(
        "--select",
        metavar="COL1,COL2,...",
        help="Comma-separated list of parquet columns to include (default: all)",
    )
    ap.add_argument(
        "--rules",
        metavar="RULES.json",
        help="JSON file with full conversion rules (overrides --rename/--date-format/--select)",
    )

    return ap


def _require_file(path: str, label: str = "Input file") -> None:
    if not Path(path).is_file():
        sys.exit(f"Error: {label} not found: {path}")


def main() -> None:
    args = build_parser().parse_args()

    if args.rules:
        _require_file(args.rules, "Rules file")

    if args.schema:
        _require_file(args.schema)
        inspect_schema(args.schema)
        return

    config = _config_from_json(args.rules, args) if args.rules else _config_from_flags(args)

    if args.parquet2csv:
        _require_file(args.parquet2csv)
        parquet_to_csv(args.parquet2csv, args.output, config)
    elif args.csv2parquet:
        _require_file(args.csv2parquet)
        csv_to_parquet(args.csv2parquet, args.output, config)
