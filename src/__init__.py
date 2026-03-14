"""parquet_csv_converter — bidirectional Parquet ↔ CSV converter.

Programmatic usage
------------------
    from parquet_csv_converter import (
        parquet_to_csv,
        csv_to_parquet,
        inspect_schema,
        ColumnRule,
        ConversionConfig,
        DateFormat,
    )

    config = ConversionConfig(
        column_rules=[
            ColumnRule(parquet_name="isin",  csv_name="etf_isin"),
            ColumnRule(parquet_name="date",  csv_name="date", date_format=DateFormat.ISO),
        ],
        select_columns=["isin", "date", "close", "adj_close", "dividends"],
    )
    result = parquet_to_csv("data.parquet", "data.csv", config)
    print(result)
"""

from .converter import csv_to_parquet, inspect_schema, parquet_to_csv
from .models import ColumnRule, ConversionConfig, ConversionResult, DateFormat

__all__ = [
    "ColumnRule",
    "ConversionConfig",
    "ConversionResult",
    "DateFormat",
    "parquet_to_csv",
    "csv_to_parquet",
    "inspect_schema",
]
