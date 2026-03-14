"""Shared fixtures for all test modules."""

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

# ---------------------------------------------------------------------------
# Canonical sample data (3 rows, 5 columns — mirrors the ETF parquet schema)
# ---------------------------------------------------------------------------

SAMPLE_DATES = [
    datetime(2021, 1, 15),
    datetime(2021, 1, 16),
    datetime(2021, 6, 1),
]


def make_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "isin": ["IE00B4L5Y983", "IE00B4L5Y983", "LU0290355717"],
            "date": pl.Series(SAMPLE_DATES).cast(pl.Datetime("us")),
            "close": [16.73, 16.90, 42.50],
            "adj_close": [16.73, 16.90, 42.50],
            "dividends": [0.0, 0.0, 0.25],
        }
    )


@pytest.fixture()
def sample_df() -> pl.DataFrame:
    return make_df()


@pytest.fixture()
def sample_parquet(tmp_path: Path) -> Path:
    path = tmp_path / "sample.parquet"
    make_df().write_parquet(path)
    return path


@pytest.fixture()
def sample_csv(tmp_path: Path) -> Path:
    path = tmp_path / "sample.csv"
    make_df().write_csv(path)
    return path
