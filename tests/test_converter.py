"""Unit and integration tests for src/converter.py."""

from pathlib import Path

import polars as pl
import pytest

from src.converter import (
    _apply_csv_to_parquet_transforms,
    _apply_parquet_to_csv_transforms,
    csv_to_parquet,
    inspect_schema,
    parquet_to_csv,
)
from src.models import ColumnRule, ConversionConfig, DateFormat

# Dates as polars-internal epoch µs, derived from the fixture to avoid
# timezone-dependent Python datetime.timestamp() calls.
_LAZY_DATETIME_DTYPES = (pl.Datetime("us"), pl.Datetime("us", None))


def quiet(**kwargs) -> ConversionConfig:
    """ConversionConfig with verbose=False for cleaner test output."""
    return ConversionConfig(verbose=False, **kwargs)


# ---------------------------------------------------------------------------
# _apply_parquet_to_csv_transforms
# ---------------------------------------------------------------------------


class TestApplyParquetToCsvTransforms:
    """Tests for the internal transform helper (parquet→csv direction)."""

    def _run(self, df: pl.DataFrame, cfg: ConversionConfig) -> pl.DataFrame:
        return _apply_parquet_to_csv_transforms(df.lazy(), cfg).collect()

    def test_no_config_passes_through(self, sample_df):
        result = self._run(sample_df, ConversionConfig(verbose=False))
        assert result.columns == sample_df.columns
        assert len(result) == len(sample_df)

    def test_rename(self, sample_df):
        cfg = ConversionConfig(
            column_rules=[ColumnRule("isin", "etf_isin")], verbose=False
        )
        result = self._run(sample_df, cfg)
        assert "etf_isin" in result.columns
        assert "isin" not in result.columns
        assert result["etf_isin"].to_list() == sample_df["isin"].to_list()

    def test_select(self, sample_df):
        cfg = ConversionConfig(select_columns=["isin", "close"], verbose=False)
        result = self._run(sample_df, cfg)
        assert result.columns == ["isin", "close"]

    def test_date_instant_produces_int64(self, sample_df):
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", DateFormat.INSTANT)],
            verbose=False,
        )
        result = self._run(sample_df, cfg)
        assert result["date"].dtype == pl.Int64
        expected = sample_df["date"].cast(pl.Int64)[0]
        assert result["date"][0] == expected

    def test_date_iso_produces_string(self, sample_df):
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", DateFormat.ISO)],
            verbose=False,
        )
        result = self._run(sample_df, cfg)
        assert result["date"].dtype == pl.String
        assert result["date"][0] == "2021-01-15T00:00:00.000000"

    def test_date_date_produces_string(self, sample_df):
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", DateFormat.DATE)],
            verbose=False,
        )
        result = self._run(sample_df, cfg)
        assert result["date"].dtype == pl.String
        assert result["date"][0] == "2021-01-15"

    def test_date_custom_format_produces_string(self, sample_df):
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", "%d/%m/%Y")],
            verbose=False,
        )
        result = self._run(sample_df, cfg)
        assert result["date"].dtype == pl.String
        assert result["date"][0] == "15/01/2021"

    def test_rename_happens_after_date_transform(self, sample_df):
        """The rename uses the parquet name as source, then produces the csv name."""
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "ts", DateFormat.DATE)],
            verbose=False,
        )
        result = self._run(sample_df, cfg)
        assert "ts" in result.columns
        assert "date" not in result.columns
        assert result["ts"][0] == "2021-01-15"

    def test_select_before_rename(self, sample_df):
        """Select uses parquet names; rename applies afterwards."""
        cfg = ConversionConfig(
            column_rules=[ColumnRule("isin", "etf_isin")],
            select_columns=["isin", "close"],
            verbose=False,
        )
        result = self._run(sample_df, cfg)
        assert result.columns == ["etf_isin", "close"]

    def test_multiple_date_formats(self, sample_df):
        extra = sample_df.with_columns(
            pl.col("date").alias("date2")
        )
        cfg = ConversionConfig(
            column_rules=[
                ColumnRule("date", "date", DateFormat.ISO),
                ColumnRule("date2", "date2", DateFormat.DATE),
            ],
            verbose=False,
        )
        result = self._run(extra, cfg)
        assert result["date"][0] == "2021-01-15T00:00:00.000000"
        assert result["date2"][0] == "2021-01-15"


# ---------------------------------------------------------------------------
# _apply_csv_to_parquet_transforms
# ---------------------------------------------------------------------------


class TestApplyCsvToParquetTransforms:
    """Tests for the internal transform helper (csv→parquet direction)."""

    def _run(self, df: pl.DataFrame, cfg: ConversionConfig) -> pl.DataFrame:
        return _apply_csv_to_parquet_transforms(df.lazy(), cfg).collect()

    def test_no_config_passes_through(self, sample_df):
        result = self._run(sample_df, ConversionConfig(verbose=False))
        assert result.columns == sample_df.columns

    def test_rename(self):
        df = pl.DataFrame({"etf_isin": ["A", "B"], "close": [1.0, 2.0]})
        cfg = ConversionConfig(
            column_rules=[ColumnRule("isin", "etf_isin")], verbose=False
        )
        result = self._run(df, cfg)
        assert "isin" in result.columns
        assert "etf_isin" not in result.columns
        assert result["isin"].to_list() == ["A", "B"]

    def test_date_iso_produces_datetime(self):
        df = pl.DataFrame({"date": ["2021-01-15T00:00:00.000000", "2021-06-01T00:00:00.000000"]})
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", DateFormat.ISO)],
            verbose=False,
        )
        result = self._run(df, cfg)
        assert result["date"].dtype in _LAZY_DATETIME_DTYPES

    def test_date_date_produces_date_type(self):
        df = pl.DataFrame({"date": ["2021-01-15", "2021-06-01"]})
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", DateFormat.DATE)],
            verbose=False,
        )
        result = self._run(df, cfg)
        assert result["date"].dtype == pl.Date

    def test_date_instant_produces_datetime(self, sample_df):
        epoch_us = sample_df["date"].cast(pl.Int64)[0]
        df = pl.DataFrame({"date": [epoch_us]})
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", DateFormat.INSTANT)],
            verbose=False,
        )
        result = self._run(df, cfg)
        assert result["date"].dtype in _LAZY_DATETIME_DTYPES

    def test_date_custom_format_produces_datetime(self):
        df = pl.DataFrame({"date": ["15/01/2021", "01/06/2021"]})
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "date", "%d/%m/%Y")],
            verbose=False,
        )
        result = self._run(df, cfg)
        assert result["date"].dtype in _LAZY_DATETIME_DTYPES

    def test_rename_happens_before_date_parse(self):
        """csv_name is renamed to parquet_name first, then date parse uses parquet_name."""
        df = pl.DataFrame({"ts_col": ["2021-01-15T00:00:00.000000"]})
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "ts_col", DateFormat.ISO)],
            verbose=False,
        )
        result = self._run(df, cfg)
        assert "date" in result.columns
        assert result["date"].dtype in _LAZY_DATETIME_DTYPES


# ---------------------------------------------------------------------------
# parquet_to_csv
# ---------------------------------------------------------------------------


class TestParquetToCsv:
    def test_base_no_config(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        result = parquet_to_csv(sample_parquet, out)
        assert result.rows_converted == 3
        df = pl.read_csv(out)
        assert set(df.columns) == {"isin", "date", "close", "adj_close", "dividends"}
        assert len(df) == 3

    def test_default_output_path(self, sample_parquet):
        result = parquet_to_csv(sample_parquet, config=quiet())
        expected = sample_parquet.with_suffix(".csv")
        assert result.output_path == expected
        assert expected.exists()

    def test_rename(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        parquet_to_csv(sample_parquet, out, quiet(column_rules=[ColumnRule("isin", "etf_isin")]))
        df = pl.read_csv(out)
        assert "etf_isin" in df.columns
        assert "isin" not in df.columns

    def test_select_columns(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        parquet_to_csv(sample_parquet, out, quiet(select_columns=["isin", "close"]))
        df = pl.read_csv(out)
        assert df.columns == ["isin", "close"]

    def test_date_instant(self, sample_parquet, tmp_path, sample_df):
        out = tmp_path / "out.csv"
        parquet_to_csv(sample_parquet, out, quiet(column_rules=[ColumnRule("date", "date", DateFormat.INSTANT)]))
        df = pl.read_csv(out)
        assert df["date"].dtype == pl.Int64
        assert df["date"][0] == sample_df["date"].cast(pl.Int64)[0]

    def test_date_iso(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        parquet_to_csv(sample_parquet, out, quiet(column_rules=[ColumnRule("date", "date", DateFormat.ISO)]))
        df = pl.read_csv(out)
        assert df["date"][0] == "2021-01-15T00:00:00.000000"

    def test_date_date(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        parquet_to_csv(sample_parquet, out, quiet(column_rules=[ColumnRule("date", "date", DateFormat.DATE)]))
        df = pl.read_csv(out)
        assert df["date"][0] == "2021-01-15"

    def test_date_custom_format(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        parquet_to_csv(sample_parquet, out, quiet(column_rules=[ColumnRule("date", "date", "%d/%m/%Y")]))
        df = pl.read_csv(out)
        assert df["date"][0] == "15/01/2021"

    def test_rename_and_date_and_select_combined(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        cfg = quiet(
            column_rules=[
                ColumnRule("isin", "etf_isin"),
                ColumnRule("date", "date", DateFormat.ISO),
            ],
            select_columns=["isin", "date", "close"],
        )
        parquet_to_csv(sample_parquet, out, cfg)
        df = pl.read_csv(out)
        assert df.columns == ["etf_isin", "date", "close"]
        assert df["date"][0] == "2021-01-15T00:00:00.000000"

    def test_custom_delimiter(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        parquet_to_csv(sample_parquet, out, quiet(delimiter=";"))
        first_line = out.read_text().split("\n")[0]
        assert ";" in first_line
        assert first_line.count(";") == 4  # 5 columns → 4 separators

    def test_result_attributes(self, sample_parquet, tmp_path):
        out = tmp_path / "out.csv"
        result = parquet_to_csv(sample_parquet, out, quiet())
        assert result.rows_converted == 3
        assert result.input_path == sample_parquet
        assert result.output_path == out
        assert result.input_size_mb > 0
        assert result.output_size_mb > 0
        assert result.elapsed_seconds > 0

    def test_accepts_string_paths(self, sample_parquet, tmp_path):
        out = str(tmp_path / "out.csv")
        parquet_to_csv(str(sample_parquet), out, quiet())
        assert Path(out).exists()


# ---------------------------------------------------------------------------
# csv_to_parquet
# ---------------------------------------------------------------------------


class TestCsvToParquet:
    def test_base_no_config(self, sample_csv, tmp_path):
        out = tmp_path / "out.parquet"
        result = csv_to_parquet(sample_csv, out)
        assert result.rows_converted is None  # not cheaply available for CSV input
        df = pl.read_parquet(out)
        assert len(df) == 3
        assert set(df.columns) == {"isin", "date", "close", "adj_close", "dividends"}

    def test_default_output_path(self, sample_csv):
        result = csv_to_parquet(sample_csv, config=quiet())
        expected = sample_csv.with_suffix(".parquet")
        assert result.output_path == expected
        assert expected.exists()

    def test_rename(self, tmp_path):
        csv = tmp_path / "in.csv"
        pl.DataFrame({"etf_isin": ["A", "B"], "val": [1.0, 2.0]}).write_csv(csv)
        out = tmp_path / "out.parquet"
        csv_to_parquet(csv, out, quiet(column_rules=[ColumnRule("isin", "etf_isin")]))
        df = pl.read_parquet(out)
        assert "isin" in df.columns
        assert "etf_isin" not in df.columns
        assert df["isin"].to_list() == ["A", "B"]

    def test_date_iso(self, tmp_path):
        csv = tmp_path / "in.csv"
        pl.DataFrame({"date": ["2021-01-15T00:00:00.000000", "2021-06-01T00:00:00.000000"]}).write_csv(csv)
        out = tmp_path / "out.parquet"
        csv_to_parquet(csv, out, quiet(column_rules=[ColumnRule("date", "date", DateFormat.ISO)]))
        df = pl.read_parquet(out)
        assert df["date"].dtype in _LAZY_DATETIME_DTYPES

    def test_date_date(self, tmp_path):
        csv = tmp_path / "in.csv"
        pl.DataFrame({"date": ["2021-01-15", "2021-06-01"]}).write_csv(csv)
        out = tmp_path / "out.parquet"
        csv_to_parquet(csv, out, quiet(column_rules=[ColumnRule("date", "date", DateFormat.DATE)]))
        df = pl.read_parquet(out)
        assert df["date"].dtype == pl.Date

    def test_date_instant(self, tmp_path, sample_df):
        epoch_us = sample_df["date"].cast(pl.Int64)[0]
        csv = tmp_path / "in.csv"
        pl.DataFrame({"date": [epoch_us]}).write_csv(csv)
        out = tmp_path / "out.parquet"
        csv_to_parquet(csv, out, quiet(column_rules=[ColumnRule("date", "date", DateFormat.INSTANT)]))
        df = pl.read_parquet(out)
        assert df["date"].dtype in _LAZY_DATETIME_DTYPES

    def test_date_custom_format(self, tmp_path):
        csv = tmp_path / "in.csv"
        pl.DataFrame({"date": ["15/01/2021", "01/06/2021"]}).write_csv(csv)
        out = tmp_path / "out.parquet"
        csv_to_parquet(csv, out, quiet(column_rules=[ColumnRule("date", "date", "%d/%m/%Y")]))
        df = pl.read_parquet(out)
        assert df["date"].dtype in _LAZY_DATETIME_DTYPES

    def test_rename_and_date_combined(self, tmp_path):
        csv = tmp_path / "in.csv"
        pl.DataFrame({
            "etf_isin": ["IE00B4L5Y983"],
            "date": ["2021-01-15T00:00:00.000000"],
            "close": [16.73],
        }).write_csv(csv)
        out = tmp_path / "out.parquet"
        cfg = quiet(
            column_rules=[
                ColumnRule("isin", "etf_isin"),
                ColumnRule("date", "date", DateFormat.ISO),
            ]
        )
        csv_to_parquet(csv, out, cfg)
        df = pl.read_parquet(out)
        assert "isin" in df.columns
        assert df["date"].dtype in _LAZY_DATETIME_DTYPES

    def test_custom_delimiter(self, tmp_path):
        csv = tmp_path / "in.csv"
        csv.write_text("a;b;c\n1;2;3\n4;5;6\n")
        out = tmp_path / "out.parquet"
        csv_to_parquet(csv, out, quiet(delimiter=";"))
        df = pl.read_parquet(out)
        assert df.columns == ["a", "b", "c"]
        assert len(df) == 2

    def test_result_attributes(self, sample_csv, tmp_path):
        out = tmp_path / "out.parquet"
        result = csv_to_parquet(sample_csv, out, quiet())
        assert result.input_path == sample_csv
        assert result.output_path == out
        assert result.input_size_mb > 0
        assert result.output_size_mb > 0
        assert result.elapsed_seconds > 0

    def test_accepts_string_paths(self, sample_csv, tmp_path):
        out = str(tmp_path / "out.parquet")
        csv_to_parquet(str(sample_csv), out, quiet())
        assert Path(out).exists()


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


class TestRoundTrip:
    _RULES = [
        ColumnRule("isin", "etf_isin"),
        ColumnRule("date", "date", DateFormat.ISO),
    ]

    def _cfg(self):
        return quiet(column_rules=self._RULES)

    def test_round_trip_preserves_values(self, sample_parquet, tmp_path):
        csv = tmp_path / "mid.csv"
        out = tmp_path / "out.parquet"
        parquet_to_csv(sample_parquet, csv, self._cfg())
        csv_to_parquet(csv, out, self._cfg())

        original = pl.read_parquet(sample_parquet)
        result = pl.read_parquet(out)
        assert result.columns == original.columns
        assert result["isin"].to_list() == original["isin"].to_list()
        assert result["close"].to_list() == original["close"].to_list()


# ---------------------------------------------------------------------------
# inspect_schema
# ---------------------------------------------------------------------------


class TestInspectSchema:
    def test_output_contains_filename(self, sample_parquet, capsys):
        inspect_schema(sample_parquet)
        assert "sample.parquet" in capsys.readouterr().out

    def test_output_contains_row_count(self, sample_parquet, capsys):
        inspect_schema(sample_parquet)
        assert "3" in capsys.readouterr().out

    def test_output_contains_all_column_names(self, sample_parquet, capsys):
        inspect_schema(sample_parquet)
        out = capsys.readouterr().out
        for col in ["isin", "date", "close", "adj_close", "dividends"]:
            assert col in out

    def test_output_contains_types(self, sample_parquet, capsys):
        inspect_schema(sample_parquet)
        out = capsys.readouterr().out
        assert "String" in out    # isin column
        assert "Datetime" in out  # date column
        assert "Float64" in out   # numeric columns

    def test_accepts_string_path(self, sample_parquet, capsys):
        inspect_schema(str(sample_parquet))
        assert "sample.parquet" in capsys.readouterr().out
