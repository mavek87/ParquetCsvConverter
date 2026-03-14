"""Unit tests for src/models.py."""

from pathlib import Path

import pytest

from src.models import ColumnRule, ConversionConfig, ConversionResult, DateFormat


# ---------------------------------------------------------------------------
# DateFormat
# ---------------------------------------------------------------------------


class TestDateFormat:
    def test_values(self):
        assert DateFormat.INSTANT.value == "instant"
        assert DateFormat.ISO.value == "iso"
        assert DateFormat.DATE.value == "date"

    def test_from_string(self):
        assert DateFormat("instant") is DateFormat.INSTANT
        assert DateFormat("iso") is DateFormat.ISO
        assert DateFormat("date") is DateFormat.DATE

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            DateFormat("unknown")

    def test_is_str_subclass(self):
        assert isinstance(DateFormat.ISO, str)
        assert DateFormat.ISO == "iso"


# ---------------------------------------------------------------------------
# ColumnRule
# ---------------------------------------------------------------------------


class TestColumnRule:
    def test_date_format_defaults_to_none(self):
        rule = ColumnRule(parquet_name="isin", csv_name="etf_isin")
        assert rule.date_format is None

    def test_with_date_format(self):
        rule = ColumnRule(parquet_name="date", csv_name="date", date_format=DateFormat.ISO)
        assert rule.date_format is DateFormat.ISO

    def test_same_name(self):
        rule = ColumnRule(parquet_name="close", csv_name="close")
        assert rule.parquet_name == rule.csv_name

    def test_all_date_formats(self):
        for fmt in DateFormat:
            rule = ColumnRule("col", "col", date_format=fmt)
            assert rule.date_format is fmt

    def test_custom_format_string(self):
        rule = ColumnRule(parquet_name="date", csv_name="date", date_format="%d/%m/%Y")
        assert rule.date_format == "%d/%m/%Y"

    def test_custom_format_string_with_time(self):
        rule = ColumnRule(parquet_name="ts", csv_name="ts", date_format="%Y%m%d %H:%M")
        assert rule.date_format == "%Y%m%d %H:%M"


# ---------------------------------------------------------------------------
# ConversionConfig
# ---------------------------------------------------------------------------


class TestConversionConfig:
    def test_defaults(self):
        cfg = ConversionConfig()
        assert cfg.delimiter == ","
        assert cfg.chunk_size == 100_000
        assert cfg.mode == "lazy"
        assert cfg.verbose is True
        assert cfg.column_rules == []
        assert cfg.select_columns is None

    # -- parquet_to_csv_rename ----------------------------------------------

    def test_parquet_to_csv_rename_only_changed(self):
        cfg = ConversionConfig(
            column_rules=[
                ColumnRule("isin", "etf_isin"),
                ColumnRule("close", "close"),  # same name → excluded
            ]
        )
        assert cfg.parquet_to_csv_rename() == {"isin": "etf_isin"}

    def test_parquet_to_csv_rename_empty(self):
        assert ConversionConfig().parquet_to_csv_rename() == {}

    def test_parquet_to_csv_rename_multiple(self):
        cfg = ConversionConfig(
            column_rules=[
                ColumnRule("isin", "etf_isin"),
                ColumnRule("date", "ts"),
            ]
        )
        assert cfg.parquet_to_csv_rename() == {"isin": "etf_isin", "date": "ts"}

    # -- csv_to_parquet_rename ----------------------------------------------

    def test_csv_to_parquet_rename_only_changed(self):
        cfg = ConversionConfig(
            column_rules=[
                ColumnRule("isin", "etf_isin"),
                ColumnRule("date", "date"),  # same → excluded
            ]
        )
        assert cfg.csv_to_parquet_rename() == {"etf_isin": "isin"}

    def test_csv_to_parquet_rename_empty(self):
        assert ConversionConfig().csv_to_parquet_rename() == {}

    # -- date_rules_by_parquet_name -----------------------------------------

    def test_date_rules_excludes_none(self):
        cfg = ConversionConfig(
            column_rules=[
                ColumnRule("date", "date", date_format=DateFormat.ISO),
                ColumnRule("isin", "etf_isin"),  # no date_format → excluded
            ]
        )
        assert cfg.date_rules_by_parquet_name() == {"date": DateFormat.ISO}

    def test_date_rules_empty(self):
        assert ConversionConfig().date_rules_by_parquet_name() == {}

    def test_date_rules_all_formats(self):
        cfg = ConversionConfig(
            column_rules=[
                ColumnRule("a", "a", DateFormat.INSTANT),
                ColumnRule("b", "b", DateFormat.ISO),
                ColumnRule("c", "c", DateFormat.DATE),
            ]
        )
        rules = cfg.date_rules_by_parquet_name()
        assert rules == {"a": DateFormat.INSTANT, "b": DateFormat.ISO, "c": DateFormat.DATE}

    def test_date_rules_custom_format_string(self):
        cfg = ConversionConfig(
            column_rules=[
                ColumnRule("date", "date", "%d/%m/%Y"),
                ColumnRule("isin", "etf_isin"),  # no date_format → excluded
            ]
        )
        assert cfg.date_rules_by_parquet_name() == {"date": "%d/%m/%Y"}

    def test_column_with_rename_and_date_format(self):
        cfg = ConversionConfig(
            column_rules=[ColumnRule("date", "ts", DateFormat.DATE)]
        )
        assert cfg.parquet_to_csv_rename() == {"date": "ts"}
        assert cfg.date_rules_by_parquet_name() == {"date": DateFormat.DATE}


# ---------------------------------------------------------------------------
# ConversionResult
# ---------------------------------------------------------------------------


class TestConversionResult:
    def _make(self, rows):
        return ConversionResult(
            input_path=Path("in.parquet"),
            output_path=Path("out.csv"),
            rows_converted=rows,
            elapsed_seconds=1.5,
            input_size_mb=10.0,
            output_size_mb=80.5,
        )

    def test_str_with_rows(self):
        s = str(self._make(1_000_000))
        assert "1,000,000 rows" in s
        assert "10.0 MB" in s
        assert "80.5 MB" in s
        assert "1.50s" in s

    def test_str_with_none_rows(self):
        s = str(self._make(None))
        assert "? rows" in s

    def test_str_with_zero_rows(self):
        s = str(self._make(0))
        assert "0 rows" in s
