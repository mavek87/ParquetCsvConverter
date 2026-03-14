"""Unit and integration tests for src/cli.py."""

import argparse
import json
from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from src.cli import _config_from_flags, _config_from_json, build_parser, main
from src.models import DateFormat


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_args(**kwargs) -> argparse.Namespace:
    """Build a minimal Namespace with sensible defaults, overridable via kwargs."""
    defaults = dict(
        rename=None,
        date_format=None,
        select=None,
        delimiter=",",
        chunk_size=100_000,
        mode="lazy",
    )
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


def write_rules(tmp_path: Path, data: dict) -> str:
    path = tmp_path / "rules.json"
    path.write_text(json.dumps(data))
    return str(path)


# ---------------------------------------------------------------------------
# _config_from_json
# ---------------------------------------------------------------------------


class TestConfigFromJson:
    def test_loads_column_rules(self, tmp_path):
        path = write_rules(tmp_path, {
            "column_rules": [
                {"parquet_name": "isin", "csv_name": "etf_isin"},
                {"parquet_name": "date", "csv_name": "date", "date_format": "iso"},
            ]
        })
        cfg = _config_from_json(path, make_args())
        assert len(cfg.column_rules) == 2
        r0 = cfg.column_rules[0]
        assert r0.parquet_name == "isin"
        assert r0.csv_name == "etf_isin"
        assert r0.date_format is None
        assert cfg.column_rules[1].date_format is DateFormat.ISO

    def test_loads_select_columns(self, tmp_path):
        path = write_rules(tmp_path, {
            "column_rules": [],
            "select_columns": ["isin", "close"],
        })
        cfg = _config_from_json(path, make_args())
        assert cfg.select_columns == ["isin", "close"]

    def test_loads_delimiter_mode_chunk_size(self, tmp_path):
        path = write_rules(tmp_path, {
            "column_rules": [],
            "delimiter": ";",
            "mode": "streaming",
            "chunk_size": 5_000,
        })
        cfg = _config_from_json(path, make_args())
        assert cfg.delimiter == ";"
        assert cfg.mode == "streaming"
        assert cfg.chunk_size == 5_000

    def test_missing_fields_fall_back_to_args_defaults(self, tmp_path):
        path = write_rules(tmp_path, {"column_rules": []})
        cfg = _config_from_json(path, make_args(delimiter=";", mode="streaming", chunk_size=999))
        assert cfg.delimiter == ";"
        assert cfg.mode == "streaming"
        assert cfg.chunk_size == 999

    def test_no_date_format_field_gives_none(self, tmp_path):
        path = write_rules(tmp_path, {
            "column_rules": [{"parquet_name": "isin", "csv_name": "etf_isin"}]
        })
        cfg = _config_from_json(path, make_args())
        assert cfg.column_rules[0].date_format is None

    def test_all_date_format_values(self, tmp_path):
        for fmt in ("instant", "iso", "date"):
            path = write_rules(tmp_path, {
                "column_rules": [{"parquet_name": "d", "csv_name": "d", "date_format": fmt}]
            })
            cfg = _config_from_json(path, make_args())
            assert cfg.column_rules[0].date_format == DateFormat(fmt)

    def test_custom_format_string(self, tmp_path):
        path = write_rules(tmp_path, {
            "column_rules": [{"parquet_name": "date", "csv_name": "date", "date_format": "%Y%m%d"}]
        })
        cfg = _config_from_json(path, make_args())
        assert cfg.column_rules[0].date_format == "%Y%m%d"

    def test_empty_column_rules(self, tmp_path):
        path = write_rules(tmp_path, {"column_rules": []})
        cfg = _config_from_json(path, make_args())
        assert cfg.column_rules == []

    def test_select_columns_none_when_absent(self, tmp_path):
        path = write_rules(tmp_path, {"column_rules": []})
        cfg = _config_from_json(path, make_args())
        assert cfg.select_columns is None


# ---------------------------------------------------------------------------
# _config_from_flags
# ---------------------------------------------------------------------------


class TestConfigFromFlags:
    def test_single_rename(self):
        cfg = _config_from_flags(make_args(rename=["isin:etf_isin"]))
        assert cfg.parquet_to_csv_rename() == {"isin": "etf_isin"}

    def test_multiple_renames(self):
        cfg = _config_from_flags(make_args(rename=["isin:etf_isin", "date:ts"]))
        rename = cfg.parquet_to_csv_rename()
        assert rename["isin"] == "etf_isin"
        assert rename["date"] == "ts"

    def test_single_date_format(self):
        cfg = _config_from_flags(make_args(date_format=["date:iso"]))
        assert cfg.date_rules_by_parquet_name() == {"date": DateFormat.ISO}

    def test_all_date_format_values(self):
        for fmt in ("instant", "iso", "date"):
            cfg = _config_from_flags(make_args(date_format=[f"date:{fmt}"]))
            assert cfg.date_rules_by_parquet_name()["date"] == DateFormat(fmt)

    def test_rename_and_date_format_on_same_column(self):
        cfg = _config_from_flags(make_args(rename=["date:ts"], date_format=["date:iso"]))
        rules = {r.parquet_name: r for r in cfg.column_rules}
        assert rules["date"].csv_name == "ts"
        assert rules["date"].date_format == DateFormat.ISO

    def test_select(self):
        cfg = _config_from_flags(make_args(select="isin,close,date"))
        assert cfg.select_columns == ["isin", "close", "date"]

    def test_select_strips_whitespace(self):
        cfg = _config_from_flags(make_args(select="isin, close, date"))
        assert cfg.select_columns == ["isin", "close", "date"]

    def test_no_flags_gives_empty_rules(self):
        cfg = _config_from_flags(make_args())
        assert cfg.column_rules == []
        assert cfg.select_columns is None
        assert cfg.delimiter == ","
        assert cfg.mode == "lazy"
        assert cfg.chunk_size == 100_000

    def test_invalid_rename_no_colon(self):
        with pytest.raises(SystemExit):
            _config_from_flags(make_args(rename=["isin_without_colon"]))

    def test_custom_format_string_via_flags(self):
        cfg = _config_from_flags(make_args(date_format=["date:%d/%m/%Y"]))
        assert cfg.date_rules_by_parquet_name()["date"] == "%d/%m/%Y"

    def test_unknown_named_format_treated_as_custom(self):
        cfg = _config_from_flags(make_args(date_format=["date:unknown"]))
        assert cfg.date_rules_by_parquet_name()["date"] == "unknown"

    def test_invalid_date_format_no_colon(self):
        with pytest.raises(SystemExit):
            _config_from_flags(make_args(date_format=["dateiso"]))

    def test_custom_delimiter_and_mode(self):
        cfg = _config_from_flags(make_args(delimiter=";", mode="streaming", chunk_size=500))
        assert cfg.delimiter == ";"
        assert cfg.mode == "streaming"
        assert cfg.chunk_size == 500


# ---------------------------------------------------------------------------
# build_parser
# ---------------------------------------------------------------------------


class TestBuildParser:
    def parse(self, args: list[str]) -> argparse.Namespace:
        return build_parser().parse_args(args)

    def test_parquet2csv_long_flag(self):
        assert self.parse(["--parquet2csv", "data.parquet"]).parquet2csv == "data.parquet"

    def test_parquet2csv_short_flag(self):
        assert self.parse(["-pc", "data.parquet"]).parquet2csv == "data.parquet"

    def test_csv2parquet_long_flag(self):
        assert self.parse(["--csv2parquet", "data.csv"]).csv2parquet == "data.csv"

    def test_csv2parquet_short_flag(self):
        assert self.parse(["-cp", "data.csv"]).csv2parquet == "data.csv"

    def test_schema_long_flag(self):
        assert self.parse(["--schema", "data.parquet"]).schema == "data.parquet"

    def test_schema_short_flag(self):
        assert self.parse(["-s", "data.parquet"]).schema == "data.parquet"

    def test_defaults(self):
        args = self.parse(["-pc", "data.parquet"])
        assert args.delimiter == ","
        assert args.mode == "lazy"
        assert args.chunk_size == 100_000
        assert args.output is None
        assert args.rename is None
        assert args.date_format is None
        assert args.select is None
        assert args.rules is None

    def test_output_short_flag(self):
        assert self.parse(["-pc", "d.parquet", "-o", "out.csv"]).output == "out.csv"

    def test_output_long_flag(self):
        assert self.parse(["-pc", "d.parquet", "--output", "out.csv"]).output == "out.csv"

    def test_mode_streaming(self):
        assert self.parse(["-pc", "d.parquet", "--mode", "streaming"]).mode == "streaming"

    def test_chunk_size(self):
        assert self.parse(["-pc", "d.parquet", "--chunk-size", "50000"]).chunk_size == 50000

    def test_delimiter_short(self):
        assert self.parse(["-pc", "d.parquet", "-d", ";"]).delimiter == ";"

    def test_delimiter_long(self):
        assert self.parse(["-pc", "d.parquet", "--delimiter", ";"]).delimiter == ";"

    def test_rename_repeatable(self):
        args = self.parse(["-pc", "d.parquet", "--rename", "a:b", "--rename", "c:d"])
        assert args.rename == ["a:b", "c:d"]

    def test_date_format_repeatable(self):
        args = self.parse(["-pc", "d.parquet", "--date-format", "date:iso", "--date-format", "ts:instant"])
        assert args.date_format == ["date:iso", "ts:instant"]

    def test_select(self):
        assert self.parse(["-pc", "d.parquet", "--select", "a,b,c"]).select == "a,b,c"

    def test_rules(self):
        assert self.parse(["-pc", "d.parquet", "--rules", "r.json"]).rules == "r.json"

    def test_mutually_exclusive_direction_flags(self):
        with pytest.raises(SystemExit):
            self.parse(["-pc", "a.parquet", "-cp", "b.csv"])

    def test_direction_flag_required(self):
        with pytest.raises(SystemExit):
            self.parse(["--delimiter", ";"])

    def test_invalid_mode(self):
        with pytest.raises(SystemExit):
            self.parse(["-pc", "d.parquet", "--mode", "invalid"])


# ---------------------------------------------------------------------------
# main() — end-to-end integration
# ---------------------------------------------------------------------------


class TestMain:
    def test_schema_prints_column_names(self, monkeypatch, tmp_path, capsys):
        pq = tmp_path / "s.parquet"
        pl.DataFrame({"alpha": [1, 2], "beta": ["x", "y"]}).write_parquet(pq)
        monkeypatch.setattr("sys.argv", ["prog", "-s", str(pq)])
        main()
        out = capsys.readouterr().out
        assert "alpha" in out
        assert "beta" in out

    def test_parquet2csv_creates_file(self, monkeypatch, tmp_path):
        pq = tmp_path / "data.parquet"
        out = tmp_path / "out.csv"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(pq)
        monkeypatch.setattr("sys.argv", ["prog", "-pc", str(pq), "-o", str(out)])
        main()
        assert out.exists()
        assert len(pl.read_csv(out)) == 3

    def test_csv2parquet_creates_file(self, monkeypatch, tmp_path):
        csv = tmp_path / "data.csv"
        out = tmp_path / "out.parquet"
        csv.write_text("x,y\n1,2\n3,4\n")
        monkeypatch.setattr("sys.argv", ["prog", "-cp", str(csv), "-o", str(out)])
        main()
        assert out.exists()
        assert len(pl.read_parquet(out)) == 2

    def test_parquet2csv_with_rename_and_date_flags(self, monkeypatch, tmp_path):
        pq = tmp_path / "data.parquet"
        out = tmp_path / "out.csv"
        pl.DataFrame({
            "isin": ["IE00B4L5Y983"],
            "date": pl.Series([datetime(2021, 1, 15)]).cast(pl.Datetime("us")),
            "close": [16.73],
        }).write_parquet(pq)
        monkeypatch.setattr("sys.argv", [
            "prog", "-pc", str(pq), "-o", str(out),
            "--rename", "isin:etf_isin",
            "--date-format", "date:iso",
            "--select", "isin,date,close",
        ])
        main()
        df = pl.read_csv(out)
        assert "etf_isin" in df.columns
        assert df["date"][0] == "2021-01-15T00:00:00.000000"

    def test_parquet2csv_with_rules_file(self, monkeypatch, tmp_path):
        pq = tmp_path / "data.parquet"
        out = tmp_path / "out.csv"
        rules = tmp_path / "rules.json"
        pl.DataFrame({
            "isin": ["A", "B"],
            "date": pl.Series([datetime(2021, 1, 15), datetime(2021, 6, 1)]).cast(pl.Datetime("us")),
            "close": [1.0, 2.0],
        }).write_parquet(pq)
        rules.write_text(json.dumps({
            "column_rules": [
                {"parquet_name": "isin", "csv_name": "etf_isin"},
                {"parquet_name": "date", "csv_name": "date", "date_format": "iso"},
            ],
            "select_columns": ["isin", "date", "close"],
        }))
        monkeypatch.setattr("sys.argv", [
            "prog", "-pc", str(pq), "-o", str(out), "--rules", str(rules),
        ])
        main()
        df = pl.read_csv(out)
        assert "etf_isin" in df.columns
        assert df["date"][0] == "2021-01-15T00:00:00.000000"
        assert "isin" not in df.columns

    def test_csv2parquet_streaming_mode(self, monkeypatch, tmp_path):
        csv = tmp_path / "data.csv"
        out = tmp_path / "out.parquet"
        csv.write_text("a,b\n1,2\n3,4\n5,6\n")
        monkeypatch.setattr("sys.argv", [
            "prog", "-cp", str(csv), "-o", str(out),
            "--mode", "streaming", "--chunk-size", "2",
        ])
        main()
        assert len(pl.read_parquet(out)) == 3

    def test_parquet2csv_semicolon_delimiter(self, monkeypatch, tmp_path):
        pq = tmp_path / "data.parquet"
        out = tmp_path / "out.csv"
        pl.DataFrame({"a": [1], "b": [2]}).write_parquet(pq)
        monkeypatch.setattr("sys.argv", [
            "prog", "-pc", str(pq), "-o", str(out), "-d", ";",
        ])
        main()
        first_line = out.read_text().split("\n")[0]
        assert ";" in first_line
