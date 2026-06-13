"""Tests for the ``station_inventory_reader`` module."""

from pathlib import Path

import pandas as pd
import pytest

from station_inventory_reader import read_station_inventory


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def csv_path() -> Path:
    """Path to the real station inventory CSV shipped with the project."""
    return Path(__file__).parent.parent / "data" / "Station Inventory EN.csv"


# ---------------------------------------------------------------------------
# Tests – read_station_inventory
# ---------------------------------------------------------------------------


def test_returns_dataframe(csv_path):
    df = read_station_inventory(csv_path)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_skiprows_removes_disclaimer(csv_path):
    """The first few data rows (after disclaimers) should parse correctly."""
    df = read_station_inventory(csv_path)
    # Known station from the CSV: "ACTIVE PASS" (row 3 of data after 2 disclaimers)
    first = df.iloc[0]
    assert first["name"] == "ACTIVE PASS"
    assert first["province"] == "BRITISH COLUMBIA"


def test_column_names_are_cleaned(csv_path):
    df = read_station_inventory(csv_path)
    for col in df.columns:
        assert col.islower(), f"Column {col!r} is not lower-case"
        assert " " not in col, f"Column {col!r} still contains spaces"
        assert all(
            c.isalnum() or c == "_" for c in col
        ), f"Column {col!r} contains special characters"


def test_expected_columns_present(csv_path):
    df = read_station_inventory(csv_path)
    expected = {
        "name",
        "province",
        "climate_id",
        "station_id",
        "longitude_decimal_degrees",
        "latitude_decimal_degrees",
        "elevation_m",
        "first_year",
        "last_year",
        "hly_first_year",
        "hly_last_year",
        "dly_first_year",
        "dly_last_year",
        "mly_first_year",
        "mly_last_year",
    }
    missing = expected - set(df.columns)
    assert not missing, f"Missing columns: {missing}"


def test_station_id_is_integer(csv_path):
    df = read_station_inventory(csv_path)
    # Station ID is nullable Int64; check a known value
    assert df["station_id"].dtype == "Int64"
    # Vancouver Int'l A (station_id=889 in 1018620 row — but there are many matches;
    # just verify the column has sensible values)
    assert df["station_id"].notna().sum() > 0


def test_elevation_is_float(csv_path):
    df = read_station_inventory(csv_path)
    assert pd.api.types.is_float_dtype(df["elevation_m"])


def test_year_columns_are_integer(csv_path):
    df = read_station_inventory(csv_path)
    for col in ["first_year", "last_year"]:
        assert df[col].dtype == "Int64", f"{col} is not Int64"


def test_string_columns_are_stripped(csv_path):
    df = read_station_inventory(csv_path)
    # If stripping worked, no leading/trailing whitespace remains
    for col in df.select_dtypes(include="object").columns:
        assert not df[col].str.match(r"^\s|\s$").any(), (
            f"Column {col!r} has unstripped whitespace"
        )


def test_default_path_matches_data_dir():
    """Calling without arguments uses the default path: data/Station Inventory EN.csv"""
    df = read_station_inventory()
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_custom_path_csv_in_memory():
    """Passing a user-supplied file path."""
    p = Path(__file__).parent / "_test_inventory.csv"
    try:
        # Must match the same shape as the real file:
        # 1 modified-date line + 2 disclaimers + header
        csv_lines = (
            "Modified Date: 2026-01-01\n"
            '"disclaimer line 1"\n'
            '"disclaimer line 2"\n'
            '"Name","Province","Station ID"\n'
            '"Test Station","TEST","123"\n'
        )
        p.write_text(csv_lines, encoding="utf-8")
        df = read_station_inventory(p)
        assert len(df) == 1
        assert df.iloc[0]["name"] == "Test Station"
        assert df.iloc[0]["province"] == "TEST"
    finally:
        if p.exists():
            p.unlink()
