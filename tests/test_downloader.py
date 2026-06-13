"""Tests for the ``downloader`` module — hits the real ECCC API."""

from pathlib import Path

import pandas as pd
import pytest

from downloader import (
    _clean_column_names,
    download_climate_data,
    load_station_inventory,
)


# ---------------------------------------------------------------------------
# Unit tests – _clean_column_names
# ---------------------------------------------------------------------------


def test_clean_column_names_lowercases():
    df = pd.DataFrame(columns=["Name", "Station ID"])
    result = _clean_column_names(df)
    assert list(result.columns) == ["name", "station_id"]


def test_clean_column_names_replaces_spaces():
    df = pd.DataFrame(columns=["Max Temp (°C)", "Total Precip (mm)"])
    result = _clean_column_names(df)
    assert "max_temp_c" in result.columns
    assert "total_precip_mm" in result.columns


def test_clean_column_names_replaces_hyphens():
    df = pd.DataFrame(columns=["Data-Quality", "Longitude-x"])
    result = _clean_column_names(df)
    assert "data_quality" in result.columns
    assert "longitude_x" in result.columns


def test_clean_column_names_strips_special_chars():
    df = pd.DataFrame(columns=["Temp (°C)", "Speed (km/h)"])
    result = _clean_column_names(df)
    # parentheses and slash stripped; only a-z, 0-9, underscore survives
    assert "temp_c" in result.columns
    assert "speed_kmh" in result.columns


# ---------------------------------------------------------------------------
# Unit tests – load_station_inventory
# ---------------------------------------------------------------------------


def test_load_station_inventory_returns_dataframe():
    inv = load_station_inventory()
    assert isinstance(inv, pd.DataFrame)
    assert not inv.empty


def test_load_station_inventory_has_expected_columns():
    inv = load_station_inventory()
    expected = {"name", "province", "climate_id", "station_id", "latitude_decimal_degrees",
                "longitude_decimal_degrees", "elevation_m", "first_year", "last_year"}
    assert expected.issubset(inv.columns)


def test_load_station_inventory_custom_path():
    """Accept a user-supplied path."""
    default = Path(__file__).parent.parent / "data" / "Station Inventory EN.csv"
    inv = load_station_inventory(default)
    assert isinstance(inv, pd.DataFrame)
    assert not inv.empty


# ---------------------------------------------------------------------------
# Integration tests – download_climate_data  (hits real API)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("tf", ["daily", "monthly"])
def test_download_climate_data_daily_and_monthly(tf):
    """One year of daily/monthly data for a small coastal station."""
    df = download_climate_data(889, 2023, 2023, timeframe=tf)  # type: ignore[arg-type]
    assert isinstance(df, pd.DataFrame)
    assert not df.empty, f"No rows returned for timeframe={tf}"
    # columns should be clean
    assert all(" " not in c and c.islower() for c in df.columns), (
        f"Unclean columns: {df.columns.tolist()}"
    )


def test_download_climate_data_hourly():
    """A single month of hourly data (fast)."""
    df = download_climate_data(889, 2023, 2023, timeframe="hourly")
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


@pytest.mark.parametrize("tz", ["ltc", "utc"])
def test_download_climate_data_timezone(tz):
    """Hourly data with both timezone options."""
    df = download_climate_data(889, 2024, 2024, timeframe="hourly", timezone=tz)  # type: ignore[arg-type]
    assert isinstance(df, pd.DataFrame)
    assert not df.empty


def test_download_climate_data_multiple_years():
    """Two years of daily data concatenated."""
    df = download_climate_data(889, 2022, 2023, timeframe="daily")
    years = df["year"].unique()
    assert 2022 in years
    assert 2023 in years


def test_download_climate_data_unknown_station():
    """A station_id with no data returns an empty DataFrame."""
    df = download_climate_data(999999, 2000, 2000, timeframe="daily")
    assert isinstance(df, pd.DataFrame)
    assert df.empty


# ---------------------------------------------------------------------------
# Edge-case validation tests (no API call)
# ---------------------------------------------------------------------------


def test_invalid_timeframe_raises():
    with pytest.raises(ValueError, match="timeframe"):
        download_climate_data(889, 2020, 2020, timeframe="weekly")  # type: ignore[arg-type]


def test_start_year_after_end_year_raises():
    with pytest.raises(ValueError, match="start_year"):
        download_climate_data(889, 2023, 2020)
