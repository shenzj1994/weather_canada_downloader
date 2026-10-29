"""Live tests against the ECCC API. Run with ``pytest -m integration``."""

# pylint: disable=missing-function-docstring,missing-class-docstring

import pandas as pd
import pytest

from canada_weather import download_climate_data

pytestmark = pytest.mark.integration

VANCOUVER = "1108395"


def test_daily_has_real_values():
    df = download_climate_data(VANCOUVER, 2023, 2023)
    assert len(df) == 365
    assert df["max_temp_c"].notna().mean() > 0.9
    assert df["datetime"].min() == pd.Timestamp("2023-01-01")


def test_hourly_utc():
    df = download_climate_data(VANCOUVER, 2024, 2024, timeframe="hourly", timezone="utc")
    assert len(df) > 8000
    assert df["temp_c"].notna().mean() > 0.9


def test_monthly_is_not_duplicated():
    df = download_climate_data("1108447", 2000, 2001, timeframe="monthly")
    assert len(df) == 24
    assert not df.duplicated(subset=["year", "month"]).any()


def test_unknown_station_is_empty():
    with pytest.warns(UserWarning):
        df = download_climate_data(start_year=2000, end_year=2000, station_id=999999)
    assert df.empty
