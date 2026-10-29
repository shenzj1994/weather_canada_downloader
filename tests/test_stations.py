"""Tests for canada_weather.stations."""

# pylint: disable=missing-function-docstring,missing-class-docstring

from datetime import datetime

import pandas as pd
import pytest

from canada_weather import stations
from canada_weather import find_stations, get_station, read_station_inventory

SAMPLE = (
    "Modified Date: 2030-01-01 00:00 UTC\n"
    '"disclaimer line 1"\n'
    '"disclaimer line 2"\n'
    '"Name","Province","Climate ID","Station ID","Latitude (Decimal Degrees)",'
    '"Longitude (Decimal Degrees)","First Year","Last Year"\n'
    '" Test Station ","TEST","123ABC","123","49.0","-123.0","2000","2010"\n'
)


def test_bundled_inventory_loads_from_any_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)  # the old relative default path broke here
    inv = read_station_inventory()
    assert len(inv) > 8000
    assert stations.bundled_inventory_path().exists()


def test_columns_are_clean_and_typed():
    inv = read_station_inventory()
    for col in inv.columns:
        assert col == col.lower() and " " not in col
    assert {"climate_id", "station_id", "dly_first_year", "mly_last_year"} <= set(inv.columns)
    assert inv["station_id"].dtype == "Int64"
    assert inv["first_year"].dtype == "Int64"
    assert pd.api.types.is_float_dtype(inv["elevation_m"])


def test_returned_frame_is_a_copy():
    inv = read_station_inventory()
    inv["name"] = "CHANGED"
    assert (read_station_inventory()["name"] != "CHANGED").all()


def test_custom_path_strips_whitespace(tmp_path):
    path = tmp_path / "inv.csv"
    path.write_text(SAMPLE, encoding="utf-8")
    df = read_station_inventory(path)
    assert df.iloc[0]["name"] == "Test Station"
    assert df.iloc[0]["climate_id"] == "123ABC"


def test_modified_date(tmp_path):
    path = tmp_path / "inv.csv"
    path.write_text(SAMPLE, encoding="utf-8")
    assert stations.inventory_modified_date(path) == datetime(2030, 1, 1)
    assert stations.inventory_modified_date() is not None


def test_refreshed_inventory_takes_priority(tmp_path, monkeypatch):
    monkeypatch.setenv("CANADA_WEATHER_CACHE", str(tmp_path))
    (tmp_path / stations.INVENTORY_FILENAME).write_text(SAMPLE, encoding="utf-8")
    assert len(read_station_inventory()) == 1


def test_refresh_downloads_and_saves(tmp_path, monkeypatch):
    from .conftest import FakeResponse, FakeSession  # pylint: disable=import-outside-toplevel
    from canada_weather import _http  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(_http, "make_session", lambda **_: FakeSession(lambda p: FakeResponse(SAMPLE)))
    path = stations.refresh_station_inventory(tmp_path / "inv.csv")
    assert path.read_text(encoding="utf-8") == SAMPLE


def test_refresh_rejects_html(tmp_path, monkeypatch):
    from .conftest import FakeResponse, FakeSession  # pylint: disable=import-outside-toplevel
    from canada_weather import _http  # pylint: disable=import-outside-toplevel

    monkeypatch.setattr(_http, "make_session",
                        lambda **_: FakeSession(lambda p: FakeResponse("<html></html>")))
    with pytest.raises(ValueError):
        stations.refresh_station_inventory(tmp_path / "inv.csv")
    assert not (tmp_path / "inv.csv").exists()


def test_get_station():
    row = get_station(1108395)
    assert row["station_id"] == 51442
    assert get_station(station_id=889)["climate_id"] == "1108447"
    with pytest.raises(ValueError, match="not found"):
        get_station("nope")


def test_find_stations_by_name_and_timeframe():
    df = find_stations("vancouver int", timeframe="hourly", years=(2020, 2024))
    assert "1108395" in set(df["climate_id"])
    assert "1108447" not in set(df["climate_id"])  # ended in 2013


def test_find_stations_near_point():
    df = find_stations(near=(49.19, -123.18), radius_km=5, timeframe="daily")
    assert not df.empty
    assert df["distance_km"].is_monotonic_increasing
    assert (df["distance_km"] <= 5).all()


def test_find_stations_by_province():
    df = find_stations(province="prince edward island")
    assert not df.empty
    assert (df["province"] == "PRINCE EDWARD ISLAND").all()
