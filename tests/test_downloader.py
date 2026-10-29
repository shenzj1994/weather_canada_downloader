"""Offline tests for canada_weather.downloader (HTTP is faked; see conftest.py)."""

# pylint: disable=missing-function-docstring,missing-class-docstring,redefined-outer-name



from datetime import date

import pandas as pd
import pytest

from canada_weather import download_climate_data, download_many
from canada_weather import downloader as dl
from canada_weather._columns import expected_columns

from .conftest import FakeResponse, FakeSession, daily_csv

VANCOUVER = "1108395"  # VANCOUVER INTL A, station 51442, 2013-present
VANCOUVER_OLD = "1108447"  # VANCOUVER INT'L A, station 889, 1937-2013


# --- request construction --------------------------------------------------


def test_daily_makes_one_request_per_year(fake_session):
    download_climate_data(VANCOUVER, 2020, 2022, session=fake_session)
    assert [(c["Year"], c["Month"]) for c in fake_session.calls] == [(2020, 1), (2021, 1), (2022, 1)]
    assert {c["stationID"] for c in fake_session.calls} == {51442}
    assert {c["timeframe"] for c in fake_session.calls} == {2}


def test_hourly_makes_one_request_per_month(fake_session):
    download_climate_data(VANCOUVER, 2020, 2020, timeframe="hourly", session=fake_session)
    assert [c["Month"] for c in fake_session.calls] == list(range(1, 13))
    assert all("time" not in c for c in fake_session.calls)


def test_utc_adds_time_param_and_renames_columns(fake_session):
    df = download_climate_data(VANCOUVER, 2020, 2020, timeframe="hourly",
                               timezone="utc", session=fake_session)
    assert all(c["time"] == "UTC" for c in fake_session.calls)
    assert {"datetime_utc", "time_utc"} <= set(df.columns)


def test_ltc_is_alias_for_lst(fake_session):
    df = download_climate_data(VANCOUVER, 2020, 2020, timeframe="hourly",
                               timezone="ltc", session=fake_session)
    assert "datetime_lst" in df.columns


def test_monthly_makes_a_single_request_and_filters_years(fake_session):
    df = download_climate_data(VANCOUVER_OLD, 2000, 2001, timeframe="monthly", session=fake_session)
    assert len(fake_session.calls) == 1
    assert sorted(df["year"].unique()) == [2000, 2001]
    assert len(df) == 24
    assert not df.duplicated(subset=["year", "month"]).any()


def test_future_periods_are_not_requested(fake_session, monkeypatch):
    class FixedDate(date):
        @classmethod
        def today(cls):
            return cls(2025, 3, 15)

    monkeypatch.setattr(dl, "date", FixedDate)
    download_climate_data(VANCOUVER, 2025, 2026, timeframe="hourly", session=fake_session)
    assert [(c["Year"], c["Month"]) for c in fake_session.calls] == [(2025, 1), (2025, 2), (2025, 3)]


# --- station record clipping --------------------------------------------


def test_years_are_clipped_to_station_record(fake_session):
    # Station 889 has daily data only up to 2013.
    download_climate_data(VANCOUVER_OLD, 2012, 2020, session=fake_session)
    assert [c["Year"] for c in fake_session.calls] == [2012, 2013]


def test_no_overlap_returns_empty_with_warning(fake_session):
    with pytest.warns(UserWarning, match="no daily data"):
        df = download_climate_data(VANCOUVER_OLD, 2020, 2021, session=fake_session)
    assert df.empty
    assert fake_session.calls == []
    assert set(df.columns) == expected_columns("daily")


def test_clip_can_be_disabled(fake_session):
    download_climate_data(VANCOUVER_OLD, 2020, 2020, clip_to_record=False, session=fake_session)
    assert len(fake_session.calls) == 1


# --- output cleaning ---------------------------------------------------


def test_daily_output_columns_and_types(fake_session):
    df = download_climate_data(VANCOUVER, 2020, 2020, session=fake_session)
    assert len(df) == 366
    assert set(df.columns) == expected_columns("daily")
    assert pd.api.types.is_datetime64_any_dtype(df["datetime"])
    assert df["max_temp_c"].dtype == "float64"
    assert df["year"].dtype == "Int64"
    assert df["climate_id"].iloc[0] == VANCOUVER
    assert df["total_rain_flag"].dtype == "string"


def test_hourly_text_and_numeric_columns(fake_session):
    df = download_climate_data(VANCOUVER, 2020, 2020, timeframe="hourly", session=fake_session)
    for col in ("temp_c", "rel_hum_", "hmdx", "wind_chill", "stn_press_kpa"):
        assert df[col].dtype == "float64", col
    assert df["weather"].dtype == "string"


def test_blank_rows_are_dropped():
    session = FakeSession(lambda p: FakeResponse(daily_csv(int(p["Year"]), blank_from=(7, 1))))
    df = download_climate_data(VANCOUVER, 2020, 2020, session=session)
    assert len(df) == 182  # Jan 1 - Jun 30 in a leap year
    kept = download_climate_data(VANCOUVER, 2020, 2020, session=session, drop_empty=False)
    assert len(kept) == 366


def test_coerce_types_false_keeps_text(fake_session):
    df = download_climate_data(VANCOUVER, 2020, 2020, session=fake_session, coerce_types=False)
    assert not pd.api.types.is_float_dtype(df["max_temp_c"])


def test_schema_drift_warns_or_raises():
    def handler(params):
        text = daily_csv(int(params["Year"])).replace('"Max Temp (°C)"', '"Maximum Temp (°C)"', 1)
        return FakeResponse(text)

    with pytest.warns(UserWarning, match="max_temp_c"):
        download_climate_data(VANCOUVER, 2020, 2020, session=FakeSession(handler))
    with pytest.raises(ValueError, match="max_temp_c"):
        download_climate_data(VANCOUVER, 2020, 2020, session=FakeSession(handler), strict=True)


def test_html_response_is_treated_as_no_data(fake_session):
    with pytest.warns(UserWarning, match="Station ID"):
        df = download_climate_data(start_year=2000, end_year=2000, station_id=999999,
                                   session=fake_session)
    assert df.empty


# --- caching ---------------------------------------------------------------


def test_cache_reuses_completed_periods(fake_session, tmp_path):
    download_climate_data(VANCOUVER, 2020, 2021, session=fake_session, cache=tmp_path)
    assert len(fake_session.calls) == 2
    download_climate_data(VANCOUVER, 2020, 2021, session=fake_session, cache=tmp_path)
    assert len(fake_session.calls) == 2  # served from disk
    assert len(list(tmp_path.rglob("*.csv"))) == 2


def test_current_year_is_not_cached(fake_session, tmp_path):
    year = date.today().year
    download_climate_data(VANCOUVER, year, year, session=fake_session, cache=tmp_path)
    assert not list(tmp_path.rglob("*.csv"))


# --- input validation --------------------------------------------------


@pytest.mark.parametrize("cid", [1108395, " 1108395 ", "1108395"])
def test_climate_id_is_normalised(fake_session, cid):
    df = download_climate_data(cid, 2020, 2020, session=fake_session)
    assert not df.empty


def test_unknown_climate_id_raises():
    with pytest.raises(ValueError, match="not found"):
        download_climate_data("0000000", 2020, 2020)


@pytest.mark.parametrize(
    "kwargs, exc, match",
    [
        ({"climate_id": VANCOUVER, "start_year": 2020, "end_year": 2020, "timeframe": "weekly"},
         ValueError, "timeframe"),
        ({"climate_id": VANCOUVER, "start_year": 2023, "end_year": 2020}, ValueError, "start_year"),
        ({"climate_id": VANCOUVER, "start_year": 2020, "end_year": 2020, "timezone": "pst"},
         ValueError, "timezone"),
        ({"climate_id": VANCOUVER, "start_year": 2020}, TypeError, "end_year"),
        ({"start_year": 2020, "end_year": 2020}, TypeError, "Either"),
        ({"climate_id": VANCOUVER, "station_id": 51442, "start_year": 2020, "end_year": 2020},
         TypeError, "only one"),
        ({"climate_id": VANCOUVER, "start_year": 2020, "end_year": 2020, "max_workers": 0},
         ValueError, "max_workers"),
    ],
)
def test_invalid_arguments(kwargs, exc, match):
    with pytest.raises(exc, match=match):
        download_climate_data(**kwargs)


def test_station_id_warns(fake_session):
    with pytest.warns(UserWarning, match="internal numbering"):
        df = download_climate_data(start_year=2020, end_year=2020, station_id=51442,
                                   session=fake_session)
    assert not df.empty


# --- download_many -----------------------------------------------------


def test_download_many_combines_and_reports_failures(monkeypatch, fake_session):
    monkeypatch.setattr(dl, "make_session", lambda **_: fake_session)
    with pytest.warns(UserWarning, match="0000000"):
        df = download_many([VANCOUVER, "0000000"], 2020, 2020)
    assert set(df["climate_id"]) == {VANCOUVER}
    assert list(df.attrs["failed"]) == ["0000000"]


def test_download_many_raise(monkeypatch, fake_session):
    monkeypatch.setattr(dl, "make_session", lambda **_: fake_session)
    with pytest.raises(ValueError):
        download_many(["0000000"], 2020, 2020, on_error="raise")
