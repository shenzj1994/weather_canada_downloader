"""Tests for canada_weather._columns."""

# pylint: disable=missing-function-docstring,missing-class-docstring

import pandas as pd

from canada_weather._columns import clean_column_names, coerce_dtypes, measurement_columns


def test_clean_column_names():
    df = pd.DataFrame(columns=["Max Temp (°C)", "Data-Quality", "Speed (km/h)",
                               "Date/Time (LST)", "Dir of Max Gust (10's deg)", "Rel Hum (%)"])
    assert list(clean_column_names(df).columns) == [
        "max_temp_c", "data_quality", "speed_kmh", "datetime_lst",
        "dir_of_max_gust_10s_deg", "rel_hum_",
    ]


def test_coerce_dtypes_handles_text_columns():
    df = pd.DataFrame({
        "climate_id": ["116FRMN"], "year": ["2020"], "datetime": ["2020-01-01"],
        "max_temp_c": [""], "hmdx": ["31"], "max_temp_flag": ["M"],
    }, dtype=str)
    out = coerce_dtypes(df)
    assert out["climate_id"].iloc[0] == "116FRMN"
    assert out["year"].dtype == "Int64"
    assert out["max_temp_c"].dtype == "float64"
    assert out["hmdx"].iloc[0] == 31.0
    assert out["max_temp_flag"].dtype == "string"


def test_measurement_columns_exclude_ids_and_flags():
    df = pd.DataFrame(columns=["climate_id", "year", "temp_c", "temp_flag", "flag", "weather"])
    assert measurement_columns(df) == ["temp_c", "weather"]
