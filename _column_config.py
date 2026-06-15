"""
Expected column names for each timeframe, derived from the Environment Canada
bulk data API (observed using station 889, Vancouver Int'l A).

Also provides ``clean_column_names()`` — a utility shared across the codebase
to normalise column names (lowercase, spaces/hyphens → underscores,
non-alphanumeric chars removed).
"""

import pandas as pd


# Numeric columns that must be coerced from object → float before writing Parquet.
# Derived from the union of all numeric-measurement columns across timeframes.
_NUMERIC_SUFFIXES = (
    "_x", "_y",            # longitude, latitude
    "_c",                  # temperature (°C)
    "_mm",                 # millimetres (rain, precip)
    "_cm",                 # centimetres (snow, snow on ground)
    "_kmh",                # km/h (wind/gust speed)
    "_deg",                # degrees (wind direction)
    "_kpa",                # kPa (pressure)
    "_days",               # heat/cool degree days
    "_days_c",             # heat/cool degree days (°C variant)
    "_",                   # rel_hum_ (relative humidity)
)
_ALWAYS_NUMERIC = frozenset({"year", "month", "day", "climate_id"})


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names and replace spaces/hyphens with underscores."""
    df = df.copy()
    df.columns = (
        df.columns.str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def coerce_numeric_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Coerce known numeric columns to float64 so Parquet writes succeed.

    Some stations return empty strings or other non-numeric placeholders in
    measurement columns.  After ``pd.concat``, those columns end up as
    ``object`` dtype, which PyArrow refuses to write.  This function converts
    them safely via ``pd.to_numeric(..., errors="coerce")``.
    """
    df = df.copy()
    for col in df.columns:
        if col in _ALWAYS_NUMERIC or col.endswith(_NUMERIC_SUFFIXES):
            if df[col].dtype == object:
                df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    return df


# Reusable column groups shared across timeframes.
_HEADER = [
    "longitude_x",
    "latitude_y",
    "station_name",
    "climate_id",
    "year",
    "month",
]

_DT_MEAN = [
    "datetime",
    "mean_temp_c",
    "mean_temp_flag",
]

_PRECIP = [
    "total_rain_mm",
    "total_rain_flag",
    "total_snow_cm",
    "total_snow_flag",
    "total_precip_mm",
    "total_precip_flag",
]

_WIND_GUST = [
    "dir_of_max_gust_10s_deg",
    "dir_of_max_gust_flag",
    "spd_of_max_gust_kmh",
    "spd_of_max_gust_flag",
]

EXPECTED_COLUMNS: dict[str, list[str]] = {
    "daily": [
        *_HEADER,
        *_DT_MEAN,
        "day",
        "data_quality",
        "max_temp_c",
        "max_temp_flag",
        "min_temp_c",
        "min_temp_flag",
        "heat_deg_days_c",
        "heat_deg_days_flag",
        "cool_deg_days_c",
        "cool_deg_days_flag",
        *_PRECIP,
        "snow_on_grnd_cm",
        "snow_on_grnd_flag",
        *_WIND_GUST,
    ],
    "hourly": [
        *_HEADER,
        "datetime_lst",
        "day",
        "time_lst",
        "flag",
        "temp_c",
        "temp_flag",
        "dew_point_temp_c",
        "dew_point_temp_flag",
        "rel_hum_",
        "rel_hum_flag",
        "precip_amount_mm",
        "precip_amount_flag",
        "wind_dir_10s_deg",
        "wind_dir_flag",
        "wind_spd_kmh",
        "wind_spd_flag",
        "visibility_km",
        "visibility_flag",
        "stn_press_kpa",
        "stn_press_flag",
        "hmdx",
        "hmdx_flag",
        "wind_chill",
        "wind_chill_flag",
        "weather",
    ],
    "monthly": [
        *_HEADER,
        *_DT_MEAN,
        "mean_max_temp_c",
        "mean_max_temp_flag",
        "mean_min_temp_c",
        "mean_min_temp_flag",
        "extr_max_temp_c",
        "extr_max_temp_flag",
        "extr_min_temp_c",
        "extr_min_temp_flag",
        *_PRECIP,
        "snow_grnd_last_day_cm",
        "snow_grnd_last_day_flag",
        *_WIND_GUST,
    ],
}
