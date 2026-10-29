"""Column handling shared across the package.

* :func:`clean_column_names` normalises ECCC column headers
  (``"Max Temp (°C)"`` → ``"max_temp_c"``).
* :data:`EXPECTED_COLUMNS` is the schema observed for each timeframe of the
  bulk-data endpoint (verified against the live API, October 2026).
* :func:`coerce_dtypes` turns the raw CSV columns into proper dtypes.
"""

from __future__ import annotations

import pandas as pd


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names, turn spaces/hyphens into underscores and drop
    every other non-alphanumeric character."""
    df = df.copy()
    df.columns = (
        df.columns.str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


# Columns that identify a row rather than carry a measurement.
ID_COLUMNS = frozenset({
    "longitude_x",
    "latitude_y",
    "station_name",
    "climate_id",
    "datetime",
    "datetime_lst",
    "datetime_utc",
    "year",
    "month",
    "day",
    "time_lst",
    "time_utc",
})

# Non-numeric columns that are not flags.
_TEXT_COLUMNS = frozenset({"station_name", "climate_id", "data_quality", "weather"})
_INT_COLUMNS = ("year", "month", "day")
_DATETIME_COLUMNS = ("datetime", "datetime_lst", "datetime_utc")


def is_flag_column(col: str) -> bool:
    """Return True for ECCC data-quality flag columns."""
    return col == "flag" or col.endswith("_flag")


def measurement_columns(df: pd.DataFrame) -> list[str]:
    """Columns holding observed values (not identifiers, flags or quality codes)."""
    return [
        c for c in df.columns
        if c not in ID_COLUMNS and not is_flag_column(c) and c != "data_quality"
    ]


def coerce_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Give the raw CSV columns proper dtypes.

    * measurements and coordinates → ``float64`` (blank → NaN)
    * ``year``/``month``/``day`` → nullable ``Int64``
    * ``datetime*`` → ``datetime64``
    * ids, names, flags and weather descriptions → ``string``
    """
    df = df.copy()
    for col in df.columns:
        if col in _DATETIME_COLUMNS:
            df[col] = pd.to_datetime(df[col], errors="coerce")
        elif col in _INT_COLUMNS:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        elif col in _TEXT_COLUMNS or is_flag_column(col) or col.startswith("time_"):
            df[col] = df[col].astype("string")
        elif not pd.api.types.is_numeric_dtype(df[col]) or df[col].dtype == bool:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
        else:
            df[col] = df[col].astype("float64")
    return df


def coerce_numeric_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """Backward-compatible alias for :func:`coerce_dtypes`."""
    return coerce_dtypes(df)


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


def expected_columns(timeframe: str, timezone: str = "lst") -> set[str]:
    """Expected cleaned column names for a timeframe (and hourly time zone)."""
    cols = set(EXPECTED_COLUMNS[timeframe])
    if timeframe == "hourly" and timezone == "utc":
        cols = {c.replace("_lst", "_utc") for c in cols}
    return cols
