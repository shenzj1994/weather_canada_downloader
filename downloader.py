"""
Download climate data from Environment and Climate Change Canada.

Provides a simple interface to download historical weather observations
from the Government of Canada's climate data portal.
"""

from __future__ import annotations

from io import StringIO
from pathlib import Path
from typing import Literal

import pandas as pd
import requests

from _column_config import EXPECTED_COLUMNS


DATA_DIR = Path(__file__).parent / "data"
STATION_INVENTORY = DATA_DIR / "Station Inventory EN.csv"


def _clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase column names and replace spaces/hyphens with underscores."""
    df = df.copy()
    df.columns = (
        df.columns.str.lower()
        .str.replace(r"[\s\-]+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


def load_station_inventory(
    path: str | Path = STATION_INVENTORY,
) -> pd.DataFrame:
    """Load the station inventory CSV and return a cleaned DataFrame.

    Parameters
    ----------
    path : str | Path
        Path to the ``Station Inventory EN.csv`` file.
        Defaults to the one shipped with the package under ``data/``.

    Returns
    -------
    pd.DataFrame
        Station inventory with cleaned (lowercase, underscore) column names.
    """
    df = pd.read_csv(path, skiprows=3)
    return _clean_column_names(df)


def download_climate_data(
    station_id: int,
    start_year: int,
    end_year: int,
    timeframe: Literal["hourly", "daily", "monthly"] = "daily",
    timezone: Literal["ltc", "utc"] = "ltc",
) -> pd.DataFrame:
    """Download climate data for a single station as a pandas DataFrame.

    Iterates over every month in the requested year range and fetches CSV
    data from the Environment and Climate Change Canada bulk data endpoint.
    All DataFrames are concatenated and returned as one.

    Parameters
    ----------
    station_id : int
        The numeric Station ID from the station inventory (the ``Station ID``
        column).  For example, the Vancouver International Airport station
        has ``station_id=889``.
    start_year : int
        First year of data to include (inclusive).
    end_year : int
        Last year of data to include (inclusive).
    timeframe : {"hourly", "daily", "monthly"}
        Temporal resolution of the data.
    timezone : {"ltc", "utc"}
        Time zone for hourly data.  ``"ltc"`` = local time (default),
        ``"utc"`` = Coordinated Universal Time (sent as ``&time=UTC`` to
        the API).  Ignored for daily/monthly.

    Returns
    -------
    pd.DataFrame
        A single DataFrame with all requested months concatenated.  Column
        names are lower-case with underscores instead of spaces.

    Examples
    --------
    >>> from downloader import download_climate_data
    >>> df = download_climate_data(889, 2020, 2021)
    >>> df = download_climate_data(889, 2020, 2021, timeframe="hourly")
    """
    timeframe_code = {"hourly": 1, "daily": 2, "monthly": 3}

    if timeframe not in timeframe_code:
        raise ValueError(
            f"`timeframe` must be one of {list(timeframe_code.keys())}, "
            f"got {timeframe!r}"
        )
    if start_year > end_year:
        raise ValueError(
            f"`start_year` ({start_year}) must be <= `end_year` ({end_year})"
        )

    tf = timeframe_code[timeframe]
    frames: list[pd.DataFrame] = []
    base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            url = (
                f"{base_url}?format=csv"
                f"&stationID={station_id}"
                f"&Year={year}"
                f"&Month={month}"
                f"&Day=1"
                f"&timeframe={tf}"
                f"&submit=Download+Data"
            )
            if timezone == "utc":
                url += "&time=UTC"

            resp = requests.get(url, timeout=60)
            resp.raise_for_status()

            # The server may return HTML (e.g. for unknown stations) instead of CSV
            if not resp.text.startswith('"'):
                continue

            chunk = pd.read_csv(StringIO(resp.text))
            if not chunk.empty:
                frames.append(chunk)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = _clean_column_names(result)

    # Validate that columns match the expected schema for this timeframe
    expected = EXPECTED_COLUMNS[timeframe]
    # Hourly data has timezone-dependent column names
    if timeframe == "hourly" and timezone == "utc":
        expected = [
            col.replace("datetime_lst", "datetime_utc").replace("time_lst", "time_utc")
            for col in expected
        ]
    if list(result.columns) != expected:
        raise ValueError(
            f"Column mismatch for timeframe={timeframe!r}.\n"
            f"Got:      {list(result.columns)}\n"
            f"Expected: {expected}"
        )

    return result
