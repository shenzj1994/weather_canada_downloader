"""
Download climate data from Environment and Climate Change Canada.

Provides a simple interface to download historical weather observations
from the Government of Canada's climate data portal.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import warnings
from io import StringIO
from typing import Literal

import pandas as pd
import requests

from _column_config import EXPECTED_COLUMNS, clean_column_names
from station_inventory_reader import read_station_inventory


def _lookup_station_id(climate_id: str) -> int:
    """Look up the internal Station ID for a given Climate ID.

    The Climate ID is the 7-digit official identifier (e.g. ``"1108447"``
    for Vancouver Int'l A).  The Station ID is an internal database number
    that the bulk-data API requires.

    Parameters
    ----------
    climate_id : str
        7-digit Climate ID from the station inventory.

    Returns
    -------
    int
        Corresponding internal Station ID.

    Raises
    ------
    ValueError
        If the Climate ID is not found in the station inventory or matches
        multiple stations.
    """
    inventory = read_station_inventory()
    matches = inventory[inventory["climate_id"] == climate_id]

    if matches.empty:
        raise ValueError(
            f"Climate ID {climate_id!r} not found in the station inventory. "
            f"Check the climate_id or use a station_id directly."
        )

    if len(matches) > 1:
        raise ValueError(
            f"Climate ID {climate_id!r} matches multiple stations in the "
            f"inventory. This should not happen — the Climate ID is supposed "
            f"to be a unique identifier."
        )

    return int(matches.iloc[0]["station_id"])


# pylint: disable=too-many-arguments,too-many-locals,too-many-branches
def download_climate_data(
    climate_id: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    *,
    station_id: int | None = None,
    timeframe: Literal["hourly", "daily", "monthly"] = "daily",
    timezone: Literal["ltc", "utc"] = "ltc",
    max_workers: int = 12,
) -> pd.DataFrame:
    """Download climate data for a single station as a pandas DataFrame.

    Fetches CSV data from the Environment and Climate Change Canada bulk
    data endpoint and concatenates all chunks into one DataFrame.
    For ``timeframe="daily"``, the downloader requests one file per year
    using ``Month=1`` and ``Day=1`` because the API returns the entire year.
    For ``"hourly"`` and ``"monthly"``, it iterates through all months.

    You must provide **either** ``climate_id`` (the 7-digit official
    identifier such as ``"1108447"``) **or** ``station_id`` (the internal
    database number).  Using ``climate_id`` is recommended because the
    Station ID is an internal numbering system subject to change without
    notice.  If you pass ``station_id`` a warning will be emitted.

    Parameters
    ----------
    climate_id : str, optional
        The 7-digit official Climate ID from the station inventory (e.g.
        ``"1108447"`` for Vancouver Int'l A).  This is the recommended
        way to identify a station.
    start_year : int, optional
        First year of data to include (inclusive).
    end_year : int, optional
        Last year of data to include (inclusive).
    station_id : int, optional (keyword-only)
        The internal Station ID from the station inventory (the ``Station
        ID`` column).  For example, Vancouver International Airport has
        ``station_id=889``.  **Not recommended** — use ``climate_id``
        instead.
    timeframe : {"hourly", "daily", "monthly"}
        Temporal resolution of the data.  For ``"daily"``, one request is
        made per year (``Month=1``, ``Day=1``) and the API returns the full
        year.  ``"hourly"`` and ``"monthly"`` fetch each month.
    timezone : {"ltc", "utc"}
        Time zone for hourly data.  ``"ltc"`` = local time (default),
        ``"utc"`` = Coordinated Universal Time (sent as ``&time=UTC`` to
        the API).  Ignored for daily/monthly.
    max_workers : int
        Maximum number of concurrent month downloads. Use ``1`` to disable
        concurrency. Defaults to ``12``.

    Returns
    -------
    pd.DataFrame
        A single DataFrame with all requested months concatenated.  Column
        names are lower-case with underscores instead of spaces.

    Examples
    --------
    >>> from downloader import download_climate_data
    >>> df = download_climate_data("1108447", 2020, 2021)
    >>> df = download_climate_data("1108447", 2020, 2021, timeframe="hourly")
    >>> # Using station_id emits a warning
    >>> df = download_climate_data(start_year=2020, end_year=2021, station_id=889)
    """
    timeframe_code = {"hourly": 1, "daily": 2, "monthly": 3}

    if station_id is None and climate_id is None:
        raise TypeError(
            "Either `station_id` or `climate_id` must be provided."
        )
    if station_id is not None and climate_id is not None:
        raise TypeError(
            "Provide only one of `station_id` or `climate_id`, not both."
        )

    if climate_id is not None:
        station_id = _lookup_station_id(climate_id)
    else:
        warnings.warn(
            "Station ID is an internal numbering system and may be subject "
            "to change without notice. Use `climate_id` (the 7-digit "
            "official Climate ID) instead.",
            UserWarning,
            stacklevel=2,
        )

    if start_year is None or end_year is None:
        raise TypeError(
            "`start_year` and `end_year` are required."
        )

    if timeframe not in timeframe_code:
        raise ValueError(
            f"`timeframe` must be one of {list(timeframe_code.keys())}, "
            f"got {timeframe!r}"
        )
    if max_workers < 1:
        raise ValueError(f"`max_workers` must be >= 1, got {max_workers}")
    if start_year > end_year:
        raise ValueError(
            f"`start_year` ({start_year}) must be <= `end_year` ({end_year})"
        )

    tf = timeframe_code[timeframe]
    frames: list[pd.DataFrame] = []
    base_url = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"

    def _download_one_period(year_month: tuple[int, int]) -> pd.DataFrame | None:
        year, month = year_month
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
            return None

        chunk = pd.read_csv(StringIO(resp.text))
        return chunk if not chunk.empty else None

    periods_to_fetch = [
        (year, 1)
        for year in range(start_year, end_year + 1)
    ] if timeframe == "daily" else [
        (year, month)
        for year in range(start_year, end_year + 1)
        for month in range(1, 13)
    ]

    workers = min(max_workers, len(periods_to_fetch))
    if workers == 1:
        for year_month in periods_to_fetch:
            chunk = _download_one_period(year_month)
            if chunk is not None:
                frames.append(chunk)
    else:
        # executor.map preserves input order, so concat order remains deterministic.
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for chunk in executor.map(_download_one_period, periods_to_fetch):
                if chunk is not None:
                    frames.append(chunk)

    if not frames:
        return pd.DataFrame()

    result = pd.concat(frames, ignore_index=True)
    result = clean_column_names(result)

    # Validate that columns match the expected schema for this timeframe
    expected = set(EXPECTED_COLUMNS[timeframe])
    # Hourly data has timezone-dependent column names
    if timeframe == "hourly" and timezone == "utc":
        expected = {
            col.replace("datetime_lst", "datetime_utc").replace("time_lst", "time_utc")
            for col in expected
        }
    actual = set(result.columns)
    if actual != expected:
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise ValueError(
            f"Column mismatch for timeframe={timeframe!r}.\n"
            f"Missing: {missing}\n"
            f"Extra:   {extra}"
        )

    return result
