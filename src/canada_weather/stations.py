"""Read, refresh and search the ECCC Station Inventory."""

from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from importlib import resources
from io import StringIO
from pathlib import Path
from typing import Literal

import numpy as np
import pandas as pd

from ._cache import default_cache_dir
from ._columns import clean_column_names

INVENTORY_URL = (
    "https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/"
    "Station%20Inventory%20EN.csv"
)
INVENTORY_FILENAME = "Station Inventory EN.csv"

_DTYPES = {
    "Climate ID": "string",
    "Station ID": "Int64",
    "WMO ID": "string",
    "TC ID": "string",
    "Elevation (m)": "float64",
    "First Year": "Int64",
    "Last Year": "Int64",
    "HLY First Year": "Int64",
    "HLY Last Year": "Int64",
    "DLY First Year": "Int64",
    "DLY Last Year": "Int64",
    "MLY First Year": "Int64",
    "MLY Last Year": "Int64",
}

# Inventory column prefix holding the record years for each timeframe.
_TIMEFRAME_PREFIX = {"hourly": "hly", "daily": "dly", "monthly": "mly"}


def bundled_inventory_path() -> Path:
    """Path of the inventory snapshot shipped inside the package."""
    return Path(str(resources.files("canada_weather") / "data" / INVENTORY_FILENAME))


def cached_inventory_path() -> Path:
    """Path where :func:`refresh_station_inventory` stores the latest copy."""
    return default_cache_dir() / INVENTORY_FILENAME


def inventory_modified_date(csv_path: str | Path | None = None) -> datetime | None:
    """Return the ``Modified Date`` stamped on the first line of an inventory."""
    path = Path(csv_path) if csv_path is not None else _active_inventory_path()
    with open(path, encoding="utf-8-sig") as fh:
        first = fh.readline()
    if not first.startswith("Modified Date:"):
        return None
    stamp = first.split(":", 1)[1].strip().replace(" UTC", "")
    try:
        return datetime.strptime(stamp, "%Y-%m-%d %H:%M")
    except ValueError:
        return None


def _active_inventory_path() -> Path:
    """The refreshed copy if it exists and is newer, otherwise the bundled one."""
    bundled = bundled_inventory_path()
    cached = cached_inventory_path()
    if cached.exists():
        cached_date = inventory_modified_date(cached)
        bundled_date = inventory_modified_date(bundled)
        if cached_date and (bundled_date is None or cached_date >= bundled_date):
            return cached
    return bundled


def _parse_inventory(source) -> pd.DataFrame:
    df = pd.read_csv(source, skiprows=3, dtype=_DTYPES, encoding="utf-8-sig")
    for col in df.columns:
        if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == object:
            df[col] = df[col].astype("string").str.strip()
    return clean_column_names(df)


@lru_cache(maxsize=8)
def _read_cached(path: str) -> pd.DataFrame:
    return _parse_inventory(path)


def read_station_inventory(csv_path: str | Path | None = None) -> pd.DataFrame:
    """Read the ECCC Station Inventory as a DataFrame.

    Parameters
    ----------
    csv_path : str | Path, optional
        Inventory CSV to read. By default the most recent of the copy saved by
        :func:`refresh_station_inventory` and the snapshot bundled with the
        package is used.

    Returns
    -------
    pd.DataFrame
        One row per station with cleaned column names (``climate_id``,
        ``station_id``, ``dly_first_year`` ...). A fresh copy is returned on
        every call, so it is safe to modify.
    """
    path = Path(csv_path) if csv_path is not None else _active_inventory_path()
    return _read_cached(str(path.resolve())).copy()


def refresh_station_inventory(
    dest: str | Path | None = None, *, timeout: float = 60
) -> Path:
    """Download the latest Station Inventory published by ECCC.

    ECCC republishes the inventory roughly monthly. The file is saved to the
    package cache (see :func:`canada_weather.default_cache_dir`) and used
    automatically by :func:`read_station_inventory` from then on.

    Returns the path of the saved file.
    """
    from ._http import make_session  # pylint: disable=import-outside-toplevel

    resp = make_session().get(INVENTORY_URL, timeout=timeout)
    resp.raise_for_status()
    text = resp.content.decode("utf-8-sig")
    if not text.startswith("Modified Date:"):
        raise ValueError("Unexpected response while downloading the station inventory.")
    parsed = _parse_inventory(StringIO(text))  # validate before saving
    if not {"climate_id", "station_id"}.issubset(parsed.columns) or parsed.empty:
        raise ValueError("Downloaded station inventory is missing expected columns.")

    path = Path(dest) if dest is not None else cached_inventory_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
    _read_cached.cache_clear()
    return path


def normalize_climate_id(climate_id: str | int) -> str:
    """Normalise a Climate ID (``1108395`` → ``"1108395"``, ``" 116frmn"`` → ``"116FRMN"``)."""
    return str(climate_id).strip().upper()


def get_station(
    climate_id: str | int | None = None, *, station_id: int | None = None
) -> pd.Series:
    """Return the inventory row for one station, looked up by Climate ID or Station ID."""
    inv = read_station_inventory()
    if climate_id is not None:
        cid = normalize_climate_id(climate_id)
        matches = inv[inv["climate_id"] == cid]
        label = f"Climate ID {cid!r}"
    elif station_id is not None:
        matches = inv[inv["station_id"] == int(station_id)]
        label = f"Station ID {station_id!r}"
    else:
        raise TypeError("Either `climate_id` or `station_id` must be provided.")

    if matches.empty:
        raise ValueError(
            f"{label} not found in the station inventory. If it is a new station, "
            f"run `canada_weather.refresh_station_inventory()`."
        )
    if len(matches) > 1:
        raise ValueError(f"{label} matches {len(matches)} stations in the inventory.")
    return matches.iloc[0]


def station_record_years(
    station: pd.Series, timeframe: Literal["hourly", "daily", "monthly"]
) -> tuple[int, int] | None:
    """First and last year a station has data for a timeframe, or None."""
    prefix = _TIMEFRAME_PREFIX[timeframe]
    first, last = station.get(f"{prefix}_first_year"), station.get(f"{prefix}_last_year")
    if pd.isna(first) or pd.isna(last):
        return None
    return int(first), int(last)


def _haversine_km(lat1: float, lon1: float, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    """Great-circle distance in km from one point to many."""
    lat1_r, lon1_r = np.radians(lat1), np.radians(lon1)
    lat2_r, lon2_r = np.radians(lat2.to_numpy()), np.radians(lon2.to_numpy())
    a = (
        np.sin((lat2_r - lat1_r) / 2) ** 2
        + np.cos(lat1_r) * np.cos(lat2_r) * np.sin((lon2_r - lon1_r) / 2) ** 2
    )
    dist = 2 * 6371.0088 * np.arcsin(np.sqrt(np.clip(a, 0, 1)))
    return pd.Series(dist, index=lat2.index)


# pylint: disable=too-many-arguments,too-many-locals
def find_stations(
    name: str | None = None,
    *,
    province: str | None = None,
    near: tuple[float, float] | None = None,
    radius_km: float | None = None,
    timeframe: Literal["hourly", "daily", "monthly"] | None = None,
    years: tuple[int, int] | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    """Search the station inventory.

    Parameters
    ----------
    name : str, optional
        Case-insensitive substring of the station name (e.g. ``"vancouver"``).
    province : str, optional
        Province name (case-insensitive, e.g. ``"British Columbia"``).
    near : (lat, lon), optional
        Sort by distance to this point and add a ``distance_km`` column.
    radius_km : float, optional
        With ``near``, keep only stations within this distance.
    timeframe : {"hourly", "daily", "monthly"}, optional
        Keep only stations that have data at this resolution.
    years : (start, end), optional
        Keep only stations whose record for ``timeframe`` (or overall record
        if no timeframe) overlaps this range.
    limit : int, optional
        Return at most this many rows.

    Examples
    --------
    >>> find_stations("vancouver int", timeframe="hourly", years=(2020, 2025))
    >>> find_stations(near=(49.17, -123.14), radius_km=15, timeframe="daily")
    """
    inv = read_station_inventory()
    mask = pd.Series(True, index=inv.index)
    if name:
        mask &= inv["name"].str.contains(name, case=False, regex=False).fillna(False)
    if province:
        mask &= (inv["province"].str.upper() == province.strip().upper()).fillna(False)

    prefix = _TIMEFRAME_PREFIX[timeframe] if timeframe else None
    first_col = f"{prefix}_first_year" if prefix else "first_year"
    last_col = f"{prefix}_last_year" if prefix else "last_year"
    if timeframe:
        mask &= inv[first_col].notna() & inv[last_col].notna()
    if years:
        start, end = years
        mask &= ((inv[first_col] <= end) & (inv[last_col] >= start)).fillna(False)

    out = inv[mask].copy()
    if near is not None:
        lat, lon = near
        out["distance_km"] = _haversine_km(
            lat, lon,
            out["latitude_decimal_degrees"].astype(float),
            out["longitude_decimal_degrees"].astype(float),
        ).round(2)
        if radius_km is not None:
            out = out[out["distance_km"] <= radius_km]
        out = out.sort_values("distance_km")
    if limit is not None:
        out = out.head(limit)
    return out.reset_index(drop=True)
