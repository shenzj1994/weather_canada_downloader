"""Download historical climate data from Environment and Climate Change Canada.

Wraps the ECCC bulk-data endpoint documented at
https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/

Endpoint behaviour this module relies on (verified against the live API):

* hourly (``timeframe=1``) returns one calendar month per request;
* daily (``timeframe=2``) returns one calendar year per request;
* monthly (``timeframe=3``) returns the station's *entire* history whatever
  ``Year``/``Month`` is sent, so one request per station is enough;
* periods outside a station's record (and future dates) come back as rows
  with blank values rather than as an error;
* an unknown station returns HTTP 200 with an HTML page.
"""

from __future__ import annotations

import logging
import warnings
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from io import StringIO
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd
import requests

from ._cache import default_cache_dir
from ._columns import (
    EXPECTED_COLUMNS,
    clean_column_names,
    coerce_dtypes,
    expected_columns,
    measurement_columns,
)
from ._http import make_session
from .stations import (
    get_station,
    inventory_modified_date,
    normalize_climate_id,
    station_record_years,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html"
_TIMEFRAME_CODE = {"hourly": 1, "daily": 2, "monthly": 3}
_TIMEZONE_ALIASES = {"lst": "lst", "ltc": "lst", "local": "lst", "utc": "utc"}

Timeframe = Literal["hourly", "daily", "monthly"]
Timezone = Literal["lst", "utc", "ltc"]


def _request_params(
    station_id: int, year: int, month: int, timeframe: str, timezone: str
) -> dict[str, str | int]:
    params: dict[str, str | int] = {
        "format": "csv",
        "stationID": station_id,
        "Year": year,
        "Month": month,
        "Day": 1,
        "timeframe": _TIMEFRAME_CODE[timeframe],
        "submit": "Download Data",
    }
    if timeframe == "hourly" and timezone == "utc":
        params["time"] = "UTC"
    return params


def _is_final(
    timeframe: str, year: int, month: int, today: date, last_year: int | None
) -> bool:
    """Whether a period is complete, so its response can be cached for good."""
    if timeframe == "hourly":
        return (year, month) < (today.year, today.month)
    if timeframe == "daily":
        return year < today.year
    return last_year is not None and last_year < today.year


def _periods(timeframe: str, start_year: int, end_year: int, today: date) -> list[tuple[int, int]]:
    """(year, month) pairs to request, skipping periods entirely in the future."""
    if timeframe == "monthly":
        return [(start_year, 1)]
    if timeframe == "daily":
        return [(y, 1) for y in range(start_year, min(end_year, today.year) + 1)]
    return [
        (y, m)
        for y in range(start_year, end_year + 1)
        for m in range(1, 13)
        if (y, m) <= (today.year, today.month)
    ]


def _validate(timeframe: str, timezone: str, start_year, end_year, max_workers: int) -> str:
    if timeframe not in _TIMEFRAME_CODE:
        raise ValueError(
            f"`timeframe` must be one of {list(_TIMEFRAME_CODE)}, got {timeframe!r}"
        )
    tz = _TIMEZONE_ALIASES.get(str(timezone).lower())
    if tz is None:
        raise ValueError(f"`timezone` must be 'lst' or 'utc', got {timezone!r}")
    if start_year is None or end_year is None:
        raise TypeError("`start_year` and `end_year` are required.")
    if start_year > end_year:
        raise ValueError(f"`start_year` ({start_year}) must be <= `end_year` ({end_year})")
    if max_workers < 1:
        raise ValueError(f"`max_workers` must be >= 1, got {max_workers}")
    return tz


def _resolve_station(climate_id, station_id) -> tuple[int, pd.Series | None]:
    """Return (station_id, inventory row or None)."""
    if station_id is None and climate_id is None:
        raise TypeError("Either `station_id` or `climate_id` must be provided.")
    if station_id is not None and climate_id is not None:
        raise TypeError("Provide only one of `station_id` or `climate_id`, not both.")

    if climate_id is not None:
        station = get_station(normalize_climate_id(climate_id))
        return int(station["station_id"]), station

    warnings.warn(
        "Station ID is an internal numbering system and may be subject to change "
        "without notice. Use `climate_id` (the official Climate ID) instead.",
        UserWarning,
        stacklevel=3,
    )
    try:
        return int(station_id), get_station(station_id=station_id)
    except ValueError:
        return int(station_id), None


def _clip_years(
    station: pd.Series | None, timeframe: str, start_year: int, end_year: int, today: date
) -> tuple[int, int] | None:
    """Intersect the requested years with the station's record from the inventory.

    The upper bound is left alone for stations that look active, because the
    inventory snapshot can lag behind the newest data.
    """
    if station is None:
        return start_year, end_year
    record = station_record_years(station, timeframe)
    if record is None:
        return None
    first, last = record
    snapshot = inventory_modified_date()
    reference_year = snapshot.year if snapshot else today.year
    hi = end_year if last >= reference_year - 1 else min(end_year, last)
    lo = max(start_year, first)
    return (lo, hi) if lo <= hi else None


# pylint: disable=too-many-arguments,too-many-locals,too-many-branches,too-many-statements
def download_climate_data(
    climate_id: str | int | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    *,
    station_id: int | None = None,
    timeframe: Timeframe = "daily",
    timezone: Timezone = "lst",
    max_workers: int = 8,
    clip_to_record: bool = True,
    drop_empty: bool = True,
    coerce_types: bool = True,
    strict: bool = False,
    cache: bool | str | Path = False,
    progress: bool = False,
    session: requests.Session | None = None,
    timeout: float = 60,
) -> pd.DataFrame:
    """Download climate data for one station as a pandas DataFrame.

    Provide **either** ``climate_id`` (recommended, e.g. ``"1108395"`` for
    Vancouver Intl A) **or** ``station_id`` (ECCC's internal number, which may
    change without notice; passing it emits a warning).

    Parameters
    ----------
    climate_id : str or int, optional
        Official Climate ID from the station inventory.
    start_year, end_year : int
        Inclusive range of years to return.
    station_id : int, optional (keyword-only)
        Internal Station ID. Not recommended.
    timeframe : {"hourly", "daily", "monthly"}
        Data resolution. Hourly makes one request per month, daily one per
        year, monthly a single request for the whole record.
    timezone : {"lst", "utc"}
        Time zone of hourly timestamps: local standard time (default) or UTC.
        ``"ltc"`` is accepted as an alias of ``"lst"``. Ignored otherwise.
    max_workers : int
        Number of concurrent requests (default 8). Use 1 to download serially.
    clip_to_record : bool
        Skip years outside the station's record as listed in the inventory
        (default True). ECCC returns blank rows for those years.
    drop_empty : bool
        Drop rows where every measurement is blank, e.g. future dates or gaps
        in the record (default True).
    coerce_types : bool
        Convert measurements to floats, dates to datetimes and flags to
        strings (default True). With False all columns are left as text.
    strict : bool
        Raise if the API's columns differ from the expected schema instead of
        warning (default False).
    cache : bool or path
        Cache responses for completed periods on disk. True uses
        :func:`canada_weather.default_cache_dir`; a path uses that directory.
    progress : bool
        Show a tqdm progress bar.
    session : requests.Session, optional
        Session to reuse (see :func:`canada_weather.download_many`).
    timeout : float
        Per-request timeout in seconds.

    Returns
    -------
    pd.DataFrame
        All rows for the requested years, with cleaned lower-case column names
        such as ``max_temp_c`` and ``total_precip_mm``. An empty DataFrame with
        the expected columns is returned when there is no data.

    Examples
    --------
    >>> from canada_weather import download_climate_data
    >>> df = download_climate_data("1108395", 2020, 2021)
    >>> hourly = download_climate_data("1108395", 2024, 2024, timeframe="hourly", timezone="utc")
    """
    tz = _validate(timeframe, timezone, start_year, end_year, max_workers)
    sid, station = _resolve_station(climate_id, station_id)
    today = date.today()

    expected = expected_columns(timeframe, tz)
    ordered = [c.replace("_lst", "_utc") if tz == "utc" else c for c in EXPECTED_COLUMNS[timeframe]]
    empty = pd.DataFrame(columns=ordered)

    years: tuple[int, int] | None = (start_year, end_year)
    if clip_to_record:
        years = _clip_years(station, timeframe, start_year, end_year, today)
        if years is None:
            warnings.warn(
                f"Station {sid} has no {timeframe} data for {start_year}-{end_year} "
                f"according to the station inventory. Pass clip_to_record=False to "
                f"request it anyway.",
                UserWarning,
                stacklevel=2,
            )
            return empty
    lo, hi = years
    last_year = None
    if station is not None:
        record = station_record_years(station, timeframe)
        last_year = record[1] if record else None

    cache_dir: Path | None = None
    if cache:
        cache_dir = (default_cache_dir() if cache is True else Path(cache)) / "bulk" / str(sid)

    periods = _periods(timeframe, lo, hi, today)
    if not periods:
        return empty
    workers = min(max_workers, len(periods))
    own_session = session is None
    http = session or make_session(pool_size=workers)

    def fetch(period: tuple[int, int]) -> pd.DataFrame | None:
        year, month = period
        cache_file = None
        if cache_dir is not None:
            cache_file = cache_dir / f"{timeframe}_{tz}_{year}_{month:02d}.csv"
            if cache_file.exists():
                return _parse_csv(cache_file.read_text(encoding="utf-8"))

        resp = http.get(
            BASE_URL, params=_request_params(sid, year, month, timeframe, tz), timeout=timeout
        )
        resp.raise_for_status()
        text = resp.content.decode("utf-8-sig", errors="replace")
        content_type = resp.headers.get("Content-Type", "").lower()
        if "html" in content_type or not text.lstrip().startswith('"'):
            logger.warning("Station %s, %s-%02d: server returned no CSV", sid, year, month)
            return None

        if cache_file is not None and _is_final(timeframe, year, month, today, last_year):
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.write_text(text, encoding="utf-8")
        return _parse_csv(text)

    try:
        if workers == 1:
            results = map(fetch, periods)
            frames = list(_progress(results, progress, len(periods), sid))
        else:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                # map() preserves input order, so rows stay chronological.
                frames = list(_progress(pool.map(fetch, periods), progress, len(periods), sid))
    finally:
        if own_session:
            http.close()

    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        return empty

    result = clean_column_names(pd.concat(frames, ignore_index=True))
    _check_schema(result, expected, timeframe, strict)

    if timeframe == "monthly":
        year_num = pd.to_numeric(result["year"], errors="coerce")
        result = result[(year_num >= start_year) & (year_num <= end_year)]
    if drop_empty:
        values = measurement_columns(result)
        if values:
            result = result.dropna(subset=values, how="all")
    if coerce_types:
        result = coerce_dtypes(result)
    return result.reset_index(drop=True)


def _parse_csv(text: str) -> pd.DataFrame:
    # Read everything as text; coerce_dtypes() converts after concatenation so
    # chunks with different inferred types don't clash.
    return pd.read_csv(StringIO(text), dtype=str)


def _progress(iterable, enabled: bool, total: int, sid: int):
    if not enabled:
        return iterable
    from tqdm.auto import tqdm  # pylint: disable=import-outside-toplevel

    return tqdm(iterable, total=total, desc=f"Station {sid}", leave=False)


def _check_schema(df: pd.DataFrame, expected: set[str], timeframe: str, strict: bool) -> None:
    actual = set(df.columns)
    missing, extra = sorted(expected - actual), sorted(actual - expected)
    if not missing and not extra:
        return
    msg = (
        f"Columns returned for timeframe={timeframe!r} differ from the expected schema "
        f"(missing: {missing}, unexpected: {extra}). The ECCC API may have changed."
    )
    if strict:
        raise ValueError(msg)
    warnings.warn(msg, UserWarning, stacklevel=3)


def download_many(
    climate_ids: Iterable[str | int],
    start_year: int,
    end_year: int,
    *,
    timeframe: Timeframe = "daily",
    on_error: Literal["warn", "raise"] = "warn",
    progress: bool = False,
    max_workers: int = 8,
    **kwargs,
) -> pd.DataFrame:
    """Download several stations and return one DataFrame.

    Stations are downloaded one after another, each with up to
    ``max_workers`` concurrent requests over a shared connection pool, so the
    load on ECCC's server stays bounded however many stations are requested.
    Use the ``climate_id`` column to tell stations apart.

    Stations that fail are skipped with a warning (``on_error="warn"``) and
    listed in ``result.attrs["failed"]`` as ``{climate_id: error message}``.
    Other keyword arguments go to :func:`download_climate_data`.
    """
    ids = [normalize_climate_id(c) for c in climate_ids]
    frames: list[pd.DataFrame] = []
    failed: dict[str, str] = {}
    session = make_session(pool_size=max_workers)
    iterator = ids
    if progress:
        from tqdm.auto import tqdm  # pylint: disable=import-outside-toplevel

        iterator = tqdm(ids, desc="Stations")
    try:
        for cid in iterator:
            try:
                df = download_climate_data(
                    cid, start_year, end_year, timeframe=timeframe,
                    max_workers=max_workers, session=session, **kwargs,
                )
            except (requests.RequestException, ValueError, OSError) as exc:
                if on_error == "raise":
                    raise
                failed[cid] = str(exc)
                warnings.warn(f"Climate ID {cid} failed: {exc}", UserWarning, stacklevel=2)
                continue
            if not df.empty:
                frames.append(df)
    finally:
        session.close()

    result = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    result.attrs["failed"] = failed
    return result
