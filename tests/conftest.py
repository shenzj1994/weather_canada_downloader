"""Shared fixtures: a fake HTTP session that mimics the ECCC bulk-data endpoint.

Headers are copied from real responses (October 2026), so the column-cleaning
and schema checks run against the actual API format.
"""

# pylint: disable=missing-function-docstring,missing-class-docstring

from __future__ import annotations

import calendar
from dataclasses import dataclass, field

import pytest

HEADERS = {
    1: ('"Longitude (x)","Latitude (y)","Station Name","Climate ID","Date/Time ({tz})","Year",'
        '"Month","Day","Time ({tz})","Flag","Temp (°C)","Temp Flag","Dew Point Temp (°C)",'
        '"Dew Point Temp Flag","Rel Hum (%)","Rel Hum Flag","Precip. Amount (mm)",'
        '"Precip. Amount Flag","Wind Dir (10s deg)","Wind Dir Flag","Wind Spd (km/h)",'
        '"Wind Spd Flag","Visibility (km)","Visibility Flag","Stn Press (kPa)","Stn Press Flag",'
        '"Hmdx","Hmdx Flag","Wind Chill","Wind Chill Flag","Weather"'),
    2: ('"Longitude (x)","Latitude (y)","Station Name","Climate ID","Date/Time","Year","Month",'
        '"Day","Data Quality","Max Temp (°C)","Max Temp Flag","Min Temp (°C)","Min Temp Flag",'
        '"Mean Temp (°C)","Mean Temp Flag","Heat Deg Days (°C)","Heat Deg Days Flag",'
        '"Cool Deg Days (°C)","Cool Deg Days Flag","Total Rain (mm)","Total Rain Flag",'
        '"Total Snow (cm)","Total Snow Flag","Total Precip (mm)","Total Precip Flag",'
        '"Snow on Grnd (cm)","Snow on Grnd Flag","Dir of Max Gust (10s deg)",'
        '"Dir of Max Gust Flag","Spd of Max Gust (km/h)","Spd of Max Gust Flag"'),
    3: ('"Longitude (x)","Latitude (y)","Station Name","Climate ID","Date/Time","Year","Month",'
        '"Mean Max Temp (°C)","Mean Max Temp Flag","Mean Min Temp (°C)","Mean Min Temp Flag",'
        '"Mean Temp (°C)","Mean Temp Flag","Extr Max Temp (°C)","Extr Max Temp Flag",'
        '"Extr Min Temp (°C)","Extr Min Temp Flag","Total Rain (mm)","Total Rain Flag",'
        '"Total Snow (cm)","Total Snow Flag","Total Precip (mm)","Total Precip Flag",'
        '"Snow Grnd Last Day (cm)","Snow Grnd Last Day Flag","Dir of Max Gust (10\'s deg)",'
        '"Dir of Max Gust Flag","Spd of Max Gust (km/h)","Spd of Max Gust Flag"'),
}

STATION = '"-123.18","49.19","VANCOUVER INTL A","1108395"'


def _q(*vals) -> str:
    return ",".join(f'"{v}"' for v in vals)


def daily_csv(year: int, blank_from: tuple[int, int] | None = None) -> str:
    """One year of daily rows; rows on/after ``blank_from`` (month, day) are blank."""
    rows = [HEADERS[2]]
    for month in range(1, 13):
        for day in range(1, calendar.monthrange(year, month)[1] + 1):
            prefix = f"{STATION},{_q(f'{year}-{month:02d}-{day:02d}', year, f'{month:02d}', f'{day:02d}')}"
            if blank_from and (month, day) >= blank_from:
                rows.append(prefix + "," + _q(*[""] * 19) + ',"","M","","M"')
            else:
                rows.append(prefix + "," + _q("", "10.5", "", "2.5", "", "6.5", "", "11.5", "",
                                              "0.0", "", "1.2", "T", "0.0", "", "1.2", "",
                                              "", "", "", "M", "", "M"))
    return "\n".join(rows) + "\n"


def hourly_csv(year: int, month: int, tz: str = "LST") -> str:
    """One month of hourly rows (first day only, to keep it small)."""
    rows = [HEADERS[1].format(tz=tz)]
    for hour in range(24):
        rows.append(f"{STATION}," + _q(
            f"{year}-{month:02d}-01 {hour:02d}:00", year, f"{month:02d}", "01", f"{hour:02d}:00",
            "", "18.5", "", "16.4", "", "87", "", "", "", "21", "", "13", "", "32.2", "",
            "101.34", "", "", "", "", "", "NA" if hour % 2 else "Clear"))
    return "\n".join(rows) + "\n"


def monthly_csv(first_year: int = 1937, last_year: int = 2013) -> str:
    """Full monthly history, as the API returns regardless of Year/Month."""
    rows = [HEADERS[3]]
    for year in range(first_year, last_year + 1):
        for month in range(1, 13):
            rows.append('"-123.18","49.20","VANCOUVER INT\'L A","1108447",' + _q(
                f"{year}-{month:02d}", year, f"{month:02d}", "10.0", "", "3.0", "", "6.5", "",
                "20.0", "", "-5.0", "", "100.0", "", "5.0", "", "105.0", "", "", "", "", "",
                "", ""))
    return "\n".join(rows) + "\n"


HTML_PAGE = "<!DOCTYPE html>\n<html><body>Not found</body></html>\n"


@dataclass
class FakeResponse:
    """Minimal stand-in for requests.Response."""

    text: str
    content_type: str = "application/force-download"
    status_code: int = 200
    headers: dict = field(init=False)

    def __post_init__(self):
        self.headers = {"Content-Type": self.content_type}

    @property
    def content(self) -> bytes:
        return self.text.encode("utf-8")

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests  # pylint: disable=import-outside-toplevel
            raise requests.HTTPError(f"{self.status_code} error")


class FakeSession:
    """Records requests and answers them with a user-supplied handler."""

    def __init__(self, handler):
        self.handler = handler
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):  # pylint: disable=unused-argument
        self.calls.append(dict(params or {}))
        return self.handler(params or {})

    def close(self):
        pass


def default_handler(params: dict) -> FakeResponse:
    """Serve plausible data for any request."""
    tf, year, month = params["timeframe"], int(params["Year"]), int(params["Month"])
    if params["stationID"] == 999999:
        return FakeResponse(HTML_PAGE, "text/html; charset=UTF-8")
    if tf == 1:
        return FakeResponse(hourly_csv(year, month, "UTC" if params.get("time") == "UTC" else "LST"))
    if tf == 2:
        return FakeResponse(daily_csv(year))
    return FakeResponse(monthly_csv())


@pytest.fixture
def fake_session():
    """A FakeSession using :func:`default_handler`."""
    return FakeSession(default_handler)
