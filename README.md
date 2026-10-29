# Canada Weather Downloader

Download historical weather observations from **Environment and Climate Change Canada** (ECCC) as clean pandas DataFrames.

The package wraps ECCC's [bulk data endpoint](https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/), so you don't have to write the `wget` loops yourself. It looks up stations, splits requests by period, retries failed requests, drops blank rows and returns typed columns.

## Installation

```bash
pip install git+https://github.com/shenzj1994/weather_canada_downloader
# with Parquet support
pip install "canada-weather-downloader[parquet] @ git+https://github.com/shenzj1994/weather_canada_downloader"
```

Requires Python ≥ 3.12, `pandas`, `requests` and `tqdm`.

## Quick start

```python
from canada_weather import find_stations, download_climate_data

# 1. Find a station
find_stations("vancouver intl", timeframe="daily", years=(2020, 2025))
#                name          province climate_id  station_id  ...  dly_first_year  dly_last_year
# 0  VANCOUVER INTL A  BRITISH COLUMBIA    1108395       51442  ...            2013           2026

# 2. Download it
daily = download_climate_data("1108395", 2020, 2024)
hourly = download_climate_data("1108395", 2024, 2024, timeframe="hourly", timezone="utc")
```

Stations near a point:

```python
find_stations(near=(49.17, -123.14), radius_km=20, timeframe="hourly", years=(2024, 2024))
```

Several stations at once:

```python
from canada_weather import download_many

bc = find_stations(province="British Columbia", timeframe="daily", years=(2020, 2024))
df = download_many(bc["climate_id"], 2020, 2024, cache=True, progress=True)
df.attrs["failed"]  # {climate_id: error} for stations that failed
```

## Command line

```bash
canada-weather stations "vancouver intl" -t hourly
canada-weather download 1108395 2020 2024 -o vancouver_daily.parquet
canada-weather download 1108395 2024 2024 -t hourly --timezone utc --cache -o yvr_hourly.csv
canada-weather refresh-inventory
```

## Climate ID vs Station ID

The **Climate ID** (e.g. `"1108395"`) is ECCC's official, permanent identifier, so use it. The **Station ID** is an internal number that the API needs. ECCC says it "may be subject to change without notice", so the package looks it up for you from the station inventory. Passing `station_id=` directly works, but you'll get a warning.

Stations are often replaced. For example, `1108447` *VANCOUVER INT'L A* covers 1937–2013, and `1108395` *VANCOUVER INTL A* covers 2013 to today. Check `find_stations()` to see which station covers the years you need.

## How the data is fetched

| timeframe | requests made | notes |
|-----------|---------------|-------|
| `hourly`  | one per month | `timezone="lst"` (local standard time, default) or `"utc"` |
| `daily`   | one per year  | |
| `monthly` | one per station | ECCC returns the whole record; the package filters it to your years |

The package handles three quirks of the API:

- **Years outside a station's record.** ECCC still returns a full calendar for these, with every value blank. Requests are clipped to the station's record from the inventory (`clip_to_record=True`).
- **Blank rows.** Future dates and gaps in the record come back as rows where every value is blank. These are dropped (`drop_empty=True`).
- **Unknown stations.** These return an HTML page, not an error, and are treated as "no data".

## `download_climate_data()` arguments

| Argument | Default | Description |
|---|---|---|
| `climate_id` | — | Official Climate ID (str or int) |
| `start_year`, `end_year` | — | Inclusive year range |
| `station_id` | `None` | *(keyword-only)* internal Station ID; not recommended |
| `timeframe` | `"daily"` | `"hourly"`, `"daily"` or `"monthly"` |
| `timezone` | `"lst"` | Hourly only: `"lst"` (alias `"ltc"`) or `"utc"` |
| `max_workers` | `8` | Concurrent requests |
| `clip_to_record` | `True` | Skip years outside the station's record |
| `drop_empty` | `True` | Drop rows with no measurements |
| `coerce_types` | `True` | Floats for values, datetimes for dates, strings for flags |
| `strict` | `False` | Raise instead of warn if the API's columns change |
| `cache` | `False` | `True` or a directory: cache completed periods on disk |
| `progress` | `False` | Show a progress bar |

Columns are lower-case with units as suffixes: `max_temp_c`, `total_precip_mm`, `spd_of_max_gust_kmh`, and so on. Each value has a matching `*_flag` column with ECCC's data-quality flag (`M` missing, `E` estimated, `T` trace...).

## Station inventory

A snapshot of ECCC's *Station Inventory EN.csv* ships with the package. ECCC republishes it about once a month. To get the latest version:

```python
from canada_weather import refresh_station_inventory, inventory_modified_date
refresh_station_inventory()     # saved to the cache dir and used from then on
inventory_modified_date()
```

## Cache

Cached data and the refreshed inventory live in `~/.cache/canada_weather` on Linux, `~/Library/Caches/canada_weather` on macOS and `%LOCALAPPDATA%\canada_weather` on Windows. To use a different location, set `CANADA_WEATHER_CACHE`. Only complete periods are cached, never the current month or year. Delete the folder to clear the cache.

## Development

```bash
uv run --extra dev pytest -m "not integration"   # offline tests (HTTP is mocked)
uv run --extra dev pytest -m integration         # live ECCC API
uv run --extra dev pylint src tests examples
```

## Data source and terms

Data © Environment and Climate Change Canada, from the [Historical Climate Data](https://climate.weather.gc.ca/) site, used under the [Open Government Licence – Canada](https://open.canada.ca/en/open-government-licence-canada).
