# Canada Weather Downloader

A Python package to download historical climate data from **Environment and Climate Change Canada** (ECCC).

The package wraps the [ECCC bulk data endpoint](https://climate.weather.gc.ca/climate_data/bulk_data_e.html) and returns clean pandas DataFrames.

## Installation

```bash
pip install canada-weather-downloader
```

Requires Python ≥ 3.13, `pandas`, and `requests`.

## Quick Start

```python
from downloader import download_climate_data

# Daily data for station 889 (Vancouver Int'l A), 2020–2021
df = download_climate_data(station_id=889, start_year=2020, end_year=2021)
print(df.head())

# Hourly data in UTC
df_hourly = download_climate_data(
    station_id=889,
    start_year=2020,
    end_year=2020,
    timeframe="hourly",
    timezone="utc",
)
```

## Find a Station ID

Use the bundled station inventory:

```python
from downloader import load_station_inventory

inv = load_station_inventory()
vancouver = inv[inv["name"].str.contains("VANCOUVER INT'L", na=False)]
print(vancouver[["name", "station_id", "province"]])
```

## API

### `download_climate_data(station_id, start_year, end_year, timeframe, timezone)`

| Argument     | Type    | Default   | Description                                     |
|--------------|---------|-----------|-------------------------------------------------|
| `station_id` | `int`   | —         | Numeric Station ID from the inventory           |
| `start_year` | `int`   | —         | First year to download (inclusive)              |
| `end_year`   | `int`   | —         | Last year to download (inclusive)               |
| `timeframe`  | `str`   | `"daily"` | One of `"hourly"`, `"daily"`, `"monthly"`       |
| `timezone`   | `str`   | `"ltc"`   | `"ltc"` or `"utc"` (hourly only; ignored otherwise) |

Returns a **single `pd.DataFrame`** with cleaned column names (lowercase, spaces replaced by underscores).

### `load_station_inventory(path)`

Load the station inventory CSV. Returns a cleaned DataFrame.
