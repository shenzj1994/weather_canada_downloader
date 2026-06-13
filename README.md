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

# Daily data for Vancouver Int'l A (Climate ID "1108447"), 2020–2021
df = download_climate_data("1108447", 2020, 2021)
print(df.head())

# Hourly data in UTC
df_hourly = download_climate_data("1108447", 2020, 2020, timeframe="hourly", timezone="utc")
```

## Find a Climate ID / Station ID

Use the bundled station inventory:

```python
from downloader import load_station_inventory

inv = load_station_inventory()
vancouver = inv[inv["name"].str.contains("VANCOUVER INT'L", na=False)]
print(vancouver[["name", "climate_id", "station_id", "province"]])
```

The **Climate ID** is the official 7-digit identifier (e.g. `"1108447"`) and
should be preferred over the internal Station ID. Per ECCC, Station IDs are
"an internal numbering system and may be subject to change without notice."

## API

### `download_climate_data(climate_id, start_year, end_year, *, station_id, timeframe, timezone)`

One of `climate_id` or `station_id` must be provided — not both, and not neither.
Using `climate_id` is recommended; passing `station_id=` emits a `UserWarning`.

| Argument     | Type    | Default   | Description                                              |
|--------------|---------|-----------|----------------------------------------------------------|
| `climate_id` | `str`   | `None`    | Official 7-digit Climate ID (e.g. `"1108447"`)          |
| `start_year` | `int`   | `None`    | First year to download (inclusive)                       |
| `end_year`   | `int`   | `None`    | Last year to download (inclusive)                        |
| `station_id` | `int`   | `None`    | *(keyword-only)* Internal Station ID. Not recommended.  |
| `timeframe`  | `str`   | `"daily"` | One of `"hourly"`, `"daily"`, `"monthly"`                |
| `timezone`   | `str`   | `"ltc"`   | `"ltc"` or `"utc"` (hourly only; ignored otherwise)      |

Returns a **single `pd.DataFrame`** with cleaned column names (lowercase, spaces replaced by underscores).

### `load_station_inventory(path)`

Load the station inventory CSV. Returns a cleaned DataFrame.
