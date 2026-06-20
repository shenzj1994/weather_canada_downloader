# Canada Weather Downloader

A Python package to download historical climate data from **Environment and Climate Change Canada** (ECCC).

The package wraps the [ECCC bulk data endpoint](https://climate.weather.gc.ca/climate_data/bulk_data_e.html) and returns clean pandas DataFrames.

## Installation

```bash
pip install canada-weather-downloader
```

Requires Python ≥ 3.10, `pandas`, `requests`, and `tqdm`.

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
from station_inventory_reader import read_station_inventory

inv = read_station_inventory()
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

Behavior by timeframe:

- `daily`: sends one request per year with `Month=1` and `Day=1`; the ECCC API returns the full year in that response.
- `hourly` and `monthly`: sends one request per month in the selected year range.

| Argument     | Type    | Default   | Description                                              |
|--------------|---------|-----------|----------------------------------------------------------|
| `climate_id` | `str`   | `None`    | Official 7-digit Climate ID (e.g. `"1108447"`)          |
| `start_year` | `int`   | `None`    | First year to download (inclusive)                       |
| `end_year`   | `int`   | `None`    | Last year to download (inclusive)                        |
| `station_id` | `int`   | `None`    | *(keyword-only)* Internal Station ID. Not recommended.  |
| `timeframe`  | `str`   | `"daily"` | One of `"hourly"`, `"daily"`, `"monthly"`                |
| `timezone`   | `str`   | `"ltc"`   | `"ltc"` or `"utc"` (hourly only; ignored otherwise)      |

Returns a **single `pd.DataFrame`** with cleaned column names (lowercase, spaces replaced by underscores).

### `read_station_inventory(csv_path)`

Load the station inventory CSV. Returns a cleaned DataFrame with normalized column names.
