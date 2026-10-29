"""Download historical climate data from Environment and Climate Change Canada.

>>> from canada_weather import find_stations, download_climate_data
>>> find_stations("vancouver intl", timeframe="daily")
>>> df = download_climate_data("1108395", 2020, 2024)
"""

from ._cache import default_cache_dir
from ._columns import clean_column_names, coerce_dtypes
from ._http import __version__
from .downloader import download_climate_data, download_many
from .stations import (
    find_stations,
    get_station,
    inventory_modified_date,
    read_station_inventory,
    refresh_station_inventory,
)

__all__ = [
    "__version__",
    "clean_column_names",
    "coerce_dtypes",
    "default_cache_dir",
    "download_climate_data",
    "download_many",
    "find_stations",
    "get_station",
    "inventory_modified_date",
    "read_station_inventory",
    "refresh_station_inventory",
]
