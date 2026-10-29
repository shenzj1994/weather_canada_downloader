"""Example: download daily data for every British Columbia station active in a period.

Run from anywhere after ``pip install "canada-weather-downloader[parquet]"``.
"""

import logging
import warnings
from pathlib import Path
from time import time

from canada_weather import download_many, find_stations

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

START_YEAR = 2015
END_YEAR = 2026
TIMEFRAME = "daily"
MAX_WORKERS = 8  # concurrent requests; keep this modest, it is a public server
OUTPUT_FILE = Path("sample_data") / f"bc_{TIMEFRAME}_climate_data_{START_YEAR}_{END_YEAR}.parquet"


def main() -> None:
    """Download BC data and save it as Parquet."""
    stations = find_stations(
        province="British Columbia", timeframe=TIMEFRAME, years=(START_YEAR, END_YEAR)
    )
    logger.info("%d BC stations have %s data in %d-%d",
                len(stations), TIMEFRAME, START_YEAR, END_YEAR)

    start = time()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)  # failures are collected in attrs
        df = download_many(
            stations["climate_id"], START_YEAR, END_YEAR,
            timeframe=TIMEFRAME, max_workers=MAX_WORKERS, cache=True, progress=True,
        )
    logger.info("Downloaded %s rows in %.0f s", f"{len(df):,}", time() - start)
    for cid, err in df.attrs["failed"].items():
        logger.error("Climate ID %s failed: %s", cid, err)

    if df.empty:
        logger.warning("No data downloaded.")
        return
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)
    logger.info("Saved to %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()
