"""Example: multi-threaded download of daily climate data for many stations."""

import logging
from time import time

import pandas as pd
import requests

from _column_config import coerce_numeric_dtypes
from station_inventory_reader import read_station_inventory
from downloader import download_climate_data
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

START_YEAR = 2015
END_YEAR = 2026
TIMEFRAME = "daily"
MAX_WORKERS = 50
OUTPUT_FILE = "sample_data/bc_daily_climate_data_2015_2026.parquet"

def main() -> None:
    """Download daily climate data for British Columbia stations in parallel."""

    df = read_station_inventory()
    df = df[
        # (df['dly_first_year'] <= START_YEAR)
        # & (df['dly_last_year'] >= END_YEAR)
        (df['province'] == 'BRITISH COLUMBIA')
    ]
    all_eligible_station_list = df['climate_id'].to_list()
    logger.info(
        "Downloading %s data for %d stations in parallel with %d workers…",
        TIMEFRAME, len(all_eligible_station_list), MAX_WORKERS,
    )
    results: list[pd.DataFrame] = []

    dl_start = time()

    for cid in tqdm(all_eligible_station_list, desc="Downloading station data"):
        try:
            df = download_climate_data(
                start_year=START_YEAR,
                end_year=END_YEAR,
                climate_id=cid,
                timeframe=TIMEFRAME
            )
        except (requests.RequestException, ValueError, OSError) as exc:
            logger.error("Station with Climate ID %s failed: %s", cid, exc)
            continue
        if df.empty:
            logger.warning("Station with Climate ID %s: no data returned", cid)
            continue
        df["climate_id"] = cid
        results.append(df)
        logger.info("Station with Climate ID %s: %d rows", cid, df.shape[0])

    dl_end = time()

    if not results:
        logger.warning("No data was downloaded from any station.")
        return

    combined = pd.concat(results, ignore_index=True)
    combined = coerce_numeric_dtypes(combined)
    logger.info("Done in %.1f seconds", dl_end - dl_start)
    logger.info("Combined shape: %s", combined.shape)
    combined.info()
    combined.to_parquet(OUTPUT_FILE, index=False)
    logger.info("Saved to %s", OUTPUT_FILE)


if __name__ == "__main__":
    main()
