"""Example: multi-threaded download of daily climate data for many stations."""

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import time

import pandas as pd

from station_inventory_reader import read_station_inventory
from downloader import download_climate_data

START_YEAR = 1996
END_YEAR = 2026
TIMEFRAME = "daily"
MAX_WORKERS = 50

def main() -> None:

    df = read_station_inventory()
    df = df[(df['dly_first_year'] <= START_YEAR) & (df['dly_last_year'] >= END_YEAR)&(df['province']=='BRITISH COLUMBIA')]
    ALL_ELIGIBLE_STATION_LIST = df['station_id'].to_list()
    print(f"Downloading {TIMEFRAME} data for {len(ALL_ELIGIBLE_STATION_LIST)} stations in parallel …")
    results: list[pd.DataFrame] = []

    dl_start = time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut = {
            pool.submit(
                download_climate_data, sid, START_YEAR, END_YEAR, timeframe=TIMEFRAME
            ): sid
            for sid in ALL_ELIGIBLE_STATION_LIST
        }
        for f in as_completed(fut):
            sid = fut[f]
            try:
                df = f.result()
            except Exception as exc:
                print(f"  ❌ Station {sid} failed: {exc}")
                continue
            if df.empty:
                print(f"  ⚠️ Station {sid}: no data returned")
                continue
            df["station_id"] = sid
            results.append(df)
            print(f"  ✅ Station {sid}: {df.shape[0]} rows")

    dl_end = time()

    if not results:
        print("No data was downloaded from any station.")
        return

    combined = pd.concat(results, ignore_index=True)
    print(f"\nDone in {dl_end - dl_start:.1f} seconds")
    print(f"Combined shape: {combined.shape}")
    combined.info()


if __name__ == "__main__":
    main()
