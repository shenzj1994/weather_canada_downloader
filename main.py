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
OUTPUT_FILE = "climate_data.parquet"

def main() -> None:

    df = read_station_inventory()
    df = df[(df['dly_first_year'] <= START_YEAR) & (df['dly_last_year'] >= END_YEAR)&(df['province']=='BRITISH COLUMBIA')]
    ALL_ELIGIBLE_STATION_LIST = df['climate_id'].to_list()
    print(f"Downloading {TIMEFRAME} data for {len(ALL_ELIGIBLE_STATION_LIST)} stations in parallel …")
    results: list[pd.DataFrame] = []

    dl_start = time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        fut = {
            pool.submit(
                download_climate_data,
                start_year=START_YEAR,
                end_year=END_YEAR,
                climate_id=cid,
                timeframe=TIMEFRAME,
            ): cid
            for cid in ALL_ELIGIBLE_STATION_LIST
        }
        for f in as_completed(fut):
            cid = fut[f]
            try:
                df = f.result()
            except Exception as exc:
                print(f"  ❌ Station {cid} failed: {exc}")
                continue
            if df.empty:
                print(f"  ⚠️ Station {cid}: no data returned")
                continue
            df["climate_id"] = cid
            results.append(df)
            print(f"  ✅ Station {cid}: {df.shape[0]} rows")

    dl_end = time()

    if not results:
        print("No data was downloaded from any station.")
        return

    combined = pd.concat(results, ignore_index=True)
    print(f"\nDone in {dl_end - dl_start:.1f} seconds")
    print(f"Combined shape: {combined.shape}")
    combined.info()
    combined.to_parquet(OUTPUT_FILE, index=False)
    print(f"Saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
