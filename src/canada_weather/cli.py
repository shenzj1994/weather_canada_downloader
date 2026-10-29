"""Command-line interface: ``canada-weather download|stations|refresh-inventory``."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

from . import __version__
from .downloader import download_many
from .stations import find_stations, inventory_modified_date, refresh_station_inventory


def _write(df: pd.DataFrame, output: str | None) -> None:
    if output is None or output == "-":
        df.to_csv(sys.stdout, index=False)
        return
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix.lower() == ".parquet":
        df.to_parquet(path, index=False)
    else:
        df.to_csv(path, index=False)
    print(f"Wrote {len(df):,} rows to {path}", file=sys.stderr)


def _cmd_download(args: argparse.Namespace) -> int:
    df = download_many(
        args.climate_ids,
        args.start_year,
        args.end_year,
        timeframe=args.timeframe,
        timezone=args.timezone,
        max_workers=args.workers,
        cache=args.cache,
        progress=not args.quiet,
    )
    _write(df, args.output)
    failed = df.attrs.get("failed", {})
    return 1 if failed and len(failed) == len(args.climate_ids) else 0


def _cmd_stations(args: argparse.Namespace) -> int:
    near = (args.lat, args.lon) if args.lat is not None and args.lon is not None else None
    years = (args.start_year, args.end_year) if args.start_year and args.end_year else None
    df = find_stations(
        args.name, province=args.province, near=near, radius_km=args.radius_km,
        timeframe=args.timeframe, years=years, limit=args.limit,
    )
    cols = ["name", "province", "climate_id", "station_id", "first_year", "last_year",
            "hly_first_year", "hly_last_year", "dly_first_year", "dly_last_year",
            "mly_first_year", "mly_last_year"]
    if "distance_km" in df.columns:
        cols.append("distance_km")
    _write(df[cols], args.output)
    return 0


def _cmd_refresh(_args: argparse.Namespace) -> int:
    path = refresh_station_inventory()
    print(f"Saved inventory dated {inventory_modified_date(path)} to {path}")
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the argument parser."""
    parser = argparse.ArgumentParser(
        prog="canada-weather",
        description="Download historical weather data from Environment and Climate Change Canada.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)

    dl = sub.add_parser("download", help="download data for one or more stations")
    dl.add_argument("climate_ids", nargs="+", help="Climate ID(s), e.g. 1108395")
    dl.add_argument("start_year", type=int)
    dl.add_argument("end_year", type=int)
    dl.add_argument("-t", "--timeframe", choices=["hourly", "daily", "monthly"], default="daily")
    dl.add_argument("--timezone", choices=["lst", "utc"], default="lst")
    dl.add_argument("-o", "--output", help="output .csv or .parquet (default: stdout)")
    dl.add_argument("-w", "--workers", type=int, default=8)
    dl.add_argument("--cache", action="store_true", help="cache completed periods on disk")
    dl.add_argument("-q", "--quiet", action="store_true", help="hide progress bars")
    dl.set_defaults(func=_cmd_download)

    st = sub.add_parser("stations", help="search the station inventory")
    st.add_argument("name", nargs="?", help="substring of the station name")
    st.add_argument("--province")
    st.add_argument("--lat", type=float)
    st.add_argument("--lon", type=float)
    st.add_argument("--radius-km", type=float)
    st.add_argument("-t", "--timeframe", choices=["hourly", "daily", "monthly"])
    st.add_argument("--start-year", type=int)
    st.add_argument("--end-year", type=int)
    st.add_argument("--limit", type=int, default=50)
    st.add_argument("-o", "--output")
    st.set_defaults(func=_cmd_stations)

    rf = sub.add_parser("refresh-inventory", help="download the latest station inventory")
    rf.set_defaults(func=_cmd_refresh)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``canada-weather`` command."""
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
