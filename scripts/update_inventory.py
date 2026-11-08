"""Replace the bundled station inventory with ECCC's latest copy, if newer.

Used by .github/workflows/update-inventory.yml, but also runs locally:

    uv run python scripts/update_inventory.py [--summary summary.md]

The file is saved byte-for-byte as ECCC publishes it (BOM included). Exits 0
whether or not anything changed; prints ``changed=true|false`` and appends it
to ``$GITHUB_OUTPUT`` when set.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
from pathlib import Path

from canada_weather._http import make_session
from canada_weather.stations import INVENTORY_URL, inventory_modified_date, read_station_inventory

BUNDLED = Path(__file__).resolve().parents[1] / "src" / "canada_weather" / "data" / "Station Inventory EN.csv"
MIN_STATIONS = 5000  # sanity floor: the inventory has ~8,600 stations


def _stations(text: str) -> dict[str, list[str]]:
    rows = list(csv.reader(io.StringIO(text.lstrip("﻿"))))[3:]
    header = rows[0]
    cid = header.index("Climate ID")
    return {r[cid].strip(): r for r in rows[1:] if len(r) == len(header)}


def _summary(old_text: str, new_text: str, old_date, new_date) -> str:
    old, new = _stations(old_text), _stations(new_text)
    added = sorted(new.keys() - old.keys())
    removed = sorted(old.keys() - new.keys())
    changed = sorted(k for k in new.keys() & old.keys() if new[k] != old[k])

    def names(ids: list[str], src: dict[str, list[str]], limit: int = 15) -> str:
        shown = ", ".join(f"{src[i][0]} (`{i}`)" for i in ids[:limit])
        return shown + (f", … and {len(ids) - limit} more" if len(ids) > limit else "")

    lines = [
        f"ECCC published a new station inventory: **{old_date:%Y-%m-%d} → {new_date:%Y-%m-%d}**.",
        "",
        "| | |",
        "|---|---|",
        f"| Stations | {len(old):,} → {len(new):,} |",
        f"| Added | {len(added)} |",
        f"| Removed | {len(removed)} |",
        f"| Updated | {len(changed)} |",
        "",
    ]
    if added:
        lines += [f"**Added:** {names(added, new)}", ""]
    if removed:
        lines += [f"**Removed:** {names(removed, old)}", ""]
    if changed:
        lines += [f"**Updated:** {names(changed, new)}", ""]
    lines.append(f"Source: {INVENTORY_URL}")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    """Download, validate and (if newer) replace the bundled inventory."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dest", type=Path, default=BUNDLED)
    parser.add_argument("--summary", type=Path, help="write a Markdown summary of the changes here")
    args = parser.parse_args(argv)

    resp = make_session().get(INVENTORY_URL, timeout=120)
    resp.raise_for_status()
    raw = resp.content
    new_text = raw.decode("utf-8-sig")

    tmp = args.dest.with_suffix(".download")
    tmp.write_bytes(raw)
    try:
        new_date = inventory_modified_date(tmp)
        if new_date is None:
            raise SystemExit("Downloaded file has no 'Modified Date' line; not an inventory?")
        inv = read_station_inventory(tmp)  # raises if the CSV can't be parsed
        missing = {"climate_id", "station_id", "dly_first_year", "hly_first_year"} - set(inv.columns)
        if missing or len(inv) < MIN_STATIONS:
            raise SystemExit(f"Downloaded inventory looks wrong: {len(inv)} rows, missing {missing}")

        old_text = args.dest.read_text(encoding="utf-8-sig")
        old_date = inventory_modified_date(args.dest)
        changed = old_date is None or new_date > old_date
        if changed:
            tmp.replace(args.dest)
            if args.summary:
                args.summary.write_text(_summary(old_text, new_text, old_date, new_date), encoding="utf-8")
        print(f"bundled: {old_date}, ECCC: {new_date}")
    finally:
        tmp.unlink(missing_ok=True)

    print(f"changed={'true' if changed else 'false'}")
    if out := os.environ.get("GITHUB_OUTPUT"):
        with open(out, "a", encoding="utf-8") as fh:
            fh.write(f"changed={'true' if changed else 'false'}\n")
            fh.write(f"date={new_date:%Y-%m-%d}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
