/** Parse the ECCC "Station Inventory EN.csv" into compact station records. */

import { parseCsv } from "./csv.ts";

/** Granularities offered by the web app. */
export type Timeframe = "hourly" | "daily";
export const TIMEFRAMES: readonly Timeframe[] = ["hourly", "daily"];

/** Inclusive [first, last] year range, or null when the station has no data. */
export type YearRange = [number, number] | null;

export interface Station {
  name: string;
  province: string;
  climateId: string;
  stationId: number;
  lat: number | null;
  lon: number | null;
  elevation: number | null;
  hourly: YearRange;
  daily: YearRange;
}

export interface Inventory {
  /** "Modified Date" stamped on the first line, as ISO 8601 (UTC), if present. */
  modified: string | null;
  stations: Station[];
}

const REQUIRED = [
  "Name",
  "Province",
  "Climate ID",
  "Station ID",
  "HLY First Year",
  "HLY Last Year",
  "DLY First Year",
  "DLY Last Year",
] as const;

function toInt(value: string | undefined): number | null {
  if (value === undefined) return null;
  const v = value.trim();
  if (v === "") return null;
  const n = Number(v);
  return Number.isInteger(n) ? n : null;
}

function toFloat(value: string | undefined): number | null {
  if (value === undefined || value.trim() === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function range(first: string | undefined, last: string | undefined): YearRange {
  const f = toInt(first);
  const l = toInt(last);
  return f !== null && l !== null && f <= l ? [f, l] : null;
}

/** Parse the "Modified Date: 2026-06-03 23:30 UTC" line. */
export function parseModifiedDate(firstLine: string): string | null {
  const m = /^﻿?Modified Date:\s*(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})/.exec(firstLine);
  return m ? `${m[1]}T${m[2]}:00Z` : null;
}

/**
 * Parse the inventory CSV. Throws if the header row or required columns are
 * missing, so a bad download is never mistaken for an empty inventory.
 */
export function parseInventory(text: string): Inventory {
  const firstLine = text.slice(0, text.indexOf("\n") >>> 0);
  const modified = parseModifiedDate(firstLine);
  const rows = parseCsv(text);
  const headerIdx = rows.findIndex((r) => r.includes("Climate ID") && r.includes("Station ID"));
  if (headerIdx < 0) throw new Error("Station inventory header row not found");
  const header = rows[headerIdx].map((h) => h.trim());
  const col: Record<string, number> = {};
  header.forEach((h, i) => (col[h] = i));
  for (const name of REQUIRED) {
    if (col[name] === undefined) throw new Error(`Station inventory is missing column "${name}"`);
  }

  const stations: Station[] = [];
  for (const r of rows.slice(headerIdx + 1)) {
    const climateId = (r[col["Climate ID"]] ?? "").trim().toUpperCase();
    const stationId = toInt(r[col["Station ID"]]);
    if (!climateId || stationId === null) continue;
    stations.push({
      name: (r[col["Name"]] ?? "").trim(),
      province: (r[col["Province"]] ?? "").trim(),
      climateId,
      stationId,
      lat: toFloat(r[col["Latitude (Decimal Degrees)"]]),
      lon: toFloat(r[col["Longitude (Decimal Degrees)"]]),
      elevation: toFloat(r[col["Elevation (m)"]]),
      hourly: range(r[col["HLY First Year"]], r[col["HLY Last Year"]]),
      daily: range(r[col["DLY First Year"]], r[col["DLY Last Year"]]),
    });
  }
  if (stations.length === 0) throw new Error("Station inventory contains no stations");
  return { modified, stations };
}

/** Case-insensitive search by station name (substring) or Climate ID (prefix). */
export function searchStations(stations: Station[], query: string, limit = 50): Station[] {
  const q = query.trim().toUpperCase();
  if (!q) return [];
  const idHits: Station[] = [];
  const nameHits: Station[] = [];
  for (const s of stations) {
    if (s.climateId.startsWith(q)) idHits.push(s);
    else if (s.name.toUpperCase().includes(q)) nameHits.push(s);
  }
  // Exact Climate ID first, then other ID prefixes, then names starting with
  // the query, then other name matches.
  idHits.sort((a, b) => Number(b.climateId === q) - Number(a.climateId === q));
  nameHits.sort(
    (a, b) =>
      Number(b.name.toUpperCase().startsWith(q)) - Number(a.name.toUpperCase().startsWith(q)) ||
      a.name.localeCompare(b.name),
  );
  return [...idHits, ...nameHits].slice(0, limit);
}
