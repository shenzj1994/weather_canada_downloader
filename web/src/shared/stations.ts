/**
 * Condensed station list served to the browser and bundled into the server.
 *
 * Generated at build time from ECCC's "Station Inventory EN.csv" (see
 * scripts/build-stations.ts) and keeps only the fields the app uses, as rows
 * of arrays rather than objects to keep the file small.
 */

import type { Inventory, Station } from "./inventory.ts";

export const STATIONS_COLUMNS = [
  "climateId",
  "stationId",
  "name",
  "province",
  "lat",
  "lon",
  "elevation",
  "hourlyFirst",
  "hourlyLast",
  "dailyFirst",
  "dailyLast",
] as const;

type Row = [
  string,
  number,
  string,
  string,
  number | null,
  number | null,
  number | null,
  number | null,
  number | null,
  number | null,
  number | null,
];

export interface StationsFile {
  version: 1;
  /** ECCC's "Modified Date" of the source inventory (ISO 8601, UTC). */
  modified: string | null;
  columns: typeof STATIONS_COLUMNS;
  stations: Row[];
}

export function encodeStations(inv: Inventory): StationsFile {
  return {
    version: 1,
    modified: inv.modified,
    columns: STATIONS_COLUMNS,
    stations: inv.stations.map((s) => [
      s.climateId,
      s.stationId,
      s.name,
      s.province,
      s.lat,
      s.lon,
      s.elevation,
      s.hourly?.[0] ?? null,
      s.hourly?.[1] ?? null,
      s.daily?.[0] ?? null,
      s.daily?.[1] ?? null,
    ]),
  };
}

const pair = (a: number | null, b: number | null): [number, number] | null =>
  a !== null && b !== null ? [a, b] : null;

function decodeRow(r: Row): Station {
  return {
    climateId: r[0],
    stationId: r[1],
    name: r[2],
    province: r[3],
    lat: r[4],
    lon: r[5],
    elevation: r[6],
    hourly: pair(r[7], r[8]),
    daily: pair(r[9], r[10]),
  };
}

function checkVersion(file: StationsFile) {
  if (file.version !== 1) throw new Error(`Unsupported stations file version ${String(file.version)}`);
}

/** Every station, for the browser's search. */
export function decodeStations(file: StationsFile): Inventory {
  checkVersion(file);
  return { modified: file.modified, stations: file.stations.map(decodeRow) };
}

/** What the server needs: the inventory date and a lookup by Climate ID. */
export interface StationLookup {
  modified: string | null;
  get(climateId: string): Station | undefined;
}

/** Index the rows only and decode a station when it is looked up (cheap Worker cold starts). */
export function stationLookup(file: StationsFile): StationLookup {
  checkVersion(file);
  const rows = new Map(file.stations.map((r) => [r[0], r]));
  return {
    modified: file.modified,
    get: (id) => {
      const r = rows.get(id);
      return r && decodeRow(r);
    },
  };
}

export function inventoryLookup(inv: Inventory): StationLookup {
  const byId = new Map(inv.stations.map((s) => [s.climateId, s]));
  return { modified: inv.modified, get: (id) => byId.get(id) };
}
