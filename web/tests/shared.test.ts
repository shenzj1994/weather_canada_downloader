import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { parseCsv, toCsv } from "../src/shared/csv.ts";
import {
  parseClimateIds,
  parseInventory,
  resolveClimateIds,
  searchStations,
  type Station,
} from "../src/shared/inventory.ts";
import { availability, periods, stationPeriods, validateRange, ym } from "../src/shared/range.ts";
import { filterRows } from "../src/shared/rows.ts";

const SNAPSHOT = readFileSync(new URL("../../src/canada_weather/data/Station Inventory EN.csv", import.meta.url), "utf-8");

const station = (over: Partial<Station> = {}): Station => ({
  name: "TEST",
  province: "BRITISH COLUMBIA",
  climateId: "1",
  stationId: 1,
  lat: null,
  lon: null,
  elevation: null,
  hourly: null,
  daily: null,
  ...over,
});

describe("csv", () => {
  it("handles BOM, quotes, embedded commas and escaped quotes", () => {
    const rows = parseCsv('﻿"a","b"\r\n"x,y","say ""hi"""\n"",""\n');
    expect(rows).toEqual([["a", "b"], ["x,y", 'say "hi"'], ["", ""]]);
  });
  it("round-trips", () => {
    const rows = [["a", "b,c"], ['"q"', ""]];
    expect(parseCsv(toCsv(rows))).toEqual(rows);
  });
});

describe("inventory", () => {
  const inv = parseInventory(SNAPSHOT);
  it("parses the bundled snapshot", () => {
    expect(inv.modified).toMatch(/^\d{4}-\d{2}-\d{2}T/);
    expect(inv.stations.length).toBeGreaterThan(8000);
    const yvr = inv.stations.find((s) => s.climateId === "1108395")!;
    expect(yvr.name).toBe("VANCOUVER INTL A");
    expect(yvr.stationId).toBe(51442);
    expect(yvr.daily?.[0]).toBe(2013);
  });
  it("rejects non-inventory text", () => {
    expect(() => parseInventory("<html></html>")).toThrow();
  });
  it("searches by climate ID first, then name", () => {
    expect(searchStations(inv.stations, "1108395")[0].climateId).toBe("1108395");
    const hits = searchStations(inv.stations, "vancouver intl");
    expect(hits.map((s) => s.climateId)).toContain("1108395");
    expect(searchStations(inv.stations, "  ")).toEqual([]);
  });
});

describe("availability", () => {
  const now = new Date("2026-11-08T00:00:00Z");
  it("extends active stations to the current month", () => {
    const a = availability(station({ hourly: [2013, 2026] }), "hourly", "2026-06-03T23:30:00Z", now)!;
    expect(a.start).toEqual(ym(2013, 1));
    expect(a.end).toEqual(ym(2026, 11));
  });
  it("extends stations that ended the year before the snapshot", () => {
    const a = availability(station({ daily: [2000, 2025] }), "daily", "2026-06-03T23:30:00Z", now)!;
    expect(a.end.year).toBe(2026);
    expect(a.extended).toBe(true);
  });
  it("keeps closed stations to their record", () => {
    const a = availability(station({ daily: [1984, 1996] }), "daily", "2026-06-03T23:30:00Z", now)!;
    expect(a.end).toEqual(ym(1996, 12));
  });
  it("returns null without data", () => {
    expect(availability(station(), "hourly", null, now)).toBeNull();
  });
  it("validates ranges against availability", () => {
    const a = availability(station({ hourly: [1990, 2000], daily: [1990, 2000] }), "hourly", null, now);
    expect(validateRange(a, "hourly", ym(1990, 1), ym(2000, 12))).toBeNull();
    expect(validateRange(a, "hourly", ym(1989, 12), ym(1990, 2))).toMatch(/only available/);
    expect(validateRange(a, "hourly", ym(2000, 2), ym(2000, 1))).toMatch(/after/);
    expect(validateRange(null, "daily", ym(2000, 1), ym(2000, 1))).toMatch(/no daily/);
  });
});

describe("periods", () => {
  it("splits hourly by month and daily by year", () => {
    expect(periods("hourly", ym(2023, 11), ym(2024, 2))).toEqual([ym(2023, 11), ym(2023, 12), ym(2024, 1), ym(2024, 2)]);
    expect(periods("daily", ym(2020, 1), ym(2022, 12))).toEqual([ym(2020, 1), ym(2021, 1), ym(2022, 1)]);
  });
});

describe("filterRows", () => {
  const header = ["Station Name", "Climate ID", "Date/Time", "Year", "Data Quality", "Max Temp (°C)", "Max Temp Flag", "Weather"];
  const rows = [
    header,
    ["A", "1", "2020-01-01", "2020", "", "1.0", "", "NA"],
    ["A", "1", "2020-01-02", "2020", "", "", "M", "NA"],
    ["A", "1", "2021-01-01", "2021", "", "", "", "Rain,Fog"],
    ["short"],
  ];
  it("drops rows whose measurements are all blank", () => {
    expect(filterRows(rows, { dropEmpty: true }).map((r) => r[2])).toEqual(["Date/Time", "2020-01-01", "2021-01-01"]);
    expect(filterRows(rows, { dropEmpty: false })).toHaveLength(4);
  });
});

describe("several stations", () => {
  const inv = parseInventory(SNAPSHOT);
  const now = new Date("2026-11-08T00:00:00Z");

  it("parses Climate IDs separated by commas, spaces and new lines", () => {
    expect(parseClimateIds(" 1108395, 1108447\n1106pf7;1108395  ")).toEqual(["1108395", "1108447", "1106PF7"]);
    expect(parseClimateIds("  ")).toEqual([]);
  });

  it("reports Climate IDs missing from the inventory", () => {
    const { found, missing } = resolveClimateIds(["1108395", "NOPE", "1108447", "0000000"], inv.stations);
    expect(found.map((s) => s.climateId)).toEqual(["1108395", "1108447"]);
    expect(missing).toEqual(["NOPE", "0000000"]);
  });

  it("downloads each station only for the part of the range it has", () => {
    const s = station({ hourly: [2000, 2010], daily: [2000, 2010] });
    const a = availability(s, "daily", "2026-06-03T23:30:00Z", now);
    expect(stationPeriods(a, "daily", ym(2008, 1), ym(2015, 12))).toEqual([ym(2008, 1), ym(2009, 1), ym(2010, 1)]);
    expect(stationPeriods(a, "daily", ym(2015, 1), ym(2020, 12))).toEqual([]);
    const h = availability(s, "hourly", "2026-06-03T23:30:00Z", now);
    expect(stationPeriods(h, "hourly", ym(2010, 11), ym(2011, 3))).toEqual([ym(2010, 11), ym(2010, 12)]);
    expect(stationPeriods(null, "hourly", ym(2010, 11), ym(2011, 3))).toEqual([]);
  });
});
