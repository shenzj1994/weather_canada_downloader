import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { BULK_URL, createApp } from "../src/server/app.ts";
import { parseInventory } from "../src/shared/inventory.ts";
import { encodeStations, stationLookup } from "../src/shared/stations.ts";

const SNAPSHOT = readFileSync(new URL("../../src/canada_weather/data/Station Inventory EN.csv", import.meta.url), "utf-8");
const inventory = parseInventory(SNAPSHOT);
const stations = stationLookup(encodeStations(inventory));
const NOW = () => new Date("2026-11-08T00:00:00Z");

const DAILY = [
  '﻿"Longitude (x)","Latitude (y)","Station Name","Climate ID","Date/Time","Year","Month","Day","Data Quality","Max Temp (°C)","Max Temp Flag"',
  '"-123.18","49.19","VANCOUVER INTL A","1108395","2024-01-01","2024","01","01","","6.3",""',
  '"-123.18","49.19","VANCOUVER INTL A","1108395","2024-01-02","2024","01","02","","",""',
].join("\n");

function mockFetch(bulk: (url: URL) => Response) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input));
    if (url.href.startsWith(BULK_URL)) return bulk(url);
    return new Response("not found", { status: 404 });
  }) as unknown as typeof fetch;
}

describe("api", () => {
  it("reports health and the inventory date", async () => {
    const app = createApp({ stations, fetch: mockFetch(() => new Response("")), now: NOW });
    expect(await (await app.request("/api/health")).json()).toEqual({ ok: true, inventory: inventory.modified });
    expect((await app.request("/api/inventory")).status).toBe(404);
  });

  it("proxies a chunk, resolving the station ID and dropping empty rows", async () => {
    let seen: URL | undefined;
    const app = createApp({
      stations,
      fetch: mockFetch((url) => {
        seen = url;
        return new Response(DAILY, { headers: { "Content-Type": "text/csv" } });
      }),
      now: NOW,
    });
    const res = await app.request("/api/chunk?climateId=1108395&timeframe=daily&year=2024");
    expect(res.status).toBe(200);
    expect(seen?.searchParams.get("stationID")).toBe("51442");
    expect(seen?.searchParams.get("timeframe")).toBe("2");
    const text = await res.text();
    expect(text.trim().split("\n")).toHaveLength(2);
    expect(res.headers.get("X-Row-Count")).toBe("1");
  });

  it("asks for UTC hourly data", async () => {
    let seen: URL | undefined;
    const app = createApp({
      stations,
      fetch: mockFetch((url) => {
        seen = url;
        return new Response(DAILY);
      }),
      now: NOW,
    });
    await app.request("/api/chunk?climateId=1108395&timeframe=hourly&year=2024&month=3&tz=utc");
    expect(seen?.searchParams.get("time")).toBe("UTC");
    expect(seen?.searchParams.get("Month")).toBe("3");
  });

  it("rejects requests outside the station record", async () => {
    const app = createApp({ stations, fetch: mockFetch(() => new Response(DAILY)), now: NOW });
    const before = await app.request("/api/chunk?climateId=1108395&timeframe=daily&year=2000");
    expect(before.status).toBe(400);
    const future = await app.request("/api/chunk?climateId=1108395&timeframe=hourly&year=2027&month=1");
    expect(future.status).toBe(400);
    const unknown = await app.request("/api/chunk?climateId=NOPE&timeframe=daily&year=2020");
    expect(unknown.status).toBe(404);
    for (const tf of ["weekly", "monthly"]) {
      const badTf = await app.request(`/api/chunk?climateId=1108395&timeframe=${tf}&year=2020`);
      expect(badTf.status).toBe(400);
    }
  });

  it("returns 204 when ECCC answers with HTML", async () => {
    const app = createApp({
      stations,
      fetch: mockFetch(() => new Response("<html></html>", { headers: { "Content-Type": "text/html" } })),
      now: NOW,
    });
    const res = await app.request("/api/chunk?climateId=1108395&timeframe=daily&year=2024");
    expect(res.status).toBe(204);
  });
});
