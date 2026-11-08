import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";
import { BULK_URL, createApp, INVENTORY_URL } from "../src/server/app.ts";

const SNAPSHOT = readFileSync(new URL("../../src/canada_weather/data/Station Inventory EN.csv", import.meta.url), "utf-8");
const NOW = () => new Date("2026-11-08T00:00:00Z");

const DAILY = [
  '﻿"Longitude (x)","Latitude (y)","Station Name","Climate ID","Date/Time","Year","Month","Day","Data Quality","Max Temp (°C)","Max Temp Flag"',
  '"-123.18","49.19","VANCOUVER INTL A","1108395","2024-01-01","2024","01","01","","6.3",""',
  '"-123.18","49.19","VANCOUVER INTL A","1108395","2024-01-02","2024","01","02","","",""',
].join("\n");

function mockFetch(bulk: (url: URL) => Response) {
  return vi.fn(async (input: RequestInfo | URL) => {
    const url = new URL(String(input));
    if (url.href === INVENTORY_URL) return new Response(SNAPSHOT);
    if (url.href.startsWith(BULK_URL)) return bulk(url);
    return new Response("not found", { status: 404 });
  }) as unknown as typeof fetch;
}

describe("api", () => {
  it("serves the inventory", async () => {
    const app = createApp({ fetch: mockFetch(() => new Response("")), now: NOW });
    const res = await app.request("/api/inventory");
    expect(res.status).toBe(200);
    const body = (await res.json()) as { source: string; stations: unknown[] };
    expect(body.source).toBe("live");
    expect(body.stations.length).toBeGreaterThan(8000);
  });

  it("falls back to the bundled snapshot", async () => {
    const failing = vi.fn(async () => {
      throw new Error("offline");
    }) as unknown as typeof fetch;
    const app = createApp({ fetch: failing, loadFallbackInventory: async () => SNAPSHOT, now: NOW });
    const body = (await (await app.request("/api/inventory")).json()) as { source: string };
    expect(body.source).toBe("snapshot");
  });

  it("returns 503 when no inventory can be loaded", async () => {
    const failing = vi.fn(async () => new Response("", { status: 404 })) as unknown as typeof fetch;
    const app = createApp({ fetch: failing, now: NOW });
    expect((await app.request("/api/inventory")).status).toBe(503);
  });

  it("proxies a chunk, resolving the station ID and dropping empty rows", async () => {
    let seen: URL | undefined;
    const app = createApp({
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
    const app = createApp({ fetch: mockFetch(() => new Response(DAILY)), now: NOW });
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
      fetch: mockFetch(() => new Response("<html></html>", { headers: { "Content-Type": "text/html" } })),
      now: NOW,
    });
    const res = await app.request("/api/chunk?climateId=1108395&timeframe=daily&year=2024");
    expect(res.status).toBe(204);
  });
});
