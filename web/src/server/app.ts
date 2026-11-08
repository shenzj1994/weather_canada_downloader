/**
 * Hono API. It only uses web-standard APIs (fetch, Request, Response), so the
 * same app runs on Node (see node.ts), Cloudflare Workers or Vercel.
 *
 *   GET /api/inventory   station inventory as JSON (live from ECCC, cached)
 *   GET /api/chunk       one period of data for one station, as CSV
 *   GET /api/health
 */

import { Hono } from "hono";
import { parseCsv, toCsv } from "../shared/csv.ts";
import { parseInventory, TIMEFRAMES, type Inventory, type Station, type Timeframe } from "../shared/inventory.ts";
import { availability, isPeriodFinal, validateRange, ym } from "../shared/range.ts";
import { filterRows } from "../shared/rows.ts";

export const INVENTORY_URL =
  "https://collaboration.cmc.ec.gc.ca/cmc/climate/Get_More_Data_Plus_de_donnees/Station%20Inventory%20EN.csv";
export const BULK_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html";
const TIMEFRAME_CODE: Record<Timeframe, number> = { hourly: 1, daily: 2, monthly: 3 };
const USER_AGENT = "canada-weather-web (+https://github.com/shenzj1994/weather_canada_downloader)";

export interface AppOptions {
  /** fetch implementation (injectable for tests). */
  fetch?: typeof fetch;
  /** Bundled inventory snapshot, used when ECCC cannot be reached. */
  loadFallbackInventory?: () => Promise<string | null>;
  /** How long a loaded inventory is reused before refreshing (ms). */
  inventoryTtlMs?: number;
  /** Clock (injectable for tests). */
  now?: () => Date;
}

interface LoadedInventory extends Inventory {
  source: "live" | "snapshot";
  loadedAt: number;
  byClimateId: Map<string, Station>;
}

class HttpError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

async function fetchWithRetry(
  doFetch: typeof fetch,
  url: string,
  { retries = 2, timeoutMs = 30_000 } = {},
): Promise<Response> {
  let lastErr: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    if (attempt) await new Promise((r) => setTimeout(r, 500 * 2 ** attempt));
    try {
      const res = await doFetch(url, {
        headers: { "User-Agent": USER_AGENT },
        signal: AbortSignal.timeout(timeoutMs),
      });
      if (res.status === 429 || res.status >= 500) {
        lastErr = new Error(`ECCC responded ${res.status}`);
        continue;
      }
      return res;
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

function index(inv: Inventory, source: LoadedInventory["source"], now: number): LoadedInventory {
  return { ...inv, source, loadedAt: now, byClimateId: new Map(inv.stations.map((s) => [s.climateId, s])) };
}

export function createApp(options: AppOptions = {}) {
  const doFetch = options.fetch ?? fetch;
  const ttl = options.inventoryTtlMs ?? 12 * 60 * 60 * 1000;
  const now = options.now ?? (() => new Date());

  let current: LoadedInventory | null = null;
  let pending: Promise<LoadedInventory> | null = null;

  async function loadInventory(): Promise<LoadedInventory> {
    const t = now().getTime();
    try {
      const res = await fetchWithRetry(doFetch, INVENTORY_URL, { retries: 1, timeoutMs: 20_000 });
      if (!res.ok) throw new Error(`ECCC responded ${res.status}`);
      const live = parseInventory(await res.text());
      return index(live, "live", t);
    } catch (err) {
      console.warn(`Could not load the live station inventory: ${(err as Error).message}`);
      if (current) return { ...current, loadedAt: t }; // keep serving the last good copy
      const text = await options.loadFallbackInventory?.();
      if (!text) throw new HttpError(503, "The station inventory is unavailable. Try again later.");
      return index(parseInventory(text), "snapshot", t);
    }
  }

  async function getInventory(): Promise<LoadedInventory> {
    // Retry ECCC sooner when serving the bundled snapshot.
    const maxAge = current?.source === "snapshot" ? Math.min(ttl, 10 * 60 * 1000) : ttl;
    const fresh = current && now().getTime() - current.loadedAt < maxAge;
    if (fresh) return current!;
    pending ??= loadInventory()
      .then((inv) => (current = inv))
      .finally(() => (pending = null));
    // Serve a stale copy while refreshing in the background.
    return current ?? pending;
  }

  const app = new Hono();

  app.onError((err, c) => {
    if (err instanceof HttpError) return c.json({ error: err.message }, err.status as 400);
    console.error(err);
    return c.json({ error: "Internal server error" }, 500);
  });

  app.get("/api/health", (c) => c.json({ ok: true }));

  app.get("/api/inventory", async (c) => {
    const inv = await getInventory();
    c.header("Cache-Control", "public, max-age=3600");
    return c.json({ modified: inv.modified, source: inv.source, stations: inv.stations });
  });

  app.get("/api/chunk", async (c) => {
    const q = c.req.query();
    const timeframe = q.timeframe as Timeframe;
    if (!TIMEFRAMES.includes(timeframe)) throw new HttpError(400, "timeframe must be hourly, daily or monthly");
    const year = Number(q.year);
    const month = timeframe === "hourly" ? Number(q.month) : 1;
    const tz = (q.tz ?? "lst").toLowerCase();
    if (tz !== "lst" && tz !== "utc") throw new HttpError(400, "tz must be lst or utc");
    const dropEmpty = q.dropEmpty !== "0";

    const inv = await getInventory();
    const station = inv.byClimateId.get((q.climateId ?? "").trim().toUpperCase());
    if (!station) throw new HttpError(404, "Unknown Climate ID");

    // Monthly data comes back in one request for the whole record and is
    // filtered to [year, endYear]; the other timeframes are one period each.
    const endYear = timeframe === "monthly" ? Number(q.endYear ?? year) : year;
    const start = ym(year, month);
    const end = timeframe === "hourly" ? start : ym(endYear, 12);
    const avail = availability(station, timeframe, inv.modified, now());
    const invalid = validateRange(avail, timeframe, start, end);
    if (invalid) throw new HttpError(400, invalid);

    const params = new URLSearchParams({
      format: "csv",
      stationID: String(station.stationId),
      Year: String(year),
      Month: String(month),
      Day: "1",
      timeframe: String(TIMEFRAME_CODE[timeframe]),
      submit: "Download Data",
    });
    if (timeframe === "hourly" && tz === "utc") params.set("time", "UTC");

    let res: Response;
    try {
      res = await fetchWithRetry(doFetch, `${BULK_URL}?${params}`);
    } catch (err) {
      throw new HttpError(502, `ECCC could not be reached: ${(err as Error).message}`);
    }
    if (!res.ok) throw new HttpError(502, `ECCC responded ${res.status}`);
    const text = await res.text();
    const contentType = res.headers.get("Content-Type") ?? "";
    // Unknown stations return an HTML page with status 200.
    if (contentType.includes("html") || !text.replace(/^﻿/, "").trimStart().startsWith('"')) {
      return c.body(null, 204);
    }

    const rows = filterRows(parseCsv(text), {
      dropEmpty,
      years: timeframe === "monthly" ? [year, endYear] : undefined,
    });
    const final = isPeriodFinal(timeframe, start, now());
    c.header("Content-Type", "text/csv; charset=utf-8");
    c.header("X-Row-Count", String(rows.length - 1));
    c.header("Cache-Control", final ? "public, max-age=86400" : "public, max-age=900");
    return c.body(toCsv(rows));
  });

  app.all("/api/*", (c) => c.json({ error: "Not found" }, 404));

  return app;
}
