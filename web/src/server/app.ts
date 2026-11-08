/**
 * Hono API. It only uses web-standard APIs (fetch, Request, Response), so the
 * same app runs on Node (node.ts, used by Docker) and Cloudflare Workers
 * (worker.ts).
 *
 *   GET /api/chunk       one period of data for one station, as CSV
 *   GET /api/health
 *
 * The station list itself is a static file (/stations.json) built from the
 * inventory in the repository, so the API never downloads the inventory.
 */

import { Hono } from "hono";
import { parseCsv, toCsv } from "../shared/csv.ts";
import { TIMEFRAMES, type Timeframe } from "../shared/inventory.ts";
import { availability, isPeriodFinal, validateRange, ym } from "../shared/range.ts";
import { filterRows } from "../shared/rows.ts";
import type { StationLookup } from "../shared/stations.ts";

export const BULK_URL = "https://climate.weather.gc.ca/climate_data/bulk_data_e.html";
const TIMEFRAME_CODE: Record<Timeframe, number> = { hourly: 1, daily: 2 };
const USER_AGENT = "canada-weather-web (+https://github.com/shenzj1994/weather_canada_downloader)";

export interface AppOptions {
  /** Stations used to resolve and validate Climate IDs. */
  stations: StationLookup;
  /** fetch implementation (injectable for tests). */
  fetch?: typeof fetch;
  /** Clock (injectable for tests). */
  now?: () => Date;
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

export function createApp(options: AppOptions) {
  const doFetch = options.fetch ?? fetch;
  const now = options.now ?? (() => new Date());
  const { stations } = options;
  const { modified } = stations;

  const app = new Hono();

  app.onError((err, c) => {
    if (err instanceof HttpError) return c.json({ error: err.message }, err.status as 400);
    console.error(err);
    return c.json({ error: "Internal server error" }, 500);
  });

  app.get("/api/health", (c) => c.json({ ok: true, inventory: modified }));

  app.get("/api/chunk", async (c) => {
    const q = c.req.query();
    const timeframe = q.timeframe as Timeframe;
    if (!TIMEFRAMES.includes(timeframe)) throw new HttpError(400, "timeframe must be hourly or daily");
    const year = Number(q.year);
    const month = timeframe === "hourly" ? Number(q.month) : 1;
    const tz = (q.tz ?? "lst").toLowerCase();
    if (tz !== "lst" && tz !== "utc") throw new HttpError(400, "tz must be lst or utc");
    const dropEmpty = q.dropEmpty !== "0";

    const station = stations.get((q.climateId ?? "").trim().toUpperCase());
    if (!station) throw new HttpError(404, "Unknown Climate ID");

    // One request is one period: a month for hourly, a year for daily.
    const start = ym(year, month);
    const end = timeframe === "hourly" ? start : ym(year, 12);
    const avail = availability(station, timeframe, modified, now());
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

    const rows = filterRows(parseCsv(text), { dropEmpty });
    const final = isPeriodFinal(timeframe, start, now());
    c.header("Content-Type", "text/csv; charset=utf-8");
    c.header("X-Row-Count", String(rows.length - 1));
    c.header("Cache-Control", final ? "public, max-age=86400" : "public, max-age=900");
    return c.body(toCsv(rows));
  });

  app.all("/api/*", (c) => c.json({ error: "Not found" }, 404));

  return app;
}
