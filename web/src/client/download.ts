/**
 * Fetch every period of one or more stations through /api/chunk and merge
 * them into a single CSV Blob with one header row: stations in the order
 * given, each in chronological order.
 */

import type { Timeframe } from "../shared/inventory.ts";
import { fmtYm, type YearMonth } from "../shared/range.ts";

export interface StationJob {
  climateId: string;
  periods: YearMonth[];
}

export interface DownloadOptions {
  timeframe: Timeframe;
  timezone: "lst" | "utc";
  dropEmpty: boolean;
  /** Throw on the first failed period (one station) instead of skipping the station (several). */
  failFast: boolean;
}

export interface DownloadResult {
  blob: Blob;
  filename: string;
  rows: number;
  /** Rows per Climate ID. */
  rowsByStation: Map<string, number>;
  /** Stations left out of the file because a period failed to download. */
  failed: { climateId: string; error: string }[];
}

export interface Progress {
  done: number;
  total: number;
}

const CONCURRENCY = 4;
const RETRIES = 3;

const slug = (s: string) => s.replace(/[^A-Za-z0-9]+/g, "_").replace(/^_|_$/g, "");

/** File name such as `1108395_VANCOUVER_INTL_A_daily_2020_2024.csv` or `5_stations_hourly_UTC_….csv`. */
export function filenameFor(
  stations: { climateId: string; name: string }[],
  opts: Pick<DownloadOptions, "timeframe" | "timezone">,
  start: YearMonth,
  end: YearMonth,
): string {
  const who =
    stations.length === 1 ? `${stations[0].climateId}_${slug(stations[0].name)}` : `${stations.length}_stations`;
  const span = opts.timeframe === "hourly" ? `${fmtYm(start)}_${fmtYm(end)}` : `${start.year}_${end.year}`;
  const tz = opts.timeframe === "hourly" ? `_${opts.timezone.toUpperCase()}` : "";
  return `${who}_${opts.timeframe}${tz}_${span}.csv`;
}

function chunkUrl(climateId: string, p: YearMonth, opts: DownloadOptions): string {
  const q = new URLSearchParams({
    climateId,
    timeframe: opts.timeframe,
    year: String(p.year),
    month: String(p.month),
    tz: opts.timezone,
    dropEmpty: opts.dropEmpty ? "1" : "0",
  });
  return `/api/chunk?${q}`;
}

async function errorMessage(res: Response): Promise<string> {
  try {
    const body = (await res.json()) as { error?: string };
    if (body.error) return body.error;
  } catch {
    /* not JSON */
  }
  return `Server responded ${res.status}`;
}

async function fetchChunk(url: string, signal: AbortSignal): Promise<string> {
  let lastError = "";
  for (let attempt = 0; attempt <= RETRIES; attempt++) {
    if (attempt) await new Promise((r) => setTimeout(r, 1000 * 2 ** (attempt - 1)));
    signal.throwIfAborted();
    let res: Response;
    try {
      res = await fetch(url, { signal });
    } catch (err) {
      if (signal.aborted) throw err;
      lastError = (err as Error).message;
      continue;
    }
    if (res.status === 204) return "";
    if (res.ok) return res.text();
    lastError = await errorMessage(res);
    if (res.status < 500 && res.status !== 429) break; // not worth retrying
  }
  throw new Error(lastError || "Download failed");
}

export async function downloadMerged(
  jobs: StationJob[],
  opts: DownloadOptions,
  filename: string,
  { signal, onProgress }: { signal: AbortSignal; onProgress: (p: Progress) => void },
): Promise<DownloadResult> {
  const tasks = jobs.flatMap((job, j) => job.periods.map((p) => ({ j, p })));
  const texts: string[][] = jobs.map((job) => new Array(job.periods.length).fill(""));
  const offsets = jobs.map((_, j) => tasks.findIndex((t) => t.j === j));
  const failed = new Map<number, string>();
  let next = 0;
  let done = 0;
  onProgress({ done, total: tasks.length });

  async function worker() {
    while (next < tasks.length) {
      const i = next++;
      const { j, p } = tasks[i];
      if (!failed.has(j)) {
        try {
          texts[j][i - offsets[j]] = await fetchChunk(chunkUrl(jobs[j].climateId, p, opts), signal);
        } catch (err) {
          if (signal.aborted) throw err;
          const label = opts.timeframe === "hourly" ? fmtYm(p) : String(p.year);
          const message = `${label}: ${(err as Error).message}`;
          if (opts.failFast) throw new Error(`Failed to download ${message}`);
          if (!failed.has(j)) failed.set(j, message);
        }
      }
      onProgress({ done: ++done, total: tasks.length });
    }
  }
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, tasks.length) }, worker));

  // Each chunk is a CSV with a header line; keep the first header only.
  const parts: string[] = ["﻿"]; // BOM so spreadsheet apps read °C correctly
  let header: string | null = null;
  let rows = 0;
  const rowsByStation = new Map<string, number>();
  jobs.forEach((job, j) => {
    if (failed.has(j)) return;
    let stationRows = 0;
    for (const text of texts[j]) {
      if (!text) continue;
      const nl = text.indexOf("\n");
      const body = nl < 0 ? "" : text.slice(nl + 1);
      if (header === null) {
        header = text.slice(0, nl < 0 ? text.length : nl + 1);
        parts.push(header);
      }
      if (body) {
        parts.push(body);
        stationRows += body.split("\n").length - 1;
      }
    }
    rowsByStation.set(job.climateId, stationRows);
    rows += stationRows;
  });
  const failedList = [...failed].map(([j, error]) => ({ climateId: jobs[j].climateId, error }));
  if (rows === 0) {
    const why = failedList.map((f) => `${f.climateId} (${f.error})`).join("; ");
    throw new Error(`ECCC returned no data for this selection.${why ? ` Failed: ${why}` : ""}`);
  }
  return {
    blob: new Blob(parts, { type: "text/csv;charset=utf-8" }),
    filename,
    rows,
    rowsByStation,
    failed: failedList,
  };
}

export function saveBlob(blob: Blob, filename: string): void {
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 60_000);
}
