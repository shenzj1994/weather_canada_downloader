/**
 * Fetch every period of a request through /api/chunk and merge them into a
 * single CSV Blob, in chronological order, with one header row.
 */

import type { Timeframe } from "../shared/inventory.ts";
import { fmtYm, periods, type YearMonth } from "../shared/range.ts";

export interface DownloadRequest {
  climateId: string;
  stationName: string;
  timeframe: Timeframe;
  start: YearMonth;
  end: YearMonth;
  timezone: "lst" | "utc";
  dropEmpty: boolean;
}

export interface DownloadResult {
  blob: Blob;
  filename: string;
  rows: number;
}

export interface Progress {
  done: number;
  total: number;
}

const CONCURRENCY = 4;
const RETRIES = 3;

export function chunkCount(req: Pick<DownloadRequest, "timeframe" | "start" | "end">): number {
  return periods(req.timeframe, req.start, req.end).length;
}

export function filenameFor(req: DownloadRequest): string {
  const slug = req.stationName.replace(/[^A-Za-z0-9]+/g, "_").replace(/^_|_$/g, "");
  const span =
    req.timeframe === "hourly"
      ? `${fmtYm(req.start)}_${fmtYm(req.end)}`
      : `${req.start.year}_${req.end.year}`;
  const tz = req.timeframe === "hourly" ? `_${req.timezone.toUpperCase()}` : "";
  return `${req.climateId}_${slug}_${req.timeframe}${tz}_${span}.csv`;
}

function chunkUrl(req: DownloadRequest, p: YearMonth): string {
  const q = new URLSearchParams({
    climateId: req.climateId,
    timeframe: req.timeframe,
    year: String(p.year),
    month: String(p.month),
    tz: req.timezone,
    dropEmpty: req.dropEmpty ? "1" : "0",
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

export async function downloadCsv(
  req: DownloadRequest,
  { signal, onProgress }: { signal: AbortSignal; onProgress: (p: Progress) => void },
): Promise<DownloadResult> {
  const todo = periods(req.timeframe, req.start, req.end);
  const results: string[] = new Array(todo.length);
  let next = 0;
  let done = 0;
  onProgress({ done, total: todo.length });

  async function worker() {
    while (next < todo.length) {
      const i = next++;
      const p = todo[i];
      try {
        results[i] = await fetchChunk(chunkUrl(req, p), signal);
      } catch (err) {
        if (signal.aborted) throw err;
        const label = req.timeframe === "hourly" ? fmtYm(p) : String(p.year);
        throw new Error(`Failed to download ${label}: ${(err as Error).message}`);
      }
      onProgress({ done: ++done, total: todo.length });
    }
  }
  await Promise.all(Array.from({ length: Math.min(CONCURRENCY, todo.length) }, worker));

  // Each chunk is a CSV with a header line; keep the first header only.
  const parts: string[] = ["﻿"]; // BOM so spreadsheet apps read °C correctly
  let header: string | null = null;
  let rows = 0;
  for (const text of results) {
    if (!text) continue;
    const nl = text.indexOf("\n");
    const body = nl < 0 ? "" : text.slice(nl + 1);
    if (header === null) {
      header = text.slice(0, nl < 0 ? text.length : nl + 1);
      parts.push(header);
    }
    if (body) {
      parts.push(body);
      rows += body.split("\n").length - 1;
    }
  }
  if (header === null || rows === 0) {
    throw new Error("ECCC returned no data for this selection.");
  }
  return {
    blob: new Blob(parts, { type: "text/csv;charset=utf-8" }),
    filename: filenameFor(req),
    rows,
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
