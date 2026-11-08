/**
 * What can be requested for a station, and how a request is split into the
 * per-period calls the ECCC bulk-data endpoint expects.
 *
 * Endpoint behaviour (same as the Python package):
 * - hourly returns one calendar month per request;
 * - daily returns one calendar year per request.
 */

import type { Station, Timeframe } from "./inventory.ts";

export interface YearMonth {
  year: number;
  month: number; // 1-12
}

export interface Availability {
  start: YearMonth;
  end: YearMonth;
  /** The inventory's last year was extended to today because the station looks active. */
  extended: boolean;
}

export const ym = (year: number, month: number): YearMonth => ({ year, month });
export const ymIndex = (v: YearMonth): number => v.year * 12 + (v.month - 1);
export const fmtYm = (v: YearMonth): string => `${v.year}-${String(v.month).padStart(2, "0")}`;

/** Today's year/month in UTC. */
export function currentYm(now: Date = new Date()): YearMonth {
  return ym(now.getUTCFullYear(), now.getUTCMonth() + 1);
}

/**
 * Range a station can be downloaded for at a timeframe, or null if none.
 *
 * The inventory is a snapshot that can lag behind the newest data, so a
 * station whose record runs to (roughly) the snapshot year is treated as
 * active and allowed up to the current month.
 */
export function availability(
  station: Station,
  timeframe: Timeframe,
  inventoryModified: string | null,
  now: Date = new Date(),
): Availability | null {
  const record = station[timeframe];
  if (!record) return null;
  const today = currentYm(now);
  const refYear = inventoryModified ? new Date(inventoryModified).getUTCFullYear() : today.year;
  const extended = record[1] >= refYear - 1 && record[1] < today.year;
  const lastYear = record[1] >= refYear - 1 ? today.year : record[1];
  const start = ym(record[0], 1);
  let end = ym(lastYear, 12);
  if (ymIndex(end) > ymIndex(today)) end = today;
  if (ymIndex(start) > ymIndex(end)) return null;
  return { start, end, extended };
}

/** Return an error message if [start, end] is not a valid request, else null. */
export function validateRange(
  avail: Availability | null,
  timeframe: Timeframe,
  start: YearMonth,
  end: YearMonth,
): string | null {
  if (!avail) return `This station has no ${timeframe} data.`;
  for (const v of [start, end]) {
    if (!Number.isInteger(v.year) || !Number.isInteger(v.month) || v.month < 1 || v.month > 12) {
      return "Invalid date.";
    }
  }
  // Daily requests are whole years.
  const s = timeframe === "hourly" ? start : ym(start.year, 1);
  const e = timeframe === "hourly" ? end : ym(end.year, 12);
  if (ymIndex(s) > ymIndex(e)) return "The start must not be after the end.";
  const lo = timeframe === "hourly" ? avail.start : ym(avail.start.year, 1);
  const hi = timeframe === "hourly" ? avail.end : ym(avail.end.year, 12);
  if (ymIndex(s) < ymIndex(lo) || ymIndex(e) > ymIndex(hi)) {
    const span =
      timeframe === "hourly"
        ? `${fmtYm(avail.start)} to ${fmtYm(avail.end)}`
        : `${avail.start.year} to ${avail.end.year}`;
    return `${timeframe[0].toUpperCase()}${timeframe.slice(1)} data is only available from ${span}.`;
  }
  return null;
}

/** The (year, month) of each request needed for [start, end]. */
export function periods(timeframe: Timeframe, start: YearMonth, end: YearMonth): YearMonth[] {
  const out: YearMonth[] = [];
  if (timeframe === "daily") {
    for (let y = start.year; y <= end.year; y++) out.push(ym(y, 1));
    return out;
  }
  for (let i = ymIndex(start); i <= ymIndex(end); i++) out.push(ym(Math.floor(i / 12), (i % 12) + 1));
  return out;
}

/** Whether a period is complete, i.e. its data should no longer change much. */
export function isPeriodFinal(timeframe: Timeframe, period: YearMonth, now: Date = new Date()): boolean {
  const today = currentYm(now);
  if (timeframe === "hourly") return ymIndex(period) < ymIndex(today);
  return period.year < today.year;
}

/**
 * Periods to request for one station: the user's [start, end] intersected
 * with what the station has. Empty when they don't overlap.
 */
export function stationPeriods(
  avail: Availability | null,
  timeframe: Timeframe,
  start: YearMonth,
  end: YearMonth,
): YearMonth[] {
  if (!avail) return [];
  const lo = ymIndex(start) > ymIndex(avail.start) ? start : avail.start;
  const hi = ymIndex(end) < ymIndex(avail.end) ? end : avail.end;
  if (timeframe === "daily" ? lo.year > hi.year : ymIndex(lo) > ymIndex(hi)) return [];
  return periods(timeframe, lo, hi);
}
