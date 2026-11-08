/** Row filtering applied to each ECCC CSV chunk before it is merged. */

/** Columns that identify a row rather than carry a measurement. */
const ID_COLUMNS = new Set([
  "Longitude (x)",
  "Latitude (y)",
  "Station Name",
  "Climate ID",
  "Date/Time",
  "Date/Time (LST)",
  "Date/Time (UTC)",
  "Year",
  "Month",
  "Day",
  "Time (LST)",
  "Time (UTC)",
  "Data Quality",
]);

const isFlag = (h: string) => h === "Flag" || h.endsWith(" Flag");
const isBlank = (v: string | undefined) => v === undefined || v.trim() === "" || v.trim() === "NA";

export interface FilterOptions {
  /** Drop rows whose measurements are all blank (future dates, gaps). */
  dropEmpty: boolean;
  /** Keep only rows whose "Year" is within this inclusive range. */
  years?: [number, number];
}

/**
 * Filter parsed CSV rows (header first). Returns the header plus kept rows.
 * Rows of the wrong width (e.g. a trailing blank line) are dropped.
 */
export function filterRows(rows: string[][], opts: FilterOptions): string[][] {
  if (rows.length === 0) return rows;
  const header = rows[0];
  const measure = header.map((h, i) => (!ID_COLUMNS.has(h) && !isFlag(h) ? i : -1)).filter((i) => i >= 0);
  const yearIdx = header.indexOf("Year");
  const out = [header];
  for (const r of rows.slice(1)) {
    if (r.length !== header.length) continue;
    if (opts.years && yearIdx >= 0) {
      const y = Number(r[yearIdx]);
      if (!(y >= opts.years[0] && y <= opts.years[1])) continue;
    }
    if (opts.dropEmpty && measure.length && measure.every((i) => isBlank(r[i]))) continue;
    out.push(r);
  }
  return out;
}
