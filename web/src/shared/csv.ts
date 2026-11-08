/**
 * Minimal RFC 4180 CSV reader/writer.
 *
 * ECCC quotes every field and some values contain commas (station names such
 * as "VANCOUVER INT'L A" are fine, but hourly "Weather" can be "Rain,Fog"),
 * so a naive split(",") is not good enough.
 */

/** Parse CSV text into rows of fields. A leading UTF-8 BOM is ignored. */
export function parseCsv(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;
  let i = text.charCodeAt(0) === 0xfeff ? 1 : 0;
  const n = text.length;

  while (i < n) {
    const ch = text[i];
    if (inQuotes) {
      if (ch === '"') {
        if (text[i + 1] === '"') {
          field += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
      } else {
        field += ch;
      }
      i += 1;
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n" || ch === "\r") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      if (ch === "\r" && text[i + 1] === "\n") i += 1;
    } else {
      field += ch;
    }
    i += 1;
  }
  if (field !== "" || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

function quote(value: string): string {
  return `"${value.replace(/"/g, '""')}"`;
}

/** Serialise rows as CSV, quoting every field (as ECCC does), CRLF-free. */
export function toCsv(rows: string[][]): string {
  return rows.map((r) => r.map(quote).join(",")).join("\n") + (rows.length ? "\n" : "");
}
