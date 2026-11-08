import { useEffect, useMemo, useRef, useState } from "react";
import { searchStations, TIMEFRAMES, type Station, type Timeframe } from "../shared/inventory.ts";
import {
  availability,
  fmtYm,
  validateRange,
  ym,
  ymIndex,
  type Availability,
  type YearMonth,
} from "../shared/range.ts";
import { chunkCount, downloadCsv, saveBlob, type DownloadResult, type Progress } from "./download.ts";

interface InventoryResponse {
  modified: string | null;
  source: "live" | "snapshot";
  stations: Station[];
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const LABEL: Record<Timeframe, string> = { hourly: "Hourly", daily: "Daily", monthly: "Monthly" };

const fmtRange = (r: [number, number] | null) => (r ? `${r[0]}–${r[1]}` : "—");
const titleCase = (s: string) => s.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());

function defaultRange(tf: Timeframe, a: Availability): [YearMonth, YearMonth] {
  if (tf === "hourly") {
    const startIdx = Math.max(ymIndex(a.start), ymIndex(a.end) - 11);
    return [ym(Math.floor(startIdx / 12), (startIdx % 12) + 1), a.end];
  }
  if (tf === "daily") return [ym(Math.max(a.start.year, a.end.year - 4), 1), ym(a.end.year, 12)];
  return [ym(a.start.year, 1), ym(a.end.year, 12)];
}

const clampYm = (v: YearMonth, a: Availability): YearMonth =>
  ymIndex(v) < ymIndex(a.start) ? a.start : ymIndex(v) > ymIndex(a.end) ? a.end : v;

export function App() {
  const [inventory, setInventory] = useState<InventoryResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [station, setStation] = useState<Station | null>(null);

  useEffect(() => {
    fetch("/api/inventory")
      .then(async (r) => {
        if (!r.ok) throw new Error(((await r.json().catch(() => ({}))) as { error?: string }).error ?? r.statusText);
        return r.json() as Promise<InventoryResponse>;
      })
      .then(setInventory)
      .catch((e: Error) => setLoadError(e.message));
  }, []);

  const results = useMemo(
    () => (inventory ? searchStations(inventory.stations, query, 50) : []),
    [inventory, query],
  );

  return (
    <div className="page">
      <header>
        <h1>Canada Weather Downloader</h1>
        <p className="muted">
          Historical observations from Environment and Climate Change Canada, merged into one CSV.
        </p>
      </header>

      <section className="card">
        <h2>1. Find a station</h2>
        {loadError && <p className="error">Could not load the station inventory: {loadError}</p>}
        {!inventory && !loadError && <p className="muted">Loading station inventory…</p>}
        {inventory && (
          <>
            <input
              className="search"
              type="search"
              autoFocus
              placeholder="Station name or Climate ID, e.g. “Vancouver Intl” or 1108395"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              aria-label="Search stations"
            />
            <p className="muted small">
              {inventory.stations.length.toLocaleString()} stations · inventory updated{" "}
              {inventory.modified?.slice(0, 10) ?? "unknown"}
              {inventory.source === "snapshot" && " (bundled snapshot; ECCC unreachable)"}
            </p>
            {query.trim() && (
              <StationList results={results} selected={station} onSelect={setStation} />
            )}
          </>
        )}
      </section>

      {station && inventory && (
        <DownloadForm key={station.climateId} station={station} inventoryModified={inventory.modified} />
      )}

      <footer className="muted small">
        Data © Environment and Climate Change Canada, from{" "}
        <a href="https://climate.weather.gc.ca/">climate.weather.gc.ca</a>, used under the{" "}
        <a href="https://open.canada.ca/en/open-government-licence-canada">
          Open Government Licence – Canada
        </a>
        .
      </footer>
    </div>
  );
}

function StationList({
  results,
  selected,
  onSelect,
}: {
  results: Station[];
  selected: Station | null;
  onSelect: (s: Station) => void;
}) {
  if (results.length === 0) return <p className="muted">No stations match.</p>;
  return (
    <div className="results" role="listbox" aria-label="Matching stations">
      <div className="row head">
        <span>Station</span>
        <span>Climate ID</span>
        <span>Hourly</span>
        <span>Daily</span>
        <span>Monthly</span>
      </div>
      {results.map((s) => (
        <button
          key={s.climateId}
          role="option"
          aria-selected={selected?.climateId === s.climateId}
          className={`row ${selected?.climateId === s.climateId ? "selected" : ""}`}
          onClick={() => onSelect(s)}
        >
          <span>
            <strong>{s.name}</strong>
            <span className="muted small"> {titleCase(s.province)}</span>
          </span>
          <span className="mono">{s.climateId}</span>
          <span>{fmtRange(s.hourly)}</span>
          <span>{fmtRange(s.daily)}</span>
          <span>{fmtRange(s.monthly)}</span>
        </button>
      ))}
      {results.length === 50 && <p className="muted small">Showing the first 50 matches; refine your search.</p>}
    </div>
  );
}

function DownloadForm({ station, inventoryModified }: { station: Station; inventoryModified: string | null }) {
  const avails = useMemo(() => {
    const out = {} as Record<Timeframe, Availability | null>;
    for (const tf of TIMEFRAMES) out[tf] = availability(station, tf, inventoryModified);
    return out;
  }, [station, inventoryModified]);

  const firstAvailable: Timeframe | undefined = avails.daily ? "daily" : TIMEFRAMES.find((tf) => avails[tf]);
  const [timeframe, setTimeframe] = useState<Timeframe | undefined>(firstAvailable);
  const avail = timeframe ? avails[timeframe] : null;
  const [range, setRange] = useState<[YearMonth, YearMonth] | null>(
    timeframe && avail ? defaultRange(timeframe, avail) : null,
  );
  const [timezone, setTimezone] = useState<"lst" | "utc">("lst");
  const [dropEmpty, setDropEmpty] = useState(true);
  const [progress, setProgress] = useState<Progress | null>(null);
  const [result, setResult] = useState<DownloadResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => () => abortRef.current?.abort(), []);

  function chooseTimeframe(tf: Timeframe) {
    const a = avails[tf];
    if (!a) return;
    setTimeframe(tf);
    setRange(defaultRange(tf, a));
    setResult(null);
    setError(null);
  }

  function update(which: 0 | 1, v: YearMonth) {
    if (!range || !avail) return;
    const next: [YearMonth, YearMonth] = [...range];
    next[which] = clampYm(v, avail);
    // Keep start <= end by moving the other bound.
    if (ymIndex(next[0]) > ymIndex(next[1])) next[which === 0 ? 1 : 0] = next[which];
    setRange(next);
    setResult(null);
    setError(null);
  }

  if (!timeframe || !avail || !range) {
    return (
      <section className="card">
        <h2>2. Choose data</h2>
        <p className="error">{station.name} has no downloadable data in the inventory.</p>
      </section>
    );
  }

  const [start, end] = range;
  const invalid = validateRange(avail, timeframe, start, end);
  const requests = chunkCount({ timeframe, start, end });
  const busy = progress !== null && result === null && error === null;

  async function startDownload() {
    if (invalid || !timeframe) return;
    const controller = new AbortController();
    abortRef.current = controller;
    setResult(null);
    setError(null);
    setProgress({ done: 0, total: requests });
    try {
      const res = await downloadCsv(
        {
          climateId: station.climateId,
          stationName: station.name,
          timeframe,
          start,
          end,
          timezone,
          dropEmpty,
        },
        { signal: controller.signal, onProgress: setProgress },
      );
      setResult(res);
      saveBlob(res.blob, res.filename);
    } catch (e) {
      setError(controller.signal.aborted ? "Download cancelled." : (e as Error).message);
    }
  }

  return (
    <section className="card">
      <h2>2. Choose data</h2>
      <div className="station-summary">
        <div>
          <strong>{station.name}</strong> · {titleCase(station.province)}
        </div>
        <div className="muted small">
          Climate ID <span className="mono">{station.climateId}</span>
          {station.lat !== null && station.lon !== null && (
            <> · {station.lat.toFixed(2)}, {station.lon.toFixed(2)}</>
          )}
          {station.elevation !== null && <> · {station.elevation} m</>}
        </div>
      </div>

      <fieldset disabled={busy}>
        <legend>Granularity</legend>
        <div className="segmented">
          {TIMEFRAMES.map((tf) => (
            <label key={tf} className={!avails[tf] ? "disabled" : ""}>
              <input
                type="radio"
                name="timeframe"
                value={tf}
                checked={timeframe === tf}
                disabled={!avails[tf]}
                onChange={() => chooseTimeframe(tf)}
              />
              <span>
                {LABEL[tf]}
                <small>
                  {avails[tf]
                    ? `${avails[tf]!.start.year}–${avails[tf]!.end.year}`
                    : "not available"}
                </small>
              </span>
            </label>
          ))}
        </div>
      </fieldset>

      <fieldset disabled={busy}>
        <legend>Range</legend>
        <div className="range">
          <RangePicker label="From" value={start} avail={avail} monthly={timeframe === "hourly"} onChange={(v) => update(0, v)} />
          <RangePicker label="To" value={end} avail={avail} monthly={timeframe === "hourly"} onChange={(v) => update(1, v)} isEnd />
        </div>
        <p className="muted small">
          Available:{" "}
          {timeframe === "hourly"
            ? `${fmtYm(avail.start)} to ${fmtYm(avail.end)}`
            : `${avail.start.year} to ${avail.end.year}`}
          {avail.extended && " (the inventory lists this station as active, so the latest data is included)"}
        </p>
      </fieldset>

      <fieldset disabled={busy}>
        <legend>Options</legend>
        {timeframe === "hourly" && (
          <label className="inline">
            Time zone{" "}
            <select value={timezone} onChange={(e) => setTimezone(e.target.value as "lst" | "utc")}>
              <option value="lst">Local standard time (LST)</option>
              <option value="utc">UTC</option>
            </select>
          </label>
        )}
        <label className="inline">
          <input type="checkbox" checked={dropEmpty} onChange={(e) => setDropEmpty(e.target.checked)} /> Drop rows
          with no measurements (future dates and gaps)
        </label>
      </fieldset>

      {invalid && <p className="error">{invalid}</p>}

      <div className="actions">
        {!busy ? (
          <button className="primary" disabled={!!invalid} onClick={startDownload}>
            Prepare CSV
          </button>
        ) : (
          <button onClick={() => abortRef.current?.abort()}>Cancel</button>
        )}
        <span className="muted small">
          {requests} request{requests === 1 ? "" : "s"} to ECCC
          {requests > 60 && " — this may take a few minutes"}
        </span>
      </div>

      {progress && busy && (
        <div className="progress" aria-live="polite">
          <progress value={progress.done} max={progress.total} />
          <span className="small">
            {progress.done} / {progress.total}
          </span>
        </div>
      )}
      {error && <p className="error">{error}</p>}
      {result && (
        <p className="success">
          Ready: {result.rows.toLocaleString()} rows ({(result.blob.size / 1024 / 1024).toFixed(1)} MB).{" "}
          <button className="link" onClick={() => saveBlob(result.blob, result.filename)}>
            Save {result.filename}
          </button>
        </p>
      )}
    </section>
  );
}

function RangePicker({
  label,
  value,
  avail,
  monthly,
  isEnd = false,
  onChange,
}: {
  label: string;
  value: YearMonth;
  avail: Availability;
  monthly: boolean;
  isEnd?: boolean;
  onChange: (v: YearMonth) => void;
}) {
  const years: number[] = [];
  for (let y = avail.start.year; y <= avail.end.year; y++) years.push(y);
  const minMonth = value.year === avail.start.year ? avail.start.month : 1;
  const maxMonth = value.year === avail.end.year ? avail.end.month : 12;
  return (
    <div className="picker">
      <span className="picker-label">{label}</span>
      {monthly && (
        <select
          aria-label={`${label} month`}
          value={value.month}
          onChange={(e) => onChange(ym(value.year, Number(e.target.value)))}
        >
          {MONTHS.map((m, i) => (
            <option key={m} value={i + 1} disabled={i + 1 < minMonth || i + 1 > maxMonth}>
              {m}
            </option>
          ))}
        </select>
      )}
      <select
        aria-label={`${label} year`}
        value={value.year}
        onChange={(e) => onChange(ym(Number(e.target.value), monthly ? value.month : isEnd ? 12 : 1))}
      >
        {years.map((y) => (
          <option key={y} value={y}>
            {y}
          </option>
        ))}
      </select>
    </div>
  );
}
