import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import {
  nearbyStations,
  parseClimateIds,
  resolveClimateIds,
  searchStations,
  TIMEFRAMES,
  type Station,
  type Timeframe,
} from "../shared/inventory.ts";
import {
  availability,
  currentYm,
  fmtYm,
  periods,
  stationPeriods,
  validateRange,
  ym,
  ymIndex,
  type Availability,
  type YearMonth,
} from "../shared/range.ts";
import {
  downloadMerged,
  filenameFor,
  saveBlob,
  type DownloadOptions,
  type DownloadResult,
  type Progress,
  type StationJob,
} from "./download.ts";

interface InventoryResponse {
  modified: string | null;
  source: "live" | "snapshot";
  stations: Station[];
}

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const LABEL: Record<Timeframe, string> = { hourly: "Hourly", daily: "Daily" };

const fmtRange = (r: [number, number] | null) => (r ? `${r[0]}–${r[1]}` : "—");
const titleCase = (s: string) => s.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());

function defaultRange(tf: Timeframe, a: Availability): [YearMonth, YearMonth] {
  if (tf === "hourly") {
    const startIdx = Math.max(ymIndex(a.start), ymIndex(a.end) - 11);
    return [ym(Math.floor(startIdx / 12), (startIdx % 12) + 1), a.end];
  }
  return [ym(Math.max(a.start.year, a.end.year - 4), 1), ym(a.end.year, 12)];
}

const clampYm = (v: YearMonth, a: Availability): YearMonth =>
  ymIndex(v) < ymIndex(a.start) ? a.start : ymIndex(v) > ymIndex(a.end) ? a.end : v;

const MAX_STATIONS = 50;
type Mode = "single" | "multi";

/** Download state shared by both forms. */
function useDownloader() {
  const [progress, setProgress] = useState<Progress | null>(null);
  const [result, setResult] = useState<DownloadResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  useEffect(() => () => abortRef.current?.abort(), []);

  const busy = progress !== null && result === null && error === null;
  const reset = () => {
    setResult(null);
    setError(null);
    setProgress(null);
  };
  async function run(jobs: StationJob[], opts: DownloadOptions, filename: string) {
    const controller = new AbortController();
    abortRef.current = controller;
    setResult(null);
    setError(null);
    setProgress({ done: 0, total: jobs.reduce((n, j) => n + j.periods.length, 0) });
    try {
      const res = await downloadMerged(jobs, opts, filename, {
        signal: controller.signal,
        onProgress: setProgress,
      });
      setResult(res);
      saveBlob(res.blob, res.filename);
    } catch (e) {
      setError(controller.signal.aborted ? "Download cancelled." : (e as Error).message);
    }
  }
  return { progress, result, error, busy, reset, run, cancel: () => abortRef.current?.abort() };
}

type Downloader = ReturnType<typeof useDownloader>;

export function App() {
  const [inventory, setInventory] = useState<InventoryResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [station, setStation] = useState<Station | null>(null);
  const [mode, setMode] = useState<Mode>("single");
  const [idsText, setIdsText] = useState("");

  const addIds = (ids: string[]) =>
    setIdsText((text) => {
      const have = new Set(parseClimateIds(text));
      const extra = ids.filter((id) => !have.has(id));
      if (!extra.length) return text;
      return (text.trim() ? `${text.trim()}\n` : "") + extra.join("\n");
    });

  function selectStation(s: Station) {
    setStation(s);
    if (mode === "multi") addIds([s.climateId]);
  }

  function chooseMode(m: Mode) {
    setMode(m);
    if (m === "multi" && station && !idsText.trim()) addIds([station.climateId]);
  }

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
              <StationList results={results} selected={station} onSelect={selectStation} />
            )}
          </>
        )}
      </section>

      {inventory && (
        <div className="tabs" role="tablist" aria-label="Download mode">
          {(["single", "multi"] as const).map((m) => (
            <button key={m} role="tab" aria-selected={mode === m} className={mode === m ? "active" : ""} onClick={() => chooseMode(m)}>
              {m === "single" ? "One station" : "Several stations"}
            </button>
          ))}
        </div>
      )}

      {inventory && mode === "single" && station && (
        <DownloadForm key={station.climateId} station={station} inventoryModified={inventory.modified} />
      )}
      {inventory && mode === "single" && !station && (
        <p className="muted hint">Search for a station above and select it.</p>
      )}
      {inventory && mode === "multi" && (
        <MultiStationForm
          inventory={inventory}
          anchor={station}
          idsText={idsText}
          setIdsText={setIdsText}
          addIds={addIds}
        />
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
  const dl = useDownloader();

  function chooseTimeframe(tf: Timeframe) {
    const a = avails[tf];
    if (!a) return;
    setTimeframe(tf);
    setRange(defaultRange(tf, a));
    dl.reset();
  }

  function update(which: 0 | 1, v: YearMonth) {
    if (!range || !avail) return;
    const next: [YearMonth, YearMonth] = [...range];
    next[which] = clampYm(v, avail);
    // Keep start <= end by moving the other bound.
    if (ymIndex(next[0]) > ymIndex(next[1])) next[which === 0 ? 1 : 0] = next[which];
    setRange(next);
    dl.reset();
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
  const todo = periods(timeframe, start, end);
  const { busy } = dl;

  function startDownload() {
    if (invalid || !timeframe) return;
    const opts = { timeframe, timezone, dropEmpty, failFast: true };
    dl.run([{ climateId: station.climateId, periods: todo }], opts, filenameFor([station], opts, start, end));
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
          <RangePicker label="From" value={start} avail={avail} showMonth={timeframe === "hourly"} onChange={(v) => update(0, v)} />
          <RangePicker label="To" value={end} avail={avail} showMonth={timeframe === "hourly"} onChange={(v) => update(1, v)} isEnd />
        </div>
        <p className="muted small">
          Available:{" "}
          {timeframe === "hourly"
            ? `${fmtYm(avail.start)} to ${fmtYm(avail.end)}`
            : `${avail.start.year} to ${avail.end.year}`}
          {avail.extended && " (the inventory lists this station as active, so the latest data is included)"}
        </p>
      </fieldset>

      <OptionsFieldset
        busy={busy}
        timeframe={timeframe}
        timezone={timezone}
        setTimezone={setTimezone}
        dropEmpty={dropEmpty}
        setDropEmpty={setDropEmpty}
      />

      {invalid && <p className="error">{invalid}</p>}
      <DownloadControls dl={dl} requests={todo.length} disabled={!!invalid} onStart={startDownload} />
    </section>
  );
}

function OptionsFieldset({
  busy,
  timeframe,
  timezone,
  setTimezone,
  dropEmpty,
  setDropEmpty,
}: {
  busy: boolean;
  timeframe: Timeframe;
  timezone: "lst" | "utc";
  setTimezone: (tz: "lst" | "utc") => void;
  dropEmpty: boolean;
  setDropEmpty: (v: boolean) => void;
}) {
  return (
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
  );
}

function DownloadControls({
  dl,
  requests,
  disabled,
  onStart,
  children,
}: {
  dl: Downloader;
  requests: number;
  disabled: boolean;
  onStart: () => void;
  children?: ReactNode;
}) {
  const { busy, progress, error, result } = dl;
  return (
    <>
      <div className="actions">
        {!busy ? (
          <button className="primary" disabled={disabled} onClick={onStart}>
            Prepare CSV
          </button>
        ) : (
          <button onClick={dl.cancel}>Cancel</button>
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
        <div className="success">
          <p>
            Ready: {result.rows.toLocaleString()} rows ({(result.blob.size / 1024 / 1024).toFixed(1)} MB).{" "}
            <button className="link" onClick={() => saveBlob(result.blob, result.filename)}>
              Save {result.filename}
            </button>
          </p>
          {children}
        </div>
      )}
    </>
  );
}

function MultiStationForm({
  inventory,
  anchor,
  idsText,
  setIdsText,
  addIds,
}: {
  inventory: InventoryResponse;
  anchor: Station | null;
  idsText: string;
  setIdsText: (t: string) => void;
  addIds: (ids: string[]) => void;
}) {
  const [timeframe, setTimeframe] = useState<Timeframe>("daily");
  const [range, setRange] = useState<[YearMonth, YearMonth]>(() => multiDefaultRange("daily"));
  const [timezone, setTimezone] = useState<"lst" | "utc">("lst");
  const [dropEmpty, setDropEmpty] = useState(true);
  const [radiusKm, setRadiusKm] = useState(25);
  const dl = useDownloader();
  const { busy } = dl;

  const ids = useMemo(() => parseClimateIds(idsText), [idsText]);
  const { found, missing } = useMemo(() => resolveClimateIds(ids, inventory.stations), [ids, inventory]);

  // The pickers span the earliest record among the entered stations to today;
  // each station is then downloaded for whatever it has inside the range.
  const bounds: Availability = useMemo(() => {
    const firsts = found.map((s) => s[timeframe]?.[0]).filter((y): y is number => y !== undefined);
    return { start: ym(firsts.length ? Math.min(...firsts) : 1840, 1), end: currentYm(), extended: false };
  }, [found, timeframe]);
  const start = clampYm(range[0], bounds);
  const end = clampYm(range[1], bounds);

  const jobs = found.map((s) => ({
    station: s,
    periods: stationPeriods(availability(s, timeframe, inventory.modified), timeframe, start, end),
  }));
  const withData = jobs.filter((j) => j.periods.length > 0);
  const noData = jobs.filter((j) => j.periods.length === 0);
  const requests = withData.reduce((n, j) => n + j.periods.length, 0);
  const tooMany = found.length > MAX_STATIONS;

  const center = anchor ?? found[0] ?? null;
  const nearby = useMemo(() => {
    if (!center) return [];
    const have = new Set(ids);
    return nearbyStations(inventory.stations, center, radiusKm)
      .filter(({ station: s }) => s[timeframe] && !have.has(s.climateId))
      .slice(0, 15);
  }, [center, inventory, radiusKm, timeframe, ids]);

  function chooseTimeframe(tf: Timeframe) {
    setTimeframe(tf);
    setRange(multiDefaultRange(tf));
    dl.reset();
  }

  function update(which: 0 | 1, v: YearMonth) {
    const next: [YearMonth, YearMonth] = [start, end];
    next[which] = clampYm(v, bounds);
    if (ymIndex(next[0]) > ymIndex(next[1])) next[which === 0 ? 1 : 0] = next[which];
    setRange(next);
    dl.reset();
  }

  function startDownload() {
    const opts = { timeframe, timezone, dropEmpty, failFast: false };
    dl.run(
      withData.map((j) => ({ climateId: j.station.climateId, periods: j.periods })),
      opts,
      filenameFor(withData.map((j) => j.station), opts, start, end),
    );
  }

  const nameOf = (id: string) => found.find((s) => s.climateId === id)?.name ?? id;

  return (
    <section className="card">
      <h2>2. Choose stations and data</h2>

      <fieldset disabled={busy}>
        <legend>Climate IDs</legend>
        <textarea
          className="ids"
          rows={4}
          value={idsText}
          onChange={(e) => {
            setIdsText(e.target.value);
            dl.reset();
          }}
          placeholder={"One per line, or separated by commas or spaces, e.g.\n1108395, 1108447, 1106PF7"}
          aria-label="Climate IDs"
        />
        <p className="muted small">
          {found.length} station{found.length === 1 ? "" : "s"} recognised. Clicking a search result above also adds
          it here.
        </p>
        {missing.length > 0 && (
          <p className="warning" role="alert">
            ⚠ {missing.length === 1 ? "This Climate ID does" : "These Climate IDs do"} not exist in the station
            inventory and will be skipped: <span className="mono">{missing.join(", ")}</span>
          </p>
        )}
        {found.length > 0 && (
          <ul className="chips">
            {found.map((s) => (
              <li key={s.climateId}>
                <span className="mono">{s.climateId}</span> {s.name}
              </li>
            ))}
          </ul>
        )}
      </fieldset>

      {center && (
        <details className="nearby" open>
          <summary>
            Stations near {center.name}{" "}
            <select
              value={radiusKm}
              onChange={(e) => setRadiusKm(Number(e.target.value))}
              onClick={(e) => e.stopPropagation()}
              aria-label="Search radius"
            >
              {[10, 25, 50, 100].map((r) => (
                <option key={r} value={r}>
                  within {r} km
                </option>
              ))}
            </select>
          </summary>
          {nearby.length === 0 ? (
            <p className="muted small">No other stations with {timeframe} data in this radius.</p>
          ) : (
            <>
              <ul className="nearby-list">
                {nearby.map(({ station: s, km }) => (
                  <li key={s.climateId}>
                    <button className="link" disabled={busy} onClick={() => addIds([s.climateId])}>
                      Add
                    </button>
                    <span className="mono">{s.climateId}</span>
                    <span>{s.name}</span>
                    <span className="muted small">
                      {km.toFixed(1)} km · {LABEL[timeframe].toLowerCase()} {fmtRange(s[timeframe])}
                    </span>
                  </li>
                ))}
              </ul>
              <button disabled={busy} onClick={() => addIds(nearby.map(({ station: s }) => s.climateId))}>
                Add all {nearby.length}
              </button>
            </>
          )}
        </details>
      )}

      <fieldset disabled={busy}>
        <legend>Granularity</legend>
        <div className="segmented">
          {TIMEFRAMES.map((tf) => (
            <label key={tf}>
              <input type="radio" name="multi-timeframe" value={tf} checked={timeframe === tf} onChange={() => chooseTimeframe(tf)} />
              <span>{LABEL[tf]}</span>
            </label>
          ))}
        </div>
      </fieldset>

      <fieldset disabled={busy}>
        <legend>Range</legend>
        <div className="range">
          <RangePicker label="From" value={start} avail={bounds} showMonth={timeframe === "hourly"} onChange={(v) => update(0, v)} />
          <RangePicker label="To" value={end} avail={bounds} showMonth={timeframe === "hourly"} onChange={(v) => update(1, v)} isEnd />
        </div>
        <p className="muted small">Each station is downloaded for whatever data it has inside this range.</p>
        {noData.length > 0 && (
          <p className="warning">
            No {timeframe} data in this range, so these will be left out:{" "}
            {noData.map((j) => `${j.station.climateId} ${j.station.name} (${fmtRange(j.station[timeframe])})`).join("; ")}
          </p>
        )}
      </fieldset>

      <OptionsFieldset
        busy={busy}
        timeframe={timeframe}
        timezone={timezone}
        setTimezone={setTimezone}
        dropEmpty={dropEmpty}
        setDropEmpty={setDropEmpty}
      />

      {tooMany && <p className="error">Enter at most {MAX_STATIONS} stations at a time.</p>}
      <DownloadControls dl={dl} requests={requests} disabled={tooMany || withData.length === 0} onStart={startDownload}>
        {dl.result && (
          <ul className="small station-rows">
            {[...dl.result.rowsByStation].map(([id, n]) => (
              <li key={id}>
                <span className="mono">{id}</span> {nameOf(id)}: {n ? `${n.toLocaleString()} rows` : "no data returned by ECCC for this range"}
              </li>
            ))}
            {dl.result.failed.map((f) => (
              <li key={f.climateId} className="error">
                <span className="mono">{f.climateId}</span> {nameOf(f.climateId)}: left out, download failed ({f.error})
              </li>
            ))}
            {missing.length > 0 && (
              <li className="warning">
                Skipped, not in the station inventory: <span className="mono">{missing.join(", ")}</span>
              </li>
            )}
          </ul>
        )}
      </DownloadControls>
    </section>
  );
}

function multiDefaultRange(tf: Timeframe): [YearMonth, YearMonth] {
  const today = currentYm();
  if (tf === "hourly") {
    const i = ymIndex(today) - 11;
    return [ym(Math.floor(i / 12), (i % 12) + 1), today];
  }
  return [ym(today.year - 4, 1), ym(today.year, 12)];
}

function RangePicker({
  label,
  value,
  avail,
  showMonth,
  isEnd = false,
  onChange,
}: {
  label: string;
  value: YearMonth;
  avail: Availability;
  showMonth: boolean;
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
      {showMonth && (
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
        onChange={(e) => onChange(ym(Number(e.target.value), showMonth ? value.month : isEnd ? 12 : 1))}
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
