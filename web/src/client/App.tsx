import { useEffect, useId, useMemo, useRef, useState, type KeyboardEvent, type ReactNode } from "react";
import {
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

type Mode = "search" | "list";

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const LABEL: Record<Timeframe, string> = { hourly: "Hourly", daily: "Daily" };
const MAX_STATIONS = 50;
const MAX_RESULTS = 50;

const fmtRange = (r: [number, number] | null) => (r ? `${r[0]}–${r[1]}` : "—");
const titleCase = (s: string) => s.toLowerCase().replace(/\b\w/g, (c) => c.toUpperCase());
const stationLabel = (s: Station) => `${s.name} (${s.climateId})`;

const clampYm = (v: YearMonth, a: Availability): YearMonth =>
  ymIndex(v) < ymIndex(a.start) ? a.start : ymIndex(v) > ymIndex(a.end) ? a.end : v;

/** Last 12 months (hourly) or last 5 years (daily) of `a`. */
function defaultRange(tf: Timeframe, a: Availability): [YearMonth, YearMonth] {
  if (tf === "hourly") {
    const i = Math.max(ymIndex(a.start), ymIndex(a.end) - 11);
    return [ym(Math.floor(i / 12), (i % 12) + 1), a.end];
  }
  return [ym(Math.max(a.start.year, a.end.year - 4), 1), ym(a.end.year, 12)];
}

export function App() {
  const [inventory, setInventory] = useState<InventoryResponse | null>(null);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [mode, setMode] = useState<Mode>("search");
  const [station, setStation] = useState<Station | null>(null);
  const [idsText, setIdsText] = useState("");

  useEffect(() => {
    fetch("/api/inventory")
      .then(async (r) => {
        if (!r.ok) throw new Error(((await r.json().catch(() => ({}))) as { error?: string }).error ?? r.statusText);
        return r.json() as Promise<InventoryResponse>;
      })
      .then(setInventory)
      .catch((e: Error) => setLoadError(e.message));
  }, []);

  const ids = useMemo(() => parseClimateIds(idsText), [idsText]);
  const resolved = useMemo(
    () => (inventory ? resolveClimateIds(ids, inventory.stations) : { found: [], missing: [] }),
    [ids, inventory],
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
        <h2>1. Choose stations</h2>
        {loadError && <p className="error">Could not load the station inventory: {loadError}</p>}
        {!inventory && !loadError && <p className="muted">Loading station inventory…</p>}
        {inventory && (
          <>
            <div className="segmented mode" role="radiogroup" aria-label="How to choose stations">
              <label>
                <input type="radio" name="mode" checked={mode === "search"} onChange={() => setMode("search")} />
                <span>Search for one station</span>
              </label>
              <label>
                <input type="radio" name="mode" checked={mode === "list"} onChange={() => setMode("list")} />
                <span>Enter a list of Climate IDs</span>
              </label>
            </div>

            {mode === "search" ? (
              <StationSearch stations={inventory.stations} selected={station} onSelect={setStation} />
            ) : (
              <ClimateIdList
                value={idsText}
                onChange={setIdsText}
                found={resolved.found}
                missing={resolved.missing}
              />
            )}

            <p className="muted small">
              {inventory.stations.length.toLocaleString()} stations · inventory updated{" "}
              {inventory.modified?.slice(0, 10) ?? "unknown"}
              {inventory.source === "snapshot" && " (bundled snapshot; ECCC unreachable)"}
            </p>
          </>
        )}
      </section>

      {inventory && mode === "search" && station && (
        <DownloadForm key={station.climateId} station={station} inventoryModified={inventory.modified} />
      )}
      {inventory && mode === "list" && resolved.found.length > 0 && (
        <MultiStationForm
          stations={resolved.found}
          missing={resolved.missing}
          inventoryModified={inventory.modified}
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

/** Search box with a dropdown of matches (combobox). */
function StationSearch({
  stations,
  selected,
  onSelect,
}: {
  stations: Station[];
  selected: Station | null;
  onSelect: (s: Station) => void;
}) {
  const [query, setQuery] = useState(selected ? stationLabel(selected) : "");
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(0);
  const listId = useId();
  const listRef = useRef<HTMLUListElement>(null);

  const results = useMemo(() => searchStations(stations, query, MAX_RESULTS), [stations, query]);
  const showList = open && query.trim() !== "";

  useEffect(() => {
    listRef.current?.querySelector(`[data-index="${active}"]`)?.scrollIntoView({ block: "nearest" });
  }, [active]);

  function choose(s: Station) {
    onSelect(s);
    setQuery(stationLabel(s));
    setOpen(false);
  }

  function onKeyDown(e: KeyboardEvent<HTMLInputElement>) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setOpen(true);
      setActive((i) => Math.min(i + 1, results.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setActive((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && showList && results[active]) {
      e.preventDefault();
      choose(results[active]);
    } else if (e.key === "Escape") {
      setOpen(false);
    }
  }

  return (
    <div className="combobox">
      <input
        className="search"
        type="search"
        autoFocus
        role="combobox"
        aria-expanded={showList}
        aria-controls={listId}
        aria-autocomplete="list"
        aria-activedescendant={showList && results[active] ? `${listId}-${active}` : undefined}
        aria-label="Search stations"
        placeholder="Station name or Climate ID, e.g. “Vancouver Intl” or 1108395"
        value={query}
        onChange={(e) => {
          setQuery(e.target.value);
          setActive(0);
          setOpen(true);
        }}
        onFocus={(e) => {
          e.target.select();
          setOpen(true);
        }}
        onBlur={() => setOpen(false)}
        onKeyDown={onKeyDown}
      />
      {showList && (
        <ul className="dropdown" id={listId} role="listbox" ref={listRef} aria-label="Matching stations">
          {results.length === 0 && <li className="muted empty">No stations match.</li>}
          {results.map((s, i) => (
            <li
              key={s.climateId}
              id={`${listId}-${i}`}
              data-index={i}
              role="option"
              aria-selected={i === active}
              className={i === active ? "active" : ""}
              // mousedown, not click, so it fires before the input's blur closes the list
              onMouseDown={(e) => {
                e.preventDefault();
                choose(s);
              }}
              onMouseEnter={() => setActive(i)}
            >
              <span>
                <strong>{s.name}</strong> <span className="muted small">{titleCase(s.province)}</span>
              </span>
              <span className="mono">{s.climateId}</span>
              <span className="muted small">
                hourly {fmtRange(s.hourly)} · daily {fmtRange(s.daily)}
              </span>
            </li>
          ))}
          {results.length === MAX_RESULTS && (
            <li className="muted small empty">Showing the first {MAX_RESULTS} matches; keep typing to narrow.</li>
          )}
        </ul>
      )}
    </div>
  );
}

function ClimateIdList({
  value,
  onChange,
  found,
  missing,
}: {
  value: string;
  onChange: (v: string) => void;
  found: Station[];
  missing: string[];
}) {
  return (
    <>
      <textarea
        className="ids"
        rows={5}
        autoFocus
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={"One per line, or separated by commas or spaces, e.g.\n1108395, 1108447, 1108380"}
        aria-label="Climate IDs"
      />
      {missing.length > 0 && (
        <p className="warning" role="alert">
          ⚠ {missing.length === 1 ? "This Climate ID does" : "These Climate IDs do"} not exist in the station
          inventory and will be skipped: <span className="mono">{missing.join(", ")}</span>
        </p>
      )}
      {found.length > 0 && (
        <>
          <p className="muted small">
            {found.length} station{found.length === 1 ? "" : "s"} recognised:
          </p>
          <ul className="chips">
            {found.map((s) => (
              <li key={s.climateId}>
                <span className="mono">{s.climateId}</span> {s.name}
              </li>
            ))}
          </ul>
        </>
      )}
      {found.length > MAX_STATIONS && <p className="error">Enter at most {MAX_STATIONS} stations at a time.</p>}
    </>
  );
}

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
                  {avails[tf] ? `${avails[tf]!.start.year}–${avails[tf]!.end.year}` : "not available"}
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

function MultiStationForm({
  stations,
  missing,
  inventoryModified,
}: {
  stations: Station[];
  missing: string[];
  inventoryModified: string | null;
}) {
  const [timeframe, setTimeframe] = useState<Timeframe>("daily");
  const [timezone, setTimezone] = useState<"lst" | "utc">("lst");
  const [dropEmpty, setDropEmpty] = useState(true);
  const dl = useDownloader();
  const { busy } = dl;

  // The pickers span the earliest record among the stations to today; each
  // station is then downloaded for whatever it has inside the chosen range.
  const bounds: Availability = useMemo(() => {
    const firsts = stations.map((s) => s[timeframe]?.[0]).filter((y): y is number => y !== undefined);
    return { start: ym(firsts.length ? Math.min(...firsts) : 1840, 1), end: currentYm(), extended: false };
  }, [stations, timeframe]);
  const [range, setRange] = useState<[YearMonth, YearMonth]>(() => defaultRange("daily", bounds));
  const start = clampYm(range[0], bounds);
  const end = clampYm(range[1], bounds);

  const jobs = stations.map((s) => ({
    station: s,
    periods: stationPeriods(availability(s, timeframe, inventoryModified), timeframe, start, end),
  }));
  const withData = jobs.filter((j) => j.periods.length > 0);
  const noData = jobs.filter((j) => j.periods.length === 0);
  const requests = withData.reduce((n, j) => n + j.periods.length, 0);
  const tooMany = stations.length > MAX_STATIONS;

  function chooseTimeframe(tf: Timeframe) {
    setTimeframe(tf);
    setRange(defaultRange(tf, { ...bounds, end: currentYm() }));
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

  const nameOf = (id: string) => stations.find((s) => s.climateId === id)?.name ?? id;

  // A new list invalidates the previous result.
  const listKey = stations.map((s) => s.climateId).join(",");
  useEffect(() => {
    if (!dl.busy) dl.reset();
  }, [listKey]);

  return (
    <section className="card">
      <h2>2. Choose data</h2>
      <p className="muted small station-summary">
        {stations.length} station{stations.length === 1 ? "" : "s"}. Each is downloaded for whatever data it has
        inside the range.
      </p>

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

      <DownloadControls dl={dl} requests={requests} disabled={tooMany || withData.length === 0} onStart={startDownload}>
        {dl.result && (
          <ul className="small station-rows">
            {[...dl.result.rowsByStation].map(([id, n]) => (
              <li key={id}>
                <span className="mono">{id}</span> {nameOf(id)}:{" "}
                {n ? `${n.toLocaleString()} rows` : "no data returned by ECCC for this range"}
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
