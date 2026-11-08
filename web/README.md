# Canada Weather Downloader — web app

A small web app on top of ECCC's bulk-data endpoint:

1. **Choose stations**, in one of two ways:
   - **Search for one station.** Type a name or Climate ID and pick a match from the dropdown. The station inventory is loaded live from ECCC and searched in the browser.
   - **Enter a list of Climate IDs**, separated by commas, spaces or new lines. IDs that are not in the inventory are named in a warning and skipped.
2. **Choose data.** Pick hourly or daily data and a range.
   - *One station:* only granularities the station has are enabled, and the range pickers only offer what the inventory lists. The server checks every request against the inventory again.
   - *A list:* one granularity and range apply to all stations, and each station is downloaded for whatever data it has inside the range. Stations with no data in the range are listed. If one station fails to download, it is left out of the file and reported, and the others are kept. The limit is 50 stations at a time.
3. **Download one CSV.** The browser fetches each period through the server (ECCC sends no CORS headers), four at a time, with retries and a progress bar. It then merges them into a single CSV with one header row and saves it. With several stations, the rows are grouped by station, in the order you entered them; the `Climate ID` and `Station Name` columns tell them apart. Nothing is written to disk on the server.

## Stack

| Part | Choice | Why |
|---|---|---|
| API | [Hono](https://hono.dev) (TypeScript) | Uses only web-standard `fetch`/`Request`/`Response`, so the same `createApp()` runs on Node, Cloudflare Workers and Vercel |
| UI | React + Vite | Static build, deployable to any CDN |
| Container | `node:22-alpine`, server bundled with esbuild | No `node_modules` at runtime; image is ~60 MB |

```
web/
  src/shared/   CSV parsing, inventory parsing, range validation, row filtering (used by client and server)
  src/server/   app.ts — runtime-agnostic Hono API; node.ts — Node entry that also serves the UI
  src/client/   React UI and the chunked downloader
  tests/        vitest (ECCC is mocked)
```

## API

| Endpoint | Description |
|---|---|
| `GET /api/inventory` | `{modified, source, stations[]}`, where `source` is `"live"` or `"snapshot"`. It is cached in memory for 12 h. If ECCC is down, the last good copy or the bundled snapshot is served. |
| `GET /api/chunk?climateId=&timeframe=&year=&month=&tz=&dropEmpty=` | One period as CSV: a month for `hourly` or a year for `daily`. Returns 400 outside the station's record and 204 when ECCC has no data. |
| `GET /api/health` | Liveness check |

As in the Python package, a station whose inventory record runs to the snapshot year, or the year before it, is treated as active. Its range is extended to the current month, because the inventory lags behind the data.

## Run

```bash
# Docker (from the repository root)
docker build -t canada-weather-web .
docker run --rm -p 8080:8080 canada-weather-web    # http://localhost:8080

# Development (API and UI with hot reload, from web/)
npm install
npm run dev
npm test && npm run typecheck
```

Environment variables (Node server): `PORT` (8080), `STATIC_ROOT` (`./dist/client`) and `INVENTORY_FALLBACK_PATH`, the inventory snapshot used when ECCC is unreachable. The Docker image includes the snapshot from `src/canada_weather/data/`.

## Hosting on Cloudflare or Vercel later

The UI is a static `dist/client`. The API is `createApp()` from `src/server/app.ts`. Cloudflare and Vercel each need only a few lines of entry code:

```ts
// Cloudflare Worker (static assets via Workers Assets)
import { createApp } from "./src/server/app.ts";
export default createApp();

// Vercel function, e.g. api/[[...route]].ts
import { handle } from "hono/vercel";
import { createApp } from "../src/server/app.ts";
export const GET = handle(createApp());
```

Each `/api/chunk` call is one short upstream request, so serverless time limits are not a concern. Responses carry `Cache-Control`, so a CDN can cache completed periods.
