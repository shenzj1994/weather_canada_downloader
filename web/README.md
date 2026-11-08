# Canada Weather Downloader — web app

A small web app on top of ECCC's bulk-data endpoint:

1. **Choose stations**, in one of two ways:
   - **Search for one station.** Type a name or Climate ID and pick a match from the dropdown. The station list is searched in the browser.
   - **Enter a list of Climate IDs**, separated by commas, spaces or new lines. IDs that are not in the inventory are named in a warning and skipped.
2. **Choose data.** Pick hourly or daily data and a range.
   - *One station:* only granularities the station has are enabled, and the range pickers only offer what the inventory lists. The server checks every request against the inventory again.
   - *A list:* one granularity and range apply to all stations, and each station is downloaded for whatever data it has inside the range. Stations with no data in the range are listed. If one station fails to download, it is left out of the file and reported, and the others are kept. The limit is 50 stations at a time.
3. **Download one CSV.** The browser fetches each period through the server (ECCC sends no CORS headers), four at a time, with retries and a progress bar. It then merges them into a single CSV with one header row and saves it. With several stations, the rows are grouped by station, in the order you entered them; the `Climate ID` and `Station Name` columns tell them apart. Nothing is written to disk on the server.

## Stack

| Part | Choice | Why |
|---|---|---|
| API | [Hono](https://hono.dev) (TypeScript) | Uses only web-standard `fetch`/`Request`/`Response`, so the same `createApp()` runs on Node (Docker) and Cloudflare Workers |
| UI | React + Vite | Static build, served from a CDN |
| Station list | `stations.json`, generated at build time | No runtime dependency on ECCC's inventory file (see below) |
| Container | `node:22-alpine`, server bundled with esbuild | No `node_modules` at runtime |

```
web/
  scripts/      build-stations.ts — inventory CSV → public/stations.json
  src/shared/   CSV parsing, inventory and stations.json formats, range validation, row filtering
  src/server/   app.ts — the API; node.ts — Docker entry; worker.ts — Cloudflare entry
  src/client/   React UI and the chunked downloader
  tests/        vitest (ECCC is mocked)
```

## Station list

The station list comes from the inventory CSV in this repository (`src/canada_weather/data/Station Inventory EN.csv`). `npm run build` (and `npm run dev`) runs `scripts/build-stations.ts`, which turns it into `public/stations.json`. That file is condensed to the fields the app uses, as arrays: about 710 KB, or 200 KB gzipped. It is git-ignored and always generated.

- **The browser** loads `/stations.json` as a static file and searches it locally.
- **The server** bundles the same file and uses it to resolve each Climate ID to ECCC's Station ID and to validate the requested period. It never downloads the inventory.

To pick up a new inventory, update the CSV and redeploy. The monthly *Update station inventory* workflow opens a pull request with the new CSV whenever ECCC publishes one, so merging that PR and redeploying is all it takes.

As in the Python package, a station whose inventory record runs to the inventory year, or the year before it, is treated as active. Its range is extended to the current month, because the inventory lags behind the data.

## API

| Endpoint | Description |
|---|---|
| `GET /stations.json` | Static station list: `{version, modified, columns, stations: [[climateId, stationId, name, province, lat, lon, elevation, hourlyFirst, hourlyLast, dailyFirst, dailyLast], …]}` |
| `GET /api/chunk?climateId=&timeframe=&year=&month=&tz=&dropEmpty=` | One period as CSV: a month for `hourly` or a year for `daily`. Returns 400 outside the station's record, 404 for an unknown Climate ID, and 204 when ECCC has no data. |
| `GET /api/health` | `{ok, inventory}`, where `inventory` is the inventory date the server was built with |

## Run locally

```bash
# Docker (from the repository root)
docker build -t canada-weather-web .
docker run --rm -p 8080:8080 canada-weather-web    # http://localhost:8080

# Development (API and UI with hot reload, from web/)
npm install
npm run dev
npm test && npm run typecheck
```

The Node server reads `PORT` (default 8080) and `STATIC_ROOT` (default `./dist/client`).

## Hosting on Cloudflare

The app runs on [Cloudflare Workers](https://developers.cloudflare.com/workers/) with [static assets](https://developers.cloudflare.com/workers/static-assets/), configured in `wrangler.jsonc`:

- The UI and `stations.json` are served as static assets, straight from Cloudflare's CDN, without running the Worker.
- Only `/api/*` runs the Worker (`src/server/worker.ts`). Each request looks up one station and makes one call to ECCC.
- On a cold start, the Worker spends about 5–10 ms indexing the bundled station list (measured on Node). The Worker is about 220 KB gzipped. Both should fit the Free plan's limits, but this has not yet been confirmed on a real deployment, so check the Worker's CPU time in its metrics after deploying.

**From your machine** (in `web/`):

```bash
npm install
npx wrangler login            # once
npm run cf:dev                # optional: local preview in Cloudflare's runtime, http://localhost:8787
npm run deploy                # builds and deploys → https://canada-weather-web.<your-subdomain>.workers.dev
```

**Or from GitHub, deploying on every push:** in the Cloudflare dashboard, go to *Workers & Pages → Create → Import a repository* and pick this repository. Then set:

| Setting | Value |
|---|---|
| Root directory | `web` |
| Build command | `npm run build` |
| Deploy command | `npx wrangler deploy` |
| Production branch | `main` (or the branch to deploy) |

The build reads `../src/canada_weather/data/Station Inventory EN.csv`, so Cloudflare needs the whole repository, which is what *Import a repository* gives it. A custom domain can be added under the Worker's *Settings → Domains & Routes*.

Things to check after the first deploy:
- **ECCC access.** Download one station to confirm ECCC accepts requests from Cloudflare.
- **Caching.** `/api/chunk` sends `Cache-Control` headers, but Cloudflare does not cache Worker responses on its own. Each download therefore reaches ECCC; caching completed periods would need the Cache API.
