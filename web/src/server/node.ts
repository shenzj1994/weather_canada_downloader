/** Node entry point: serves the API and the built single-page app. */

import { readFile } from "node:fs/promises";
import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { compress } from "hono/compress";
import { Hono } from "hono";
import { createApp } from "./app.ts";

const port = Number(process.env.PORT ?? 8080);
const staticRoot = process.env.STATIC_ROOT ?? "./dist/client";
const fallbackPath =
  process.env.INVENTORY_FALLBACK_PATH ?? "../src/canada_weather/data/Station Inventory EN.csv";

export async function loadFallbackInventory(path = fallbackPath): Promise<string | null> {
  try {
    return await readFile(path, "utf-8");
  } catch {
    return null;
  }
}

const server = new Hono();
server.use("*", compress());
server.route("/", createApp({ loadFallbackInventory }));
server.use("/*", serveStatic({ root: staticRoot }));
server.get("*", serveStatic({ path: `${staticRoot}/index.html` }));

serve({ fetch: server.fetch, port }, (info) => {
  console.log(`canada-weather-web listening on http://localhost:${info.port}`);
});
