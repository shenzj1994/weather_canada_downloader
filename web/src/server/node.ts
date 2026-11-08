/** Node entry point (Docker): serves the API and the built single-page app. */

import { serve } from "@hono/node-server";
import { serveStatic } from "@hono/node-server/serve-static";
import { compress } from "hono/compress";
import { Hono } from "hono";
import { createApp } from "./app.ts";
import { stations } from "./inventory.ts";

const port = Number(process.env.PORT ?? 8080);
const staticRoot = process.env.STATIC_ROOT ?? "./dist/client";

const server = new Hono();
server.use("*", compress());
server.route("/", createApp({ stations }));
server.use("/*", serveStatic({ root: staticRoot }));
server.get("*", serveStatic({ path: `${staticRoot}/index.html` }));

serve({ fetch: server.fetch, port }, (info) => {
  console.log(`canada-weather-web listening on http://localhost:${info.port}`);
});
