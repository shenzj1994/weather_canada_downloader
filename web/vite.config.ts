/// <reference types="vitest/config" />
import { readFileSync } from "node:fs";
import { getRequestListener } from "@hono/node-server";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";
import { createApp } from "./src/server/app.ts";
import { parseInventory } from "./src/shared/inventory.ts";
import { inventoryLookup } from "./src/shared/stations.ts";

/** Serve the Hono API from the Vite dev server so `npm run dev` is all you need. */
function apiDevServer(): Plugin {
  return {
    name: "api-dev-server",
    configureServer(server) {
      // Same inventory the build turns into public/stations.json (see `predev`).
      const csv = readFileSync("../src/canada_weather/data/Station Inventory EN.csv", "utf-8");
      const app = createApp({ stations: inventoryLookup(parseInventory(csv)) });
      const listener = getRequestListener(app.fetch);
      server.middlewares.use((req, res, next) => {
        if (req.url?.startsWith("/api/")) listener(req, res);
        else next();
      });
    },
  };
}

export default defineConfig({
  plugins: [react(), apiDevServer()],
  build: { outDir: "dist/client", emptyOutDir: true },
  test: { include: ["tests/**/*.test.ts"] },
});
