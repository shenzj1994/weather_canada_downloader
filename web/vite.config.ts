/// <reference types="vitest/config" />
import { readFile } from "node:fs/promises";
import { getRequestListener } from "@hono/node-server";
import react from "@vitejs/plugin-react";
import { defineConfig, type Plugin } from "vite";
import { createApp } from "./src/server/app.ts";

/** Serve the Hono API from the Vite dev server so `npm run dev` is all you need. */
function apiDevServer(): Plugin {
  return {
    name: "api-dev-server",
    configureServer(server) {
      const app = createApp({
        loadFallbackInventory: () =>
          readFile("../src/canada_weather/data/Station Inventory EN.csv", "utf-8").catch(() => null),
      });
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
