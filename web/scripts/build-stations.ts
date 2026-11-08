/**
 * Build public/stations.json from the station inventory bundled with the
 * Python package. Runs before `dev` and `build`.
 *
 *   npm run stations [-- path/to/Station\ Inventory\ EN.csv]
 */

import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseInventory } from "../src/shared/inventory.ts";
import { encodeStations } from "../src/shared/stations.ts";

const here = dirname(fileURLToPath(import.meta.url));
const source = resolve(process.argv[2] ?? resolve(here, "../../src/canada_weather/data/Station Inventory EN.csv"));
const target = resolve(here, "../public/stations.json");

const inventory = parseInventory(readFileSync(source, "utf-8")); // throws on a malformed inventory
const json = JSON.stringify(encodeStations(inventory));
mkdirSync(dirname(target), { recursive: true });
writeFileSync(target, json);
console.log(
  `stations.json: ${inventory.stations.length} stations, inventory dated ${inventory.modified ?? "unknown"}, ` +
    `${(json.length / 1024).toFixed(0)} KB`,
);
