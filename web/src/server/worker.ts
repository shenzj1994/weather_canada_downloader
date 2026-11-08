/**
 * Cloudflare Worker entry point. Static files (the UI and stations.json) are
 * served by Workers Static Assets; only /api/* reaches this Worker
 * (see wrangler.jsonc).
 */

import { createApp } from "./app.ts";
import { stations } from "./inventory.ts";

export default createApp({ stations });
