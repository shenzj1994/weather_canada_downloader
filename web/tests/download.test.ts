import { afterEach, describe, expect, it, vi } from "vitest";
import { downloadMerged } from "../src/client/download.ts";
import { ym } from "../src/shared/range.ts";

const HEADER = '"Climate ID","Year","Max Temp (°C)"\n';
const OPTS = { timeframe: "daily" as const, timezone: "lst" as const, dropEmpty: true };
const noop = { signal: new AbortController().signal, onProgress: () => {} };

function stubFetch(handler: (q: URLSearchParams) => Response) {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (url: string) => handler(new URL(url, "http://x").searchParams)),
  );
}

afterEach(() => vi.unstubAllGlobals());

describe("downloadMerged", () => {
  it("merges stations in order with one header", async () => {
    stubFetch((q) => new Response(`${HEADER}"${q.get("climateId")}","${q.get("year")}","1.0"\n`));
    const res = await downloadMerged(
      [
        { climateId: "B", periods: [ym(2020, 1), ym(2021, 1)] },
        { climateId: "A", periods: [ym(2021, 1)] },
      ],
      { ...OPTS, failFast: false },
      "out.csv",
      noop,
    );
    const text = (await res.blob.text()).replace(/^﻿/, "");
    expect(text).toBe(`${HEADER}"B","2020","1.0"\n"B","2021","1.0"\n"A","2021","1.0"\n`);
    expect(res.rows).toBe(3);
    expect([...res.rowsByStation]).toEqual([["B", 2], ["A", 1]]);
    expect(res.failed).toEqual([]);
  });

  it("leaves out a station whose download fails and reports it", async () => {
    stubFetch((q) =>
      q.get("climateId") === "BAD"
        ? new Response(JSON.stringify({ error: "nope" }), { status: 400 })
        : new Response(`${HEADER}"OK","2020","1.0"\n`),
    );
    const jobs = [
      { climateId: "OK", periods: [ym(2020, 1)] },
      { climateId: "BAD", periods: [ym(2020, 1), ym(2021, 1)] },
    ];
    const res = await downloadMerged(jobs, { ...OPTS, failFast: false }, "out.csv", noop);
    expect(res.rows).toBe(1);
    expect(res.failed).toHaveLength(1);
    expect(res.failed[0]).toMatchObject({ climateId: "BAD", error: expect.stringMatching(/^202[01]: nope$/) });
    await expect(downloadMerged(jobs, { ...OPTS, failFast: true }, "out.csv", noop)).rejects.toThrow(/nope/);
  });

  it("fails when there is no data at all", async () => {
    stubFetch(() => new Response(null, { status: 204 }));
    await expect(
      downloadMerged([{ climateId: "A", periods: [ym(2020, 1)] }], { ...OPTS, failFast: false }, "x.csv", noop),
    ).rejects.toThrow(/no data/);
  });
});
