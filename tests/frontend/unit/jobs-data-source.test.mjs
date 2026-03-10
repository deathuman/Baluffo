import test from "node:test";
import assert from "node:assert/strict";
import { parseUnifiedJobsPayload, parseCSVLarge, fetchUnifiedJobs } from "../../../frontend/jobs/data-source.js";

test("jobs data-source parses unified payload variants", () => {
  assert.equal(parseUnifiedJobsPayload([{ id: 1 }], null).length, 1);
  assert.equal(parseUnifiedJobsPayload({ jobs: [{ id: 1 }] }, null).length, 1);
  assert.equal(parseUnifiedJobsPayload({ items: [{ id: 1 }] }, null).length, 1);
});

test("jobs data-source delegates csv parse", () => {
  const rows = parseCSVLarge("a,b", {
    jobsParsing: {
      parseCSVLarge: (_csv, _deps) => [{ id: 1 }]
    },
    parserDeps: {}
  });
  assert.equal(rows.length, 1);
});

test("jobs data-source fetchUnifiedJobs short-circuits on first unified JSON success", async () => {
  const calls = [];
  const result = await fetchUnifiedJobs({
    unifiedJsonSources: [
      { name: "Unified JSON A", url: "json-a" },
      { name: "Unified JSON B", url: "json-b" }
    ],
    unifiedCsvSources: [{ name: "Unified CSV", url: "csv-a" }],
    sheetsFallbackSource: { sheetId: "sheet", gid: "1" },
    parseUnifiedPayload: payload => (Array.isArray(payload?.jobs) ? payload.jobs : []),
    parseCSV: () => [{ id: "csv" }],
    fetcher: async url => {
      calls.push(url);
      if (url === "json-a") {
        return {
          ok: true,
          json: async () => ({ jobs: [{ id: "json" }] })
        };
      }
      return { ok: false, json: async () => ({}), text: async () => "" };
    }
  });

  assert.deepEqual(result.jobs, [{ id: "json" }]);
  assert.equal(result.sourceName, "Unified JSON A");
  assert.deepEqual(calls, ["json-a"]);
});

test("jobs data-source fetchUnifiedJobs falls back to Google Sheets when unified sources fail", async () => {
  const calls = [];
  const result = await fetchUnifiedJobs({
    unifiedJsonSources: [{ name: "Unified JSON", url: "json-a" }],
    unifiedCsvSources: [{ name: "Unified CSV", url: "csv-a" }],
    sheetsFallbackSource: { sheetId: "sheet123", gid: "42" },
    parseUnifiedPayload: () => [],
    parseCSV: () => [{ id: "sheet-job" }],
    fetcher: async url => {
      calls.push(url);
      if (url.includes("spreadsheets/d/sheet123/export")) {
        return { ok: true, text: async () => "x".repeat(180) };
      }
      return {
        ok: false,
        json: async () => ({}),
        text: async () => ""
      };
    }
  });

  assert.deepEqual(result.jobs, [{ id: "sheet-job" }]);
  assert.equal(result.sourceName, "Google Sheets fallback");
  assert.equal(calls[0], "json-a");
  assert.equal(calls[1], "csv-a");
  assert.match(calls[2], /spreadsheets\/d\/sheet123\/export/);
});

test("jobs data-source fetchUnifiedJobs returns final error contract when all sources fail", async () => {
  const result = await fetchUnifiedJobs({
    unifiedJsonSources: [{ name: "Unified JSON", url: "json-a" }],
    unifiedCsvSources: [{ name: "Unified CSV", url: "csv-a" }],
    sheetsFallbackSource: { sheetId: "sheet123", gid: "42" },
    parseUnifiedPayload: () => [],
    parseCSV: () => [],
    fetcher: async () => ({
      ok: false,
      json: async () => ({}),
      text: async () => ""
    })
  });

  assert.equal(result.jobs, null);
  assert.equal(result.sourceName, "");
  assert.equal(result.error, "Could not fetch listings from unified feeds or Sheets fallback source.");
});

test("jobs data-source fetchUnifiedJobs can skip sheets fallback for fast first-load", async () => {
  const calls = [];
  const result = await fetchUnifiedJobs({
    unifiedJsonSources: [{ name: "Unified JSON", url: "json-a" }],
    unifiedCsvSources: [{ name: "Unified CSV", url: "csv-a" }],
    sheetsFallbackSource: { sheetId: "sheet123", gid: "42" },
    allowSheetsFallback: false,
    parseUnifiedPayload: () => [],
    parseCSV: () => [{ id: "sheet-job" }],
    fetcher: async url => {
      calls.push(url);
      return {
        ok: false,
        json: async () => ({}),
        text: async () => ""
      };
    }
  });

  assert.equal(result.jobs, null);
  assert.equal(result.sourceName, "");
  assert.equal(result.error, "Could not fetch listings from local unified feeds.");
  assert.deepEqual(calls, ["json-a", "csv-a"]);
});
