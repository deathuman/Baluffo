import test from "node:test";
import assert from "node:assert/strict";
import {
  parseUnifiedJobsPayload,
  parseCSVLarge,
  fetchUnifiedJobs,
  fetchJsonFromCandidates
} from "../../../frontend/jobs/data-source.js";

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

test("jobs data-source fetchUnifiedJobs races unified JSON mirrors and reports the winner", async () => {
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
    fetcher: async (url, _timeoutMs, init) => {
      calls.push({ url, init });
      if (String(url).startsWith("json-a?")) {
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
  assert.equal(calls.length, 2);
  assert.match(calls[0].url, /^json-a\?t=\d+/);
  assert.equal(calls[0].init.cache, "no-store");
});

test("jobs data-source fetchUnifiedJobs survives a failing primary via racing", async () => {
  const result = await fetchUnifiedJobs({
    unifiedJsonSources: [
      { name: "Dead Mirror", url: "dead-a" },
      { name: "Healthy Mirror", url: "healthy-b" }
    ],
    unifiedCsvSources: [],
    allowSheetsFallback: false,
    parseUnifiedPayload: payload => (Array.isArray(payload?.jobs) ? payload.jobs : []),
    parseCSV: () => [],
    fetcher: async url => {
      if (String(url).startsWith("healthy-b?")) {
        return { ok: true, json: async () => ({ jobs: [{ id: "from-b" }] }) };
      }
      throw new Error("mirror down");
    }
  });

  assert.deepEqual(result.jobs, [{ id: "from-b" }]);
  assert.equal(result.sourceName, "Healthy Mirror");
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
  assert.match(calls[0], /^json-a\?t=\d+/);
  assert.match(calls[1], /^csv-a\?t=\d+/);
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
  assert.equal(calls.length, 2);
  assert.match(calls[0], /^json-a\?t=\d+/);
  assert.match(calls[1], /^csv-a\?t=\d+/);
});

test("jobs data-source local JSON and CSV fetches use no-store cache-busted requests", async () => {
  const calls = [];
  const result = await fetchUnifiedJobs({
    unifiedJsonSources: [{ name: "Unified JSON", url: "jobs-unified.json" }],
    unifiedCsvSources: [{ name: "Unified CSV", url: "jobs-unified.csv" }],
    allowSheetsFallback: false,
    parseUnifiedPayload: () => [],
    parseCSV: () => [],
    fetcher: async (url, _timeoutMs, init) => {
      calls.push({ url, init });
      return {
        ok: false,
        json: async () => ({}),
        text: async () => ""
      };
    }
  });

  assert.equal(result.jobs, null);
  assert.equal(calls.length, 2);
  assert.match(calls[0].url, /^jobs-unified\.json\?t=\d+/);
  assert.equal(calls[0].init.cache, "no-store");
  assert.equal(calls[0].init.headers.Accept, "application/json");
  assert.match(calls[1].url, /^jobs-unified\.csv\?t=\d+/);
  assert.equal(calls[1].init.cache, "no-store");
  assert.equal(calls[1].init.headers.Accept, "text/csv,*/*");
});

test("jobs data-source consumes a hoisted first-render response without refetching", async () => {
  const calls = [];
  const candidates = [];
  const taken = [];
  globalThis.__baluffoJobsEarlyData = {
    take: url => {
      taken.push(url);
      return url === "data/jobs-fetch-report.json"
        ? Promise.resolve({ ok: true, status: 200, json: async () => ({ finishedAt: "" }) })
        : null;
    }
  };
  try {
    const payload = await fetchJsonFromCandidates(["data/jobs-fetch-report.json"], {
      onCandidate: candidate => candidates.push(candidate),
      fetcher: async url => {
        calls.push(url);
        return { ok: false, status: 0, json: async () => ({}) };
      }
    });

    assert.deepEqual(payload, { finishedAt: "" });
    assert.deepEqual(calls, []);
    assert.deepEqual(taken, ["data/jobs-fetch-report.json"]);
    assert.equal(candidates[0].hoisted, true);
    assert.equal(candidates[0].status, 200);
  } finally {
    delete globalThis.__baluffoJobsEarlyData;
  }
});

test("jobs data-source refetches when the hoisted response failed", async () => {
  const calls = [];
  const candidates = [];
  globalThis.__baluffoJobsEarlyData = {
    take: () => Promise.resolve(null)
  };
  try {
    const payload = await fetchJsonFromCandidates(["data/jobs-fetch-report.json"], {
      onCandidate: candidate => candidates.push(candidate),
      fetcher: async url => {
        calls.push(url);
        return { ok: true, status: 200, json: async () => ({ finishedAt: "2026-01-01T00:00:00Z" }) };
      }
    });

    assert.deepEqual(payload, { finishedAt: "2026-01-01T00:00:00Z" });
    assert.equal(calls.length, 1);
    assert.equal(candidates[0].hoisted, false);
  } finally {
    delete globalThis.__baluffoJobsEarlyData;
  }
});

test("jobs data-source JSON candidate fetches preserve existing query before cache-busting", async () => {
  const calls = [];
  await fetchJsonFromCandidates(["data/jobs-fetch-report.json?source=local"], {
    fetcher: async (url, _timeoutMs, init) => {
      calls.push({ url, init });
      return { ok: false, json: async () => ({}) };
    }
  });

  assert.equal(calls.length, 1);
  assert.match(calls[0].url, /^data\/jobs-fetch-report\.json\?source=local&t=\d+/);
  assert.equal(calls[0].init.cache, "no-store");
});
