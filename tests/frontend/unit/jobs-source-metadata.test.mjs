import test from "node:test";
import assert from "node:assert/strict";
import {
  getJobsFetchReportUrlsForRuntime,
  getSourceRegistryActiveUrlsForRuntime
} from "../../../frontend/jobs/app/sources.js";
import { normalizeSourceRows, renderDataSourcesPanel } from "../../../frontend/jobs/render.js";

test("jobs source metadata keeps Google Sheets as a core source", () => {
  const result = normalizeSourceRows([], null, { sheetId: "sheet123", gid: "77" });
  const names = result.rows.map(row => row.name);
  assert.ok(names.includes("Google Sheets"));
  assert.match(
    result.rows.find(row => row.name === "Google Sheets")?.url || "",
    /spreadsheets\/d\/sheet123\/edit\?gid=77/
  );
});

test("jobs source metadata skips static registry fetches in desktop runtime", () => {
  const originalWindow = globalThis.window;
  globalThis.window = {
    location: { href: "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877" }
  };
  try {
    assert.deepEqual(getSourceRegistryActiveUrlsForRuntime(), []);
  } finally {
    if (originalWindow === undefined) {
      delete globalThis.window;
    } else {
      globalThis.window = originalWindow;
    }
  }
});

test("jobs source metadata omits fetch report URLs in container runtime", () => {
  const originalConfig = globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
  globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = { runtime: { mode: "container" } };
  try {
    assert.deepEqual(getJobsFetchReportUrlsForRuntime(), []);
  } finally {
    if (originalConfig === undefined) {
      delete globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG;
    } else {
      globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = originalConfig;
    }
  }
});

test("jobs source metadata omits active registry rows that are disabled by default", () => {
  const result = normalizeSourceRows(
    [{ name: "Disabled Source", listing_url: "https://example.com/careers", enabledByDefault: false }],
    null,
    { sheetId: "sheet123", gid: "77" }
  );
  const names = result.rows.map(row => row.name);
  assert.equal(names.includes("Disabled Source"), false);
});

test("jobs source metadata sanitizes urls and compacts static source noise", () => {
  const activeRegistry = [
    { name: "Unsafe Source", listing_url: "javascript:alert(1)", adapter: "greenhouse" },
    { name: "Static A", listing_url: "https://a.example.com/jobs", adapter: "static" },
    { name: "Static B", listing_url: "https://b.example.com/jobs", adapter: "static" },
    { name: "Static C", listing_url: "https://c.example.com/jobs", adapter: "static" },
    { name: "Static D", listing_url: "https://d.example.com/jobs", adapter: "static" },
    { name: "Static E", listing_url: "https://e.example.com/jobs", adapter: "static" },
    { name: "Static F", listing_url: "https://f.example.com/jobs", adapter: "static" },
    { name: "Static G", listing_url: "https://g.example.com/jobs", adapter: "static" },
    { name: "Static H", listing_url: "https://h.example.com/jobs", adapter: "static" },
    { name: "Static I", listing_url: "https://i.example.com/jobs", adapter: "static" }
  ];
  const fetchReport = {
    sources: [
      { name: "static_source::static:listing_url:https://one.example.com/jobs", status: "excluded", error: "only_sources_filter" },
      { name: "static_source::static:listing_url:https://two.example.com/jobs", status: "excluded", error: "only_sources_filter" }
    ]
  };

  const result = normalizeSourceRows(activeRegistry, fetchReport, { sheetId: "sheet123", gid: "77" });
  const unsafe = result.rows.find(row => row.name === "Unsafe Source");
  assert.equal(unsafe?.url || "", "");
  assert.ok(result.rows.some(row => row.name === "Static sources"));
  assert.ok(result.rows.some(row => row.name === "Static sources (excluded)"));
  assert.equal(result.rows.filter(row => row.name === "Static I").length, 0);
});

test("jobs source metadata panel renders excluded source note and fetch report counters", async () => {
  const listEl = { innerHTML: "" };
  const captionEl = { textContent: "" };
  const fetchJsonFromCandidates = async urls => {
    if (Array.isArray(urls) && urls[0] === "active-a") {
      return [{ name: "Greenhouse", api_url: "https://boards-api.greenhouse.io/v1/boards/acme/jobs", enabledByDefault: true }];
    }
    return {
      finishedAt: "2026-03-08T10:00:00.000Z",
      sources: [
        { name: "Greenhouse", fetchedCount: 5, keptCount: 3, status: "ok" },
        { name: "Blocked Source", status: "excluded", error: "rate limited" }
      ]
    };
  };

  await renderDataSourcesPanel({
    dataSourcesListEl: listEl,
    dataSourcesCaptionEl: captionEl,
    sourceRegistryActiveUrls: ["active-a"],
    jobsFetchReportUrls: ["report-a"],
    sheetsFallbackSource: { sheetId: "sheet123", gid: "77" },
    fetchJsonFromCandidates
  });

  assert.match(listEl.innerHTML, /Google Sheets/);
  assert.match(listEl.innerHTML, /Greenhouse/);
  assert.match(listEl.innerHTML, /Greenhouse/);
  assert.match(listEl.innerHTML, /Blocked Source/);
  assert.match(listEl.innerHTML, /fetched 0, kept 0/);
  assert.match(captionEl.textContent, /latest fetch report/i);
});

test("jobs source metadata panel shows unavailable copy only after fetch failure", async () => {
  const listEl = { innerHTML: "" };
  const captionEl = { textContent: "" };

  await renderDataSourcesPanel({
    dataSourcesListEl: listEl,
    dataSourcesCaptionEl: captionEl,
    sourceRegistryActiveUrls: ["active-a"],
    jobsFetchReportUrls: ["report-a"],
    sheetsFallbackSource: { sheetId: "sheet123", gid: "77" },
    fetchJsonFromCandidates: async () => {
      throw new Error("bridge unavailable");
    }
  });

  assert.match(listEl.innerHTML, /Source metadata unavailable/);
  assert.equal(captionEl.textContent, "Source metadata unavailable.");
});
