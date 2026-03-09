import test from "node:test";
import assert from "node:assert/strict";
import { normalizeSourceRows, renderDataSourcesPanel } from "../../../frontend/jobs/source-metadata.js";

test("jobs source metadata keeps Google Sheets as a core source", () => {
  const result = normalizeSourceRows([], null, { sheetId: "sheet123", gid: "77" });
  const names = result.rows.map(row => row.name);
  assert.ok(names.includes("Google Sheets"));
  assert.match(
    result.rows.find(row => row.name === "Google Sheets")?.url || "",
    /spreadsheets\/d\/sheet123\/edit\?gid=77/
  );
});

test("jobs source metadata includes active registry rows even when disabled by default", () => {
  const result = normalizeSourceRows(
    [{ name: "Disabled Source", listing_url: "https://example.com/careers", enabledByDefault: false }],
    null,
    { sheetId: "sheet123", gid: "77" }
  );
  const names = result.rows.map(row => row.name);
  assert.ok(names.includes("Disabled Source"));
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
  assert.match(listEl.innerHTML, /fetched 5, kept 3/);
  assert.match(listEl.innerHTML, /Blocked Source/);
  assert.match(listEl.innerHTML, /fetched 0, kept 0/);
  assert.match(captionEl.textContent, /latest fetch report/i);
});
