import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeLogLevel,
  createLogEvent,
  mergeSourceStatusFromReport,
  applySourceFilter,
  getSourceJobsFoundCount,
  normalizeOpsRuns,
  getOpsPollIntervalMs
} from "../../../frontend/admin/domain.js";

test("admin domain normalizes log level and event", () => {
  assert.equal(normalizeLogLevel("warning"), "log-warn");
  const event = createLogEvent("fetcher", "hello", "info");
  assert.equal(event.scope, "fetcher");
  assert.equal(event.message, "hello");
});

test("admin domain merges source statuses and filters", () => {
  const rows = mergeSourceStatusFromReport(
    [{ name: "A" }, { name: "B" }],
    { sources: [{ name: "A", status: "error", fetchedCount: 0 }] },
    "pending"
  );
  assert.equal(rows[0]._lastStatus, "error");
  const filtered = applySourceFilter(rows, "error");
  assert.equal(filtered.length, 1);
  assert.equal(filtered[0].name, "A");
});

test("admin domain maps grouped adapter errors to matching source names", () => {
  const rows = mergeSourceStatusFromReport(
    [
      { name: "InnoGames (Personio)", studio: "InnoGames" },
      { name: "Travian (Personio)", studio: "Travian" }
    ],
    {
      sources: [
        {
          name: "personio_sources",
          studio: "multiple",
          status: "error",
          error: "personio:InnoGames (Personio): HTTP 429 for https://innogames.jobs.personio.de/xml; personio:Travian (Personio): HTTP 429 for https://travian.jobs.personio.de/xml"
        }
      ]
    },
    "active"
  );
  assert.equal(rows[0]._lastStatus, "error");
  assert.equal(rows[1]._lastStatus, "error");
  assert.match(String(rows[0]._lastError || ""), /HTTP 429/i);
});

test("admin domain resolves jobs found from merged kept/fetched counters", () => {
  assert.equal(getSourceJobsFoundCount({ _lastKeptCount: 7 }), 7);
  assert.equal(getSourceJobsFoundCount({ _lastFetchedCount: 12 }), 12);
  assert.equal(getSourceJobsFoundCount({ keptCount: 3 }), 3);
});

test("admin domain normalizes ops runs into current + collapsed completed groups", () => {
  const model = normalizeOpsRuns([
    { id: "f1", type: "fetch", status: "started", startedAt: "2026-03-08T10:00:00.000Z", finishedAt: "", durationMs: 0 },
    { id: "f0", type: "fetch", status: "ok", startedAt: "2026-03-08T09:00:00.000Z", finishedAt: "2026-03-08T09:02:00.000Z", durationMs: 120000 },
    { id: "d0", type: "discovery", status: "warning", startedAt: "2026-03-08T08:00:00.000Z", finishedAt: "2026-03-08T08:01:00.000Z", durationMs: 60000 },
    { id: "x1", type: "fetch", status: "ok", startedAt: "2026-03-08T07:00:00.000Z", finishedAt: "2026-03-08T07:01:00.000Z", durationMs: 60000 },
    { id: "x2", type: "discovery", status: "error", startedAt: "2026-03-08T06:00:00.000Z", finishedAt: "2026-03-08T06:01:00.000Z", durationMs: 60000 },
    { id: "x3", type: "fetch", status: "ok", startedAt: "2026-03-08T05:00:00.000Z", finishedAt: "2026-03-08T05:01:00.000Z", durationMs: 60000 }
  ], Date.parse("2026-03-08T10:01:00.000Z"));

  assert.equal(model.currentRows.length, 1);
  assert.equal(model.currentRows[0].displayStatus, "running");
  assert.equal(model.currentRows[0].isLive, true);
  assert.equal(model.hasLiveRuns, true);
  assert.equal(model.visibleCompletedRows.length, 2);
  assert.equal(model.olderCompletedRows.length, 3);
});

test("admin domain derives adaptive ops polling interval", () => {
  assert.equal(getOpsPollIntervalMs(true), 2000);
  assert.equal(getOpsPollIntervalMs(false), 10000);
});
