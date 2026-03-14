import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeLogLevel,
  createLogEvent,
  deriveSourceStatus,
  mergeSourceStatusFromReport,
  applySourceFilter,
  getSourceJobsFoundCount,
  normalizeOpsRuns,
  applyOptimisticDiscoveryRun,
  deriveFetcherProgressModel,
  deriveDiscoveryProgressModel,
  deriveDiscoveryQueuedCount,
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

test("admin domain parses stringified detail rows when merging statuses", () => {
  const rows = mergeSourceStatusFromReport(
    [{ name: "Jagex (Lever)", studio: "Jagex" }],
    {
      sources: [
        {
          name: "lever_sources",
          status: "ok",
          details: [
            "{'adapter': 'lever', 'studio': 'Jagex', 'name': 'Jagex (Lever)', 'status': 'ok', 'fetchedCount': 2, 'keptCount': 2, 'error': ''}"
          ]
        }
      ]
    },
    "active"
  );
  assert.equal(rows[0]._lastStatus, "ok");
  assert.equal(rows[0]._lastKeptCount, 2);
});

test("admin domain matches static source rows by source id from loader name", () => {
  const rows = mergeSourceStatusFromReport(
    [
      {
        id: "static:listing_url:https://www.naconstudiomilan.com/careers",
        name: "Nacon Studio Milan (Manual Website)",
        studio: "Nacon Studio Milan"
      }
    ],
    {
      sources: [
        {
          name: "static_source::static:listing_url:https://www.naconstudiomilan.com/careers",
          status: "ok",
          fetchedCount: 1,
          keptCount: 1
        }
      ]
    },
    "active"
  );
  assert.equal(rows[0]._lastStatus, "ok");
  assert.equal(rows[0]._lastKeptCount, 1);
});

test("admin domain resolves jobs found from merged kept/fetched counters", () => {
  assert.equal(getSourceJobsFoundCount({ _lastKeptCount: 7 }), 7);
  assert.equal(getSourceJobsFoundCount({ _lastFetchedCount: 12 }), 12);
  assert.equal(getSourceJobsFoundCount({ keptCount: 3 }), 3);
});

test("admin domain derives not_run status when no probe/report data exists", () => {
  assert.equal(deriveSourceStatus({ name: "Unknown Source" }), "not_run");
  assert.equal(deriveSourceStatus({ status: "n/a" }), "not_run");
});

test("admin domain normalizes ops runs into current + collapsed completed groups", () => {
  const model = normalizeOpsRuns([
    { id: "p1", type: "pipeline", status: "started", startedAt: "2026-03-08T10:01:00.000Z", finishedAt: "", durationMs: 0 },
    { id: "f1", type: "fetch", status: "started", startedAt: "2026-03-08T10:00:00.000Z", finishedAt: "", durationMs: 0 },
    { id: "f0", type: "fetch", status: "ok", startedAt: "2026-03-08T09:00:00.000Z", finishedAt: "2026-03-08T09:02:00.000Z", durationMs: 120000 },
    { id: "d0", type: "discovery", status: "warning", startedAt: "2026-03-08T08:00:00.000Z", finishedAt: "2026-03-08T08:01:00.000Z", durationMs: 60000 },
    { id: "x1", type: "fetch", status: "ok", startedAt: "2026-03-08T07:00:00.000Z", finishedAt: "2026-03-08T07:01:00.000Z", durationMs: 60000 },
    { id: "x2", type: "discovery", status: "error", startedAt: "2026-03-08T06:00:00.000Z", finishedAt: "2026-03-08T06:01:00.000Z", durationMs: 60000 },
    { id: "x3", type: "fetch", status: "ok", startedAt: "2026-03-08T05:00:00.000Z", finishedAt: "2026-03-08T05:01:00.000Z", durationMs: 60000 }
  ], Date.parse("2026-03-08T10:01:00.000Z"));

  assert.equal(model.currentRows.length, 2);
  assert.equal(model.currentRows[0].displayStatus, "running");
  assert.equal(model.currentRows[0].isLive, true);
  assert.equal(model.hasLiveRuns, true);
  assert.ok(model.liveTypes.includes("pipeline"));
  assert.equal(model.visibleCompletedRows.length, 2);
  assert.equal(model.olderCompletedRows.length, 3);
});

test("admin domain derives adaptive ops polling interval", () => {
  assert.equal(getOpsPollIntervalMs(true), 2000);
  assert.equal(getOpsPollIntervalMs(false), 10000);
});

test("admin domain injects optimistic discovery row into current runs when history lags", () => {
  const baseModel = normalizeOpsRuns([
    { id: "f1", type: "fetch", status: "started", startedAt: "2026-03-08T10:00:00.000Z", finishedAt: "", durationMs: 0 }
  ], Date.parse("2026-03-08T10:01:00.000Z"));

  const model = applyOptimisticDiscoveryRun(baseModel, {
    runId: "disc_1",
    startedAt: "2026-03-08T10:00:30.000Z"
  }, Date.parse("2026-03-08T10:01:00.000Z"));

  assert.equal(model.currentRows.length, 2);
  assert.ok(model.currentRows.some(row => row.type === "discovery" && row.isLive === true && row.optimistic === true));
  assert.ok(model.liveTypes.includes("discovery"));
});

test("admin domain does not duplicate discovery when a live history row already exists", () => {
  const baseModel = normalizeOpsRuns([
    { id: "d1", type: "discovery", status: "started", startedAt: "2026-03-08T10:00:30.000Z", finishedAt: "", durationMs: 0 }
  ], Date.parse("2026-03-08T10:01:00.000Z"));

  const model = applyOptimisticDiscoveryRun(baseModel, {
    runId: "disc_1",
    startedAt: "2026-03-08T10:00:30.000Z"
  }, Date.parse("2026-03-08T10:01:00.000Z"));

  assert.equal(model.currentRows.length, 1);
  assert.equal(model.currentRows[0].optimistic, undefined);
});

test("admin domain suppresses optimistic discovery row once a matching completed run exists", () => {
  const baseModel = normalizeOpsRuns([
    { id: "d1", type: "discovery", status: "ok", startedAt: "2026-03-08T10:00:30.000Z", finishedAt: "2026-03-08T10:01:20.000Z", durationMs: 50000 }
  ], Date.parse("2026-03-08T10:01:30.000Z"));

  const model = applyOptimisticDiscoveryRun(baseModel, {
    runId: "disc_1",
    startedAt: "2026-03-08T10:00:30.000Z"
  }, Date.parse("2026-03-08T10:01:30.000Z"));

  assert.equal(model.currentRows.length, 0);
  assert.equal(model.visibleCompletedRows.length, 1);
});

test("admin domain derives determinate fetcher progress when total sources are known", () => {
  const view = deriveFetcherProgressModel({
    summary: {
      successfulSources: 8,
      failedSources: 1,
      excludedSources: 2,
      outputCount: 45,
      sourceCount: 20
    },
    runtime: {
      selectedSourceCount: 20
    }
  }, { running: true });

  assert.equal(view.active, true);
  assert.equal(view.determinate, true);
  assert.equal(view.ratio, 11 / 20);
  assert.match(view.label, /11\/20 sources resolved/i);
});

test("admin domain falls back to indeterminate discovery progress when only scanning is known", () => {
  const view = deriveDiscoveryProgressModel({
    summary: {
      queuedCandidateCount: 3,
      discoverableButDeferredCount: 2,
      failedProbeCount: 1
    }
  }, { running: true });

  assert.equal(view.active, true);
  assert.equal(view.determinate, false);
  assert.match(view.label, /initializing scan/i);
  assert.match(view.label, /queued 3/i);
  assert.match(view.label, /deferred 2/i);
});

test("admin domain derives queued discovery count from candidate rows when summary is stale", () => {
  const queued = deriveDiscoveryQueuedCount({
    summary: {
      queuedCandidateCount: 0
    },
    candidates: [
      { name: "A" },
      { name: "B", deferred: false },
      { name: "C", deferred: true }
    ]
  });

  assert.equal(queued, 2);
});
