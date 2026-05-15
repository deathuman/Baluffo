import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeLogLevel,
  createLogEvent,
  deriveSourceApprovalStatus,
  deriveSourceStatus,
  deriveFetcherFailureSummary,
  mergeSourceStatusFromReport,
  applySourceFilter,
  getSourceDiscoveryJobsCount,
  getSourceFetchJobsCount,
  getSourceJobsFoundCount,
  normalizeOpsRuns,
  applyOptimisticDiscoveryRun,
  applyOptimisticFetchRun,
  deriveAdminRunsModel,
  deriveFetcherProgressModel,
  deriveDiscoveryProgressModel,
  deriveDiscoveryLifecycleCounts,
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

test("admin domain derives bucketed fetch failure summary with examples", () => {
  const summary = deriveFetcherFailureSummary({
    sources: [
      {
        name: "ashby_sources",
        adapter: "ashby",
        status: "error",
        error: "ashby:Jagex (Ashby): no jobs extracted from ashby board html; ashby:Scopely (Ashby): no jobs extracted from ashby board html"
      },
      {
        name: "personio_sources",
        adapter: "personio",
        status: "ok",
        details: [
          {
            adapter: "personio",
            name: "InnoGames (Personio)",
            status: "error",
            error: "HTTP 429 for https://innogames.jobs.personio.de/xml"
          },
          {
            adapter: "personio",
            name: "Travian (Personio)",
            status: "error",
            error: "HTTP 429 for https://travian.jobs.personio.de/xml"
          }
        ]
      },
      {
        name: "greenhouse_boards",
        adapter: "greenhouse",
        status: "ok",
        details: [
          {
            adapter: "greenhouse",
            name: "Example Studio GmbH (Greenhouse)",
            status: "error",
            error: "HTTP 404 for https://boards-api.greenhouse.io/v1/boards/examplestudio/jobs?content=true"
          }
        ]
      }
    ]
  });

  assert.equal(summary.topLevelFailedSources, 1);
  assert.equal(summary.detailFailureCount, 3);
  const extractZero = summary.buckets.find(bucket => bucket.key === "extract_zero");
  const rateLimited = summary.buckets.find(bucket => bucket.key === "provider_rate_limited");
  const badConfig = summary.buckets.find(bucket => bucket.key === "provider_not_found_or_bad_config");
  assert.equal(extractZero?.count, 1);
  assert.ok(extractZero?.examples.includes("ashby_sources"));
  assert.equal(rateLimited?.count, 2);
  assert.ok(rateLimited?.examples.includes("InnoGames (Personio)"));
  assert.equal(badConfig?.count, 1);
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

test("admin domain separates discovery and fetch job counts", () => {
  const row = { jobsFound: 0, sampleCount: 0, _lastKeptCount: 179, _lastStatus: "ok" };
  assert.equal(getSourceDiscoveryJobsCount(row), 0);
  assert.equal(getSourceFetchJobsCount(row), 179);
  assert.equal(getSourceJobsFoundCount(row), 0);
  assert.equal(getSourceDiscoveryJobsCount({ name: "Missing discovery evidence" }), 0);
});

test("admin domain derives pending approval status from discovery evidence", () => {
  assert.equal(
    deriveSourceApprovalStatus({ jobsFound: 0, _lastKeptCount: 179, _lastStatus: "ok" }, "pending").label,
    "Blocked: 0 discovery jobs"
  );
  assert.equal(
    deriveSourceApprovalStatus({ jobsFound: 2, _lastStatus: "ok" }, "pending").label,
    "Auto-approvable"
  );
  assert.equal(
    deriveSourceApprovalStatus({ jobsFound: 2, status: "error" }, "pending").label,
    "Blocked: error"
  );
  assert.equal(
    deriveSourceApprovalStatus({ jobsFound: 2, deferred: true, deferReason: "adapter_cap" }, "pending").label,
    "Auto-approvable"
  );
  assert.equal(
    deriveSourceApprovalStatus(
      { jobsFound: 2, deferred: true, deferReason: "adapter_cap", rankReasons: ["existing_family_match"] },
      "pending"
    ).label,
    "Skipped: existing source"
  );
  assert.equal(
    deriveSourceApprovalStatus(
      { jobsFound: 2, deferred: true, deferReason: "adapter_cap", weakSignal: true },
      "pending"
    ).label,
    "Deferred: weak signal"
  );
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

  assert.equal(model.currentRows.length, 0);
  assert.equal(model.hasLiveRuns, false);
  assert.equal(model.visibleCompletedRows.length, 2);
  assert.equal(model.olderCompletedRows.length, 3);
  assert.equal(model.visibleCompletedRows[0].displayStatus, "ok");
  assert.equal(model.visibleCompletedRows[0].elapsedMs, 120000);
});


test("admin domain orders completed runs by finishedAt across mixed durations", () => {
  const model = normalizeOpsRuns([
    {
      id: "fetch-long",
      type: "fetch",
      status: "ok",
      startedAt: "2026-03-08T08:00:00.000Z",
      finishedAt: "2026-03-08T10:00:00.000Z",
      durationMs: 7200000
    },
    {
      id: "sync-short",
      type: "sync",
      status: "ok",
      startedAt: "2026-03-08T09:00:00.000Z",
      finishedAt: "2026-03-08T09:05:00.000Z",
      durationMs: 300000
    },
    {
      id: "discovery-old",
      type: "discovery",
      status: "warning",
      startedAt: "2026-03-08T07:00:00.000Z",
      finishedAt: "2026-03-08T07:10:00.000Z",
      durationMs: 600000
    }
  ]);

  assert.equal(model.visibleCompletedRows.length, 2);
  assert.equal(model.visibleCompletedRows[0].type, "fetch");
  assert.equal(model.visibleCompletedRows[1].type, "sync");
  assert.equal(model.olderCompletedRows[0].type, "discovery");
});

test("admin domain orders older completed runs by finishedAt with startedAt fallback", () => {
  const model = normalizeOpsRuns([
    {
      id: "recent-1",
      type: "fetch",
      status: "ok",
      startedAt: "2026-03-08T09:00:00.000Z",
      finishedAt: "2026-03-08T10:00:00.000Z",
      durationMs: 3600000
    },
    {
      id: "recent-2",
      type: "sync",
      status: "ok",
      startedAt: "2026-03-08T09:30:00.000Z",
      finishedAt: "2026-03-08T09:45:00.000Z",
      durationMs: 900000
    },
    {
      id: "older-newest",
      type: "pipeline",
      status: "ok",
      startedAt: "2026-03-08T08:00:00.000Z",
      finishedAt: "2026-03-08T09:30:00.000Z",
      durationMs: 5400000
    },
    {
      id: "older-fallback",
      type: "fetch",
      status: "ok",
      startedAt: "2026-03-08T09:15:00.000Z",
      finishedAt: "not-a-date",
      durationMs: 600000
    },
    {
      id: "older-oldest",
      type: "discovery",
      status: "warning",
      startedAt: "2026-03-08T07:00:00.000Z",
      finishedAt: "2026-03-08T07:30:00.000Z",
      durationMs: 1800000
    }
  ]);

  assert.deepEqual(model.visibleCompletedRows.map(row => row.id), ["recent-1", "recent-2"]);
  assert.deepEqual(model.olderCompletedRows.map(row => row.id), ["older-newest", "older-fallback", "older-oldest"]);
});

test("admin domain attaches ordered pipeline children without hiding child rows", () => {
  const model = normalizeOpsRuns([
    {
      id: "pipeline_1",
      runId: "pipeline_1",
      type: "pipeline",
      status: "ok",
      startedAt: "2026-03-08T09:00:00.000Z",
      finishedAt: "2026-03-08T10:00:00.000Z",
      durationMs: 3600000
    },
    {
      id: "sync_1",
      runId: "sync_1",
      type: "sync",
      status: "ok",
      parentRunId: "pipeline_1",
      parentTaskType: "pipeline",
      startedAt: "2026-03-08T09:50:00.000Z",
      finishedAt: "2026-03-08T09:59:00.000Z",
      durationMs: 540000
    },
    {
      id: "fetch_1",
      runId: "fetch_1",
      type: "fetch",
      status: "ok",
      parentRunId: "pipeline_1",
      parentTaskType: "pipeline",
      startedAt: "2026-03-08T09:20:00.000Z",
      finishedAt: "2026-03-08T09:45:00.000Z",
      durationMs: 1500000
    },
    {
      id: "discovery_1",
      runId: "discovery_1",
      type: "discovery",
      status: "ok",
      parentRunId: "pipeline_1",
      parentTaskType: "pipeline",
      startedAt: "2026-03-08T09:01:00.000Z",
      finishedAt: "2026-03-08T09:10:00.000Z",
      durationMs: 540000
    }
  ]);

  const pipeline = model.visibleCompletedRows[0];
  assert.equal(pipeline.type, "pipeline");
  assert.deepEqual(pipeline.pipelineChildren.map(row => row.type), ["discovery", "fetch", "sync"]);
  assert.deepEqual(
    [...model.visibleCompletedRows, ...model.olderCompletedRows].map(row => row.id).sort(),
    ["discovery_1", "fetch_1", "pipeline_1", "sync_1"]
  );
});

test("admin domain derives adaptive ops polling interval", () => {
  assert.equal(getOpsPollIntervalMs(true), 2000);
  assert.equal(getOpsPollIntervalMs(false), 10000);
});

test("admin domain keeps discovery optimistic helpers as no-ops", () => {
  const cases = [
    {
      label: "active fetch history",
      baseRows: [
        { id: "f1", type: "fetch", status: "started", startedAt: "2026-03-08T10:00:00.000Z", finishedAt: "", durationMs: 0 }
      ],
      nowMs: Date.parse("2026-03-08T10:01:00.000Z")
    },
    {
      label: "staged discovery history",
      baseRows: [
        { id: "d1", type: "discovery", status: "started", startedAt: "2026-03-08T10:00:30.000Z", finishedAt: "", durationMs: 0 }
      ],
      nowMs: Date.parse("2026-03-08T10:01:00.000Z")
    },
    {
      label: "completed discovery history",
      baseRows: [
        {
          id: "d1",
          type: "discovery",
          status: "ok",
          startedAt: "2026-03-08T10:00:30.000Z",
          finishedAt: "2026-03-08T10:01:20.000Z",
          durationMs: 50000
        }
      ],
      nowMs: Date.parse("2026-03-08T10:01:30.000Z")
    }
  ];

  for (const { label, baseRows, nowMs } of cases) {
    const baseModel = normalizeOpsRuns(baseRows, nowMs);
    const model = applyOptimisticDiscoveryRun(
      baseModel,
      {
        runId: "disc_1",
        startedAt: "2026-03-08T10:00:30.000Z"
      },
      nowMs
    );

    assert.equal(model.currentRows.length, baseModel.currentRows.length, label);
    assert.equal(model.visibleCompletedRows.length, baseModel.visibleCompletedRows.length, label);
    assert.equal(model.olderCompletedRows.length, baseModel.olderCompletedRows.length, label);
    assert.equal(model.hasLiveRuns, baseModel.hasLiveRuns, label);
    assert.deepEqual(model.liveTypes, baseModel.liveTypes, label);
    assert.ok(!model.currentRows.some(row => row.optimistic === true), label);
  }
});

test("admin domain keeps fetch optimistic helpers as no-ops", () => {
  const cases = [
    {
      label: "empty fetch history",
      baseRows: [],
      nowMs: Date.parse("2026-03-08T10:01:00.000Z")
    },
    {
      label: "completed fetch history",
      baseRows: [
        {
          id: "f1",
          runId: "fetch_1",
          type: "fetch",
          status: "warning",
          startedAt: "2026-03-08T10:00:30.000Z",
          finishedAt: "2026-03-08T10:01:20.000Z",
          durationMs: 50000
        }
      ],
      nowMs: Date.parse("2026-03-08T10:01:30.000Z")
    }
  ];

  for (const { label, baseRows, nowMs } of cases) {
    const baseModel = normalizeOpsRuns(baseRows, nowMs);
    const model = applyOptimisticFetchRun(
      baseModel,
      {
        runId: "fetch_1",
        startedAt: "2026-03-08T10:00:30.000Z"
      },
      nowMs
    );

    assert.equal(model.currentRows.length, baseModel.currentRows.length, label);
    assert.equal(model.visibleCompletedRows.length, baseModel.visibleCompletedRows.length, label);
    assert.equal(model.olderCompletedRows.length, baseModel.olderCompletedRows.length, label);
    assert.equal(model.hasLiveRuns, baseModel.hasLiveRuns, label);
    assert.deepEqual(model.liveTypes, baseModel.liveTypes, label);
    assert.ok(!model.currentRows.some(row => row.optimistic === true), label);
  }
});

test("admin domain derives current runs from task state and completed runs from history", () => {
  const model = deriveAdminRunsModel(
    {
      taskState: {
        tasks: [
          {
            taskType: "discovery",
            runId: "disc_1",
            active: true,
            startedAt: "2026-03-08T10:00:30.000Z",
            status: "running",
            taskProgress: {
              active: true,
              phaseKey: "scanning_sources",
              phaseLabel: "Scanning known careers pages",
              mode: "indeterminate",
              ratio: 0,
              counts: {}
            },
            summary: { queuedCandidateCount: 3 }
          }
        ]
      },
      historyRuns: [
        { id: "h1", runId: "disc_1", type: "discovery", status: "started", startedAt: "2026-03-08T10:00:30.000Z", finishedAt: "", durationMs: 0 },
        { id: "h2", runId: "fetch_1", type: "fetch", status: "ok", startedAt: "2026-03-08T09:00:00.000Z", finishedAt: "2026-03-08T09:01:00.000Z", durationMs: 60000 }
      ]
    },
    Date.parse("2026-03-08T10:01:00.000Z")
  );

  assert.equal(model.currentRows.length, 1);
  assert.equal(model.currentRows[0].type, "discovery");
  assert.equal(model.currentRows[0].runId, "disc_1");
  assert.equal(model.currentRows[0].isLive, true);
  assert.equal(model.currentRows[0].taskProgress.phaseLabel, "Scanning known careers pages");
  assert.equal(model.visibleCompletedRows.length, 1);
  assert.equal(model.visibleCompletedRows[0].type, "fetch");
});

test("admin domain drops optimistic fetch row once task state confirms the active run", () => {
  const model = deriveAdminRunsModel(
    {
      taskState: {
        tasks: [
          {
            taskType: "fetch",
            runId: "fetch_1",
            active: true,
            startedAt: "2026-03-08T10:00:30.000Z",
            status: "running",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 0.5,
              counts: {}
            }
          }
        ]
      },
      historyRuns: [],
      optimisticFetchRun: {
        runId: "fetch_1",
        startedAt: "2026-03-08T10:00:30.000Z"
      }
    },
    Date.parse("2026-03-08T10:01:00.000Z")
  );

  assert.equal(model.currentRows.length, 1);
  assert.equal(model.currentRows[0].optimistic, undefined);
  assert.equal(model.currentRows[0].runId, "fetch_1");
});

test("admin domain drops stale fetch task state when history already has the completed run", () => {
  const model = deriveAdminRunsModel(
    {
      taskState: {
        tasks: [
          {
            taskType: "fetch",
            runId: "fetch_1",
            active: true,
            startedAt: "2026-03-08T10:00:30.000Z",
            status: "running",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 0.9,
              counts: {}
            }
          }
        ]
      },
      historyRuns: [
        {
          id: "fetch_1",
          runId: "fetch_1",
          type: "fetch",
          status: "ok",
          startedAt: "2026-03-08T10:00:30.000Z",
          finishedAt: "2026-03-08T10:05:30.000Z",
          durationMs: 300000
        }
      ]
    },
    Date.parse("2026-03-08T10:06:00.000Z")
  );

  assert.equal(model.currentRows.length, 1);
  assert.equal(model.visibleCompletedRows.length, 1);
  assert.equal(model.visibleCompletedRows[0].type, "fetch");
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

test("admin domain prefers shared fetch task progress contract over raw mixed-unit counters", () => {
  const view = deriveFetcherProgressModel({
    summary: {
      successfulSources: 13,
      failedSources: 0,
      excludedSources: 511,
      outputCount: 34828,
      sourceCount: 524
    },
    runtime: {
      selectedSourceCount: 13
    },
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "determinate",
      ratio: 0.5,
      counts: {
        resolvedSources: 6,
        sourceCount: 12,
        outputCount: 34828,
        failedSources: 0,
        excludedSources: 3
      }
    }
  }, { running: true });

  assert.equal(view.active, true);
  assert.equal(view.determinate, true);
  assert.equal(view.ratio, 0.5);
  assert.match(view.label, /Executing sources/i);
  assert.match(view.label, /6\/12 sources resolved/i);
  assert.doesNotMatch(view.label, /524\/13/i);
});

test("admin domain keeps fetcher progress indeterminate while only reported source rows are growing", () => {
  const view = deriveFetcherProgressModel({
    summary: {
      successfulSources: 1,
      failedSources: 0,
      excludedSources: 510,
      outputCount: 34828,
      sourceCount: 511
    },
    runtime: {
      selectedSourceCount: 0
    }
  }, { running: true });

  assert.equal(view.active, true);
  assert.equal(view.determinate, false);
  assert.equal(view.ratio, 0);
  assert.match(view.label, /511 sources resolved/i);
  assert.doesNotMatch(view.label, /511\/511/i);
});

test("admin domain ignores stale active fetch task progress when report is finished", () => {
  const view = deriveFetcherProgressModel({
    finishedAt: "2026-03-23T16:18:10.053424+00:00",
    summary: {
      successfulSources: 38,
      failedSources: 23,
      excludedSources: 0,
      outputCount: 3683,
      sourceCount: 61
    },
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "determinate",
      ratio: 0.18,
      counts: {
        resolvedSources: 61,
        sourceCount: 520,
        outputCount: 3683,
        failedSources: 23,
        excludedSources: 0
      }
    }
  }, { running: false });

  assert.equal(view.active, false);
  assert.equal(view.determinate, false);
  assert.equal(view.ratio, 0);
  assert.equal(view.label, "");
});

test("admin domain keeps fetcher progress indeterminate when runtime loader count and resolved source rows use different units", () => {
  const view = deriveFetcherProgressModel({
    summary: {
      successfulSources: 13,
      failedSources: 0,
      excludedSources: 511,
      outputCount: 34828,
      sourceCount: 524
    },
    runtime: {
      selectedSourceCount: 13
    }
  }, { running: true });

  assert.equal(view.active, true);
  assert.equal(view.determinate, false);
  assert.equal(view.ratio, 0);
  assert.match(view.label, /524 sources resolved/i);
  assert.doesNotMatch(view.label, /524\/13/i);
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
  assert.equal(view.determinate, true);
  assert.equal(view.ratio, 0.5);
  assert.match(view.label, /initializing scan/i);
  assert.match(view.label, /queued 3/i);
  assert.match(view.label, /deferred 2/i);
});

test("admin domain uses phase hints through the shared discovery progress mapper before report progress arrives", () => {
  const view = deriveDiscoveryProgressModel(null, {
    running: true,
    phaseHint: "Scanning known careers pages"
  });

  assert.equal(view.active, true);
  assert.equal(view.determinate, true);
  assert.equal(view.ratio, 0.5);
  assert.match(view.label, /Scanning known careers pages/i);
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

test("admin domain exposes additive lifecycle counts from discovery summary", () => {
  const counts = deriveDiscoveryLifecycleCounts({
    summary: {
      validatedCandidateCount: 7,
      approvedCandidateCount: 3,
      liveCandidateCount: 2,
      quarantinedCandidateCount: 1
    }
  });

  assert.deepEqual(counts, {
    validated: 7,
    approved: 3,
    live: 2,
    quarantined: 1
  });
});

test("admin domain uses probe totals instead of found endpoints for discovery progress", () => {
  const view = deriveDiscoveryProgressModel({
    summary: {
      phaseLabel: "Probing 124 candidate(s)",
      foundEndpointCount: 785,
      probedCandidateCount: 0,
      queuedCandidateCount: 0,
      failedProbeCount: 0,
      lossAccounting: {
        generated: 785,
        dedupSkipped: 661,
        validationSkipped: 2,
        lowEvidenceSkipped: 2
      }
    }
  }, { running: true });

  assert.equal(view.active, true);
  assert.equal(view.determinate, true);
  assert.equal(view.ratio, 0.6);
  assert.match(view.label, /probed 0\/120/i);
});

test("admin domain lets live discovery phase hints override a stale starting task progress shell", () => {
  const view = deriveDiscoveryProgressModel({
    summary: {
      phaseLabel: "Initializing scan",
      foundEndpointCount: 0,
      probedCandidateCount: 0,
      queuedCandidateCount: 0
    },
    taskProgress: {
      active: true,
      phaseKey: "starting",
      phaseLabel: "Initializing scan",
      mode: "indeterminate",
      ratio: 0,
      counts: {
        foundEndpoints: 0,
        probedCandidates: 0,
        probeTotal: 0,
        queuedCandidates: 0,
        deferredCandidates: 0,
        failedProbes: 0
      }
    }
  }, { running: true, phaseHint: "Running web-search discovery queries" });

  assert.equal(view.active, true);
  assert.equal(view.determinate, true);
  assert.equal(view.ratio, 0.28);
  assert.match(view.label, /Running web-search discovery queries/i);
  assert.doesNotMatch(view.label, /Initializing scan/i);
});

test("admin domain maps discovery finalizing and completed phases to near-complete and complete fill", () => {
  const finalizing = deriveDiscoveryProgressModel({
    taskProgress: {
      active: true,
      phaseKey: "finalizing",
      phaseLabel: "Finalizing discovery report",
      mode: "indeterminate",
      ratio: 0,
      counts: {}
    }
  }, { running: true });
  const completed = deriveDiscoveryProgressModel({
    finishedAt: "2026-03-08T10:02:00.000Z",
    taskProgress: {
      active: false,
      phaseKey: "completed",
      phaseLabel: "Discovery completed",
      mode: "determinate",
      ratio: 1,
      counts: {}
    }
  }, { running: false });

  assert.equal(finalizing.determinate, true);
  assert.equal(finalizing.ratio, 0.96);
  assert.match(finalizing.label, /Finalizing discovery report/i);
  assert.equal(completed.active, false);
});
