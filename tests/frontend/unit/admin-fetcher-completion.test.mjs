import test from "node:test";

import assert from "node:assert/strict";

import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";

import { createClassList, createElement, createFetcherControllerFixture } from "./helpers/admin-controller-test-helpers.mjs";

test("admin fetcher controller syncs source tables once after completion", async () => {
  const syncCalls = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLogRemoteOffset: 0,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: {
      runId: "fetch_done_1",
      startedAt: "2026-03-08T10:00:00.000Z"
    },
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
    adminFetcherProgressBarEl: createElement({ style: {} }),
    adminFetcherProgressLabelEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  let manualReportLoads = 0;
  let summaryLoads = 0;
  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch?view=summary") {
          return {
            runId: "fetch_done_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "2026-03-08T10:05:00.000Z",
            taskProgress: {
              active: false,
              phaseKey: "completed",
              phaseLabel: "Completed",
              mode: "determinate",
              ratio: 1,
              counts: { resolvedSources: 8, sourceCount: 8, outputCount: 12 }
            }
          };
        }
        if (path === "/ops/fetch-report?view=summary") { summaryLoads += 1; return { runId: "fetch_done_1", startedAt: "2026-03-08T10:00:00.000Z", finishedAt: "2026-03-08T10:05:00.000Z", summary: { outputCount: 12, failedSources: 0, excludedSources: 0, sourceCount: 8 }, taskProgress: { active: false, phaseKey: "completed", phaseLabel: "Completed", mode: "determinate", ratio: 1, counts: { resolvedSources: 8, sourceCount: 8, outputCount: 12 } } }; }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async options => {
        if (options?.live) {
          return {
            runId: "fetch_done_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "2026-03-08T10:05:00.000Z",
            summary: { outputCount: 12, failedSources: 0, excludedSources: 0, sourceCount: 8 },
            taskProgress: {
              active: false,
              phaseKey: "completed",
              phaseLabel: "Completed",
              mode: "determinate",
              ratio: 1,
              counts: { resolvedSources: 8, sourceCount: 8, outputCount: 12 }
            }
          };
        }
        manualReportLoads += 1;
        return {
          runId: "fetch_done_1",
          startedAt: "2026-03-08T10:00:00.000Z",
          finishedAt: "2026-03-08T10:05:00.000Z",
          summary: { outputCount: 12, failedSources: 0, excludedSources: 0, sourceCount: 8 },
          taskProgress: {
            active: false,
            phaseKey: "completed",
            phaseLabel: "Completed",
            mode: "determinate",
            ratio: 1,
            counts: { resolvedSources: 8, sourceCount: 8, outputCount: 12 }
          },
          sources: [{ name: "Studio A", status: "ok", durationMs: 1200 }]
        };
      },
      writeJobsAutoRefreshSignal() {},
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown"),
      logAdminError() {},
      setBusyFlag(key, value) {
        state.adminBusyState[key] = value;
      },
      getSourceStatusSetter: () => () => {},
      loadOpsHealthData: async () => {},
      jobsAutoRefreshSignalKey: "k",
      jobsFetcherCommand: "python -m src.jobs_fetcher",
      jobsFetcherTaskLabel: "Run jobs fetcher",
      syncSourceTablesAfterTaskCompletion: async payload => {
        syncCalls.push(payload);
      },
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow() {}
    });

    controller.startFetcherCompletionWatch();
    await scheduled[scheduled.length - 1]();

    assert.equal(manualReportLoads, 0);
    assert.equal(summaryLoads, 1);
    assert.equal(syncCalls.length, 1);
    assert.equal(syncCalls[0].taskType, "fetch");
    assert.equal(syncCalls[0].completionSignature, "fetch_done_1|2026-03-08T10:05:00.000Z");
    assert.equal((state.latestFetcherReportCache || {}).runId, "fetch_done_1");
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller forwards uncapped preset payload", async () => {
  const calls = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: 0,
    fetcherLogRemoteOffset: 0,
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: null,
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: false,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
  };
  const refs = {
    adminFetcherLogEl: createElement(),
    adminRunFetcherBtnEl: createElement(),
    adminRunFetcherIncrementalBtnEl: createElement(),
    adminRunFetcherUncappedBtnEl: createElement(),
    adminRunFetcherForceBtnEl: createElement(),
    adminRetryFailedBtnEl: createElement()
  };
  const controller = createAdminFetcherController({
    state,
    refs,
    getBridge: async path => {
      if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
        return { text: "", nextOffset: 0 };
      }
      return {};
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      return {
        started: true,
        runId: "fetch_uncapped",
        startedAt: "2026-03-08T10:01:00.000Z",
        preset: "uncapped",
        args: ["--force-refresh-all", "--ignore-circuit-breaker"]
      };
    },
    fetchJobsFetchReportJson: async () => ({}),
    writeJobsAutoRefreshSignal() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getSourceStatusSetter: () => () => {},
    loadOpsHealthData: async () => {},
    jobsAutoRefreshSignalKey: "k",
    jobsFetcherCommand: "python -m src.jobs_fetcher --social-enabled",
    jobsFetcherTaskLabel: "Run jobs fetcher",
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {}
  });

  await controller.triggerJobsFetcherTask({ preset: "uncapped" });

  assert.ok(calls.includes('/tasks/run-fetcher:{"preset":"uncapped"}'));
});

test("admin fetcher log holds back a half-written line until its newline lands", () => {
  const rows = [];
  const fixture = createFetcherControllerFixture();
  // A real client-clock stamp is what made truncated rows read as out-of-order.
  fixture.options.createLogEvent = (scope, message, level) => ({ scope, message, level, timestamp: "CLIENT-CLOCK" });
  fixture.options.appendLogRow = (_container, event) => rows.push(event);
  const controller = createAdminFetcherController(fixture.options);

  controller.appendFetcherServerLogText(
    "[2026-03-08T10:01:01.000Z] Fetching studio careers page 41/612 pages"
  );
  assert.equal(rows.length, 0, "an unterminated line must not be rendered on its own");

  controller.appendFetcherServerLogText(
    ".\n[2026-03-08T10:01:02.000Z] Fetching studio careers page 42/612 pages.\n"
  );

  assert.deepEqual(rows.map(row => row.message), [
    "Fetching studio careers page 41/612 pages.",
    "Fetching studio careers page 42/612 pages."
  ]);
  assert.deepEqual(rows.map(row => row.timestamp), [
    "2026-03-08T10:01:01.000Z",
    "2026-03-08T10:01:02.000Z"
  ]);
  assert.ok(rows.every(row => row.timestamp !== "CLIENT-CLOCK"));
});
