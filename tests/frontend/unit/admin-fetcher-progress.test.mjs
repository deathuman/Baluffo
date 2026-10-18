import test from "node:test";

import assert from "node:assert/strict";

import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";

import { createClassList, createElement } from "./helpers/admin-controller-test-helpers.mjs";

test("admin fetcher controller renders progress from the shared task progress contract", async () => {
  const logs = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLiveProgressState: null,
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
  const previousDateNow = Date.now;
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => Date.parse("2026-03-08T10:00:00.500Z");

  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
          return {
            text: "[2026-03-08T10:00:01.000Z] [jobs_fetcher] START source=Studio A\n[2026-03-08T10:00:02.000Z] [jobs_fetcher] WARN source=Studio B HTTP 403\n",
            nextOffset: 120
          };
        }
        if (path === "/ops/task-live/fetch?view=summary") {
          return {
            runId: "fetch_live_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 0.5,
              counts: {
                resolvedSources: 6,
                sourceCount: 12,
                outputCount: 18,
                failedSources: 1,
                excludedSources: 1
              }
            },
            summary: {
              outputCount: 18,
              failedSources: 1,
              sourceCount: 10
            },
            workItems: [
              {
                id: "studio_a",
                name: "Studio A",
                status: "running",
                progress: {
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  counts: { resolvedSources: 6, sourceCount: 12 },
                  updatedAt: "2026-03-08T10:00:01.000Z"
                }
              },
              {
                id: "scrapy_static_sources",
                name: "scrapy_static_sources",
                status: "running",
                progress: {
                  phaseKey: "loading_source",
                  phaseLabel: "Processing browser fallback queue",
                  counts: { completedSources: 19, totalSources: 26 },
                  updatedAt: "2026-03-08T10:00:01.000Z"
                }
              }
            ]
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => ({
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "determinate",
          ratio: 0.5,
          counts: {
            resolvedSources: 6,
            sourceCount: 12,
            outputCount: 18,
            failedSources: 1,
            excludedSources: 1
          }
        },
        runtime: {
          selectedSourceCount: 10,
          timingSummary: {
            stageTop: [{ stage: "detailFetch", durationMs: 47000 }]
          }
        },
        summary: {
          successfulSources: 4,
          failedSources: 1,
          excludedSources: 1,
          outputCount: 18,
          sourceCount: 10
        },
        sources: [
          { name: "Studio A", status: "ok", keptCount: 4, durationMs: 1200 },
          { name: "Studio B", status: "error", keptCount: 0, durationMs: 2200, error: "HTTP 403" },
          { name: "Studio C", status: "running", keptCount: 0, durationMs: 26000 }
        ]
      }),
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
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    controller.startFetcherCompletionWatch();
    await scheduled[0]();
    await scheduled[1]();

    assert.ok(logs.some(line => /fetcher started\. watching live progress/i.test(line)));
    assert.ok(logs.some(line => /start source=studio a/i.test(line)));
    assert.ok(logs.some(line => /warn source=studio b http 403/i.test(line)));
    assert.ok(logs.some(line => /6\/12 sources resolved/i.test(line)));
    assert.ok(logs.some(line => /Browser fallback 19\/26/i.test(line)));
    assert.equal(refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(refs.adminFetcherProgressEl.classList.contains("determinate"), true);
    assert.equal(refs.adminFetcherProgressEl.classList.contains("indeterminate"), false);
    assert.equal(refs.adminFetcherProgressBarEl.style.width, "50%");
    assert.equal(refs.adminFetcherProgressBarEl.style.left, "0");
    assert.equal(refs.adminFetcherProgressEl.attributes["aria-valuenow"], "50");
    assert.match(String(refs.adminFetcherProgressLabelEl.textContent || ""), /executing sources/i);
    assert.match(String(refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    assert.ok(!logs.some(line => /timeout window/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin fetcher controller keeps current live detail when an active report refresh lags behind", async () => {
  const logs = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: {
      runId: "fetch_live_current_1",
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
            runId: "fetch_live_current_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 10 / 551,
              counts: {
                resolvedSources: 10,
                sourceCount: 551,
                runningTasks: 541,
                queuedTasks: 0,
                outputCount: 34081,
                failedSources: 0,
                excludedSources: 0
              }
            },
            workItems: [
              {
                id: "studio_a",
                name: "Studio A",
                status: "running",
                progress: {
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  counts: { emittedJobs: 17 },
                  updatedAt: "2026-03-08T10:03:00.000Z"
                }
              }
            ]
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => ({
        runId: "fetch_live_current_1",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "determinate",
          ratio: 9 / 551,
          counts: {
            resolvedSources: 9,
            sourceCount: 551,
            runningTasks: 542,
            queuedTasks: 0,
            outputCount: 29957,
            failedSources: 0,
            excludedSources: 0
          }
        },
        summary: {
          successfulSources: 9,
          failedSources: 0,
          excludedSources: 0,
          outputCount: 29957,
          sourceCount: 551
        },
        sources: []
      }),
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
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow(_container, event) {
        logs.push(String(event.message || ""));
      }
    });

    controller.startFetcherCompletionWatch();
    await scheduled[scheduled.length - 1]();
    await scheduled[1]();
    await controller.loadLatestFetcherReport({ silent: true });

    assert.match(String(refs.adminFetcherProgressLabelEl.textContent || ""), /10\/551 sources resolved/i);
    assert.equal((state.latestFetcherReportCache || {}).summary?.outputCount, 29957);
    assert.ok(!logs.some(line => /No source entries found in report/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});
