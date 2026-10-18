import test from "node:test";

import assert from "node:assert/strict";

import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";

import { createClassList, createElement } from "./helpers/admin-controller-test-helpers.mjs";

test("admin fetcher controller treats scrapy fallback progress changes as summary activity", async () => {
  const logs = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherCompletionPollAttempts: 0,
    fetcherLogPollTimer: null,
    fetcherLogRemoteOffset: 0,
    fetcherLiveProgressState: null,
    fetchOptimisticRun: null,
    jobsAutoRefreshSignalKey: "k",
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
  const livePayloads = [
    {
      taskType: "fetch",
      active: true,
      runId: "fetch_tail_1",
      startedAt: "2026-03-08T10:00:00.000Z",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: {
          resolvedSources: 550,
          sourceCount: 551,
          outputCount: 40_279,
          failedSources: 69,
          excludedSources: 0
        }
      },
      workItems: [
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
    },
    {
      taskType: "fetch",
      active: true,
      runId: "fetch_tail_1",
      startedAt: "2026-03-08T10:00:00.000Z",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: {
          resolvedSources: 550,
          sourceCount: 551,
          outputCount: 40_279,
          failedSources: 69,
          excludedSources: 0
        }
      },
      workItems: [
        {
          id: "scrapy_static_sources",
          name: "scrapy_static_sources",
          status: "running",
          progress: {
            phaseKey: "loading_source",
            phaseLabel: "Processing browser fallback queue",
            counts: { completedSources: 24, totalSources: 26 },
            updatedAt: "2026-03-08T10:00:05.000Z"
          }
        }
      ]
    }
  ];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousDateNow = Date.now;
  const scheduled = [];
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => Date.parse("2026-03-08T10:00:00.500Z");

  let livePayloadIndex = 0;
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
          return livePayloads[Math.min(livePayloadIndex++, livePayloads.length - 1)];
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async () => ({
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "determinate",
          ratio: 0.5,
          counts: {
            resolvedSources: 550,
            sourceCount: 551,
            outputCount: 40_279,
            failedSources: 69,
            excludedSources: 0
          }
        },
        summary: { outputCount: 40_279, failedSources: 69, sourceCount: 551 }
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
    const runLatestScheduled = async () => {
      const callback = scheduled[scheduled.length - 1];
      await callback();
    };
    await runLatestScheduled();
    await runLatestScheduled();

    assert.ok(logs.some(line => /Browser fallback 19\/26/i.test(line)));
    assert.ok(logs.some(line => /Browser fallback 24\/26/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});

test("admin fetcher controller prefers task-live payload during active runs and keeps manual report loads full-fidelity", async () => {
  const fetchReportCalls = [];
  const state = {
    latestFetcherReportCache: null,
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherCompletionPollTimer: null,
    fetcherLogPollTimer: null,
    fetcherLogRemoteOffset: 0,
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
            runId: "fetch_live_1",
            startedAt: "2026-03-08T10:00:00.000Z",
            finishedAt: "",
            taskProgress: {
              active: true,
              phaseKey: "executing_sources",
              phaseLabel: "Executing sources",
              mode: "determinate",
              ratio: 0.5,
              counts: { resolvedSources: 4, sourceCount: 8, outputCount: 12 }
            },
            workItems: [{ id: "studio_a", name: "Studio A", status: "running" }]
          };
        }
        return {};
      },
      postBridge: async () => ({}),
      fetchJobsFetchReportJson: async options => {
        fetchReportCalls.push(options || {});
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
            counts: { resolvedSources: 4, sourceCount: 8, outputCount: 12 }
          },
          summary: { outputCount: 12, failedSources: 0, sourceCount: 8 },
          sources: [{ name: "Studio A", status: "running", details: [{ url: "https://example.com/job/1" }] }]
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
      createLogEvent(scope, message, level) {
        return { scope, message, level, timestamp: "2026-03-08T10:00:00.000Z" };
      },
      appendLogRow() {}
    });

    controller.startFetcherCompletionWatch();
    for (const callback of [...scheduled]) {
      await callback();
    }
    await controller.loadLatestFetcherReport({ silent: true });

    assert.deepEqual(fetchReportCalls, [{}]);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});
