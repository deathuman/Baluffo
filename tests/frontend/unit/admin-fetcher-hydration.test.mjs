import test from "node:test";

import assert from "node:assert/strict";

import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";

import { createClassList, createElement, createFetcherControllerFixture, stubScheduledTimers } from "./helpers/admin-controller-test-helpers.mjs";

test("admin fetcher controller hydrates progress from the report without replaying summary noise when live payload is empty", async () => {
  const timerStub = stubScheduledTimers();

  let controller;
  try {
    const fixture = createFetcherControllerFixture({
      state: {
        fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z")
      }
    });
    fixture.options.getBridge = async path => {
        if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch?view=summary") {
          return {};
        }
        return {};
    };
    fixture.options.postBridge = async () => ({});
    fixture.options.fetchJobsFetchReportJson = async () => ({
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: true,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "determinate",
        ratio: 0.5,
        counts: { resolvedSources: 6, sourceCount: 12 }
      },
      summary: { outputCount: 18, failedSources: 1, sourceCount: 12 },
      sources: [{ name: "Studio A", status: "running" }]
    });
    fixture.options.loadOpsHealthData = async () => {};
    controller = createAdminFetcherController(fixture.options);

    controller.startFetcherCompletionWatch();
    await timerStub.scheduled[0]();
    await timerStub.scheduled[1]();
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.match(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), /6\/12 sources resolved/i);
    assert.ok(!fixture.logs.some(line => /6\/12 sources resolved/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller only emits generic active heartbeat after sustained idle time", async () => {
  const logs = [];
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  const previousDateNow = Date.now;
  let nowMs = Date.parse("2026-03-08T10:00:00.000Z");
  global.setTimeout = callback => {
    scheduled.push(callback);
    return scheduled.length;
  };
  global.clearTimeout = () => {};
  Date.now = () => nowMs;

  const report = {
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
      selectedSourceCount: 12
    },
    summary: {
      successfulSources: 4,
      failedSources: 1,
      excludedSources: 1,
      outputCount: 18,
      sourceCount: 12
    },
    sources: [
      { name: "Studio A", status: "ok", keptCount: 4, durationMs: 1200 }
    ]
  };

  let controller;
  try {
    const state = {
      latestFetcherReportCache: null,
      fetcherLaunchAtMs: nowMs,
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
      adminFetcherProgressEl: createElement({ style: {}, classList: createClassList(["hidden"]) }),
      adminFetcherProgressBarEl: createElement({ style: {} }),
      adminFetcherProgressLabelEl: createElement(),
      adminRunFetcherBtnEl: createElement(),
      adminRunFetcherIncrementalBtnEl: createElement(),
      adminRunFetcherUncappedBtnEl: createElement(),
      adminRunFetcherForceBtnEl: createElement(),
      adminRetryFailedBtnEl: createElement()
    };

    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch?view=summary") {
          return {
            taskType: "fetch",
            active: true,
            runId: "fetch_heartbeat_1",
            startedAt: "2026-03-08T10:00:00.000Z",
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
              excludedSources: 1,
              sourceCount: 12
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
      fetchJobsFetchReportJson: async () => report,
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
    assert.equal(logs.filter(line => /Fetcher active:/i.test(line)).length, 0);

    nowMs += 30_000;
    await runLatestScheduled();
    assert.equal(logs.filter(line => /Fetcher active:/i.test(line)).length, 0);

    nowMs += 31_000;
    await runLatestScheduled();
    assert.equal(logs.filter(line => /Fetcher active:/i.test(line)).length, 1);
    assert.ok(logs.some(line => /Fetcher: .*6\/12 sources resolved/i.test(line)));
    assert.ok(logs.some(line => /Fetcher active: .*Browser fallback 19\/26/i.test(line)));
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
    Date.now = previousDateNow;
  }
});
