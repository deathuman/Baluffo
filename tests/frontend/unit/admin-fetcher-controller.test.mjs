import test from "node:test";
import assert from "node:assert/strict";
import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import {
  createClassList,
  createElement,
  createFetcherControllerFixture,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin fetcher controller stores optimistic run metadata while fetch watch is active", async () => {
  const timerStub = stubScheduledTimers();

  let controller;
  try {
    const fixture = createFetcherControllerFixture({
      refs: {
        adminFetcherLogEl: createElement()
      }
    });
    fixture.options.getBridge = async path => {
      fixture.calls.push(path);
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        return {};
    };
    fixture.options.postBridge = async path => {
      fixture.calls.push(path);
      return {
        started: true,
        runId: "fetch_123",
        startedAt: "2026-03-08T10:01:00.000Z",
        preset: "default",
        args: ["--quiet"]
      };
    };
    fixture.options.loadOpsHealthData = async () => {
      fixture.calls.push("loadOpsHealthData");
    };
    controller = createAdminFetcherController(fixture.options);

    await controller.triggerJobsFetcherTask({ preset: "default" });

    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_123",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.ok(fixture.calls.includes("/tasks/run-fetcher"));
    assert.ok(fixture.logs.some(line => /triggered fetcher via local admin bridge/i.test(line)));
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller attaches to an already-running bridge task on conflict", async () => {
  const timerStub = stubScheduledTimers();
  let controller;

  try {
    const fixture = createFetcherControllerFixture();
    fixture.options.postBridge = async path => {
      fixture.calls.push(path);
        return {
          status: 409,
          data: {
            started: false,
            alreadyRunning: true,
            runId: "fetch_live_1",
            startedAt: "2026-03-08T10:01:00.000Z",
            task: "jobs_fetcher",
            taskType: "fetch",
            pid: 654,
            status: "running"
          }
        };
    };
    controller = createAdminFetcherController(fixture.options);

    await controller.triggerJobsFetcherTask({ preset: "default" });

    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_live_1",
      startedAt: "2026-03-08T10:01:00.000Z"
    });
    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.ok(fixture.calls.includes("/tasks/run-fetcher"));
    assert.ok(fixture.calls.includes("loadOpsHealthData"));
    assert.ok(fixture.logs.some(line => /fetcher already running; attached/i.test(line)));
    assert.ok(fixture.toasts.some(item => item.message === "Fetcher already running. Attached to active run." && item.level === "info"));
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller starts live progress watching for an explicit bridge-launched fetch", async () => {
  const timerStub = stubScheduledTimers();
  const activeReport = {
    startedAt: "2026-03-08T10:00:00.000Z",
    finishedAt: "",
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "indeterminate",
      ratio: 0,
      counts: {}
    },
    runtime: {},
    summary: {},
    sources: []
  };
  const fixture = createFetcherControllerFixture({
    options: {
      getBridge: async path => {
      if (String(path).startsWith("/fetcher/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/ops/dashboard-health") return {};
      return activeReport;
    },
      fetchJobsFetchReportJson: async () => activeReport
    }
  });
  const controller = createAdminFetcherController(fixture.options);

  try {
    controller.attachToActiveFetchRun({
      runId: "fetch_123",
      startedAt: "2026-03-08T10:00:00.000Z"
    });

    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.deepEqual(fixture.state.fetchOptimisticRun, {
      runId: "fetch_123",
      startedAt: "2026-03-08T10:00:00.000Z"
    });
    assert.ok(fixture.logs.some(line => /fetcher started\. watching live progress/i.test(line)));
    assert.ok(!fixture.logs.some(line => /timeout window/i.test(line)));
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("indeterminate"), true);
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

test("admin fetcher controller can restore a live watch from local state when the latest report is stale", async () => {
  const fixture = createFetcherControllerFixture({
    state: {
    fetcherLaunchAtMs: Date.parse("2026-03-08T10:00:00.000Z"),
    fetcherLiveProgressState: {
      summarySignature: "",
      sourceSignatures: new Map(),
      reportedSlowSources: new Set(),
      serverLogSignatures: new Set(),
      slowSourceSummarySignature: "",
      slowStageSummarySignature: "",
      lastHeartbeatAtMs: 0,
      lastActivityAtMs: Date.now()
    },
    fetchOptimisticRun: {
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z"
    },
    adminBusyState: {
      fetcherRun: false,
      fetcherWatch: true,
      fetcherReportLoad: false,
      liveFetchRunning: false
    }
    },
    options: {
      getBridge: async path => {
      if (String(path).startsWith("/fetcher/log?offset=")) {
        return { text: "", nextOffset: 0 };
      }
      return {
        runId: "fetch_stale_2",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "",
        taskProgress: {
          active: false,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "indeterminate",
          ratio: 0,
          counts: {}
        },
        runtime: {},
        summary: {},
        sources: []
      };
    },
      fetchJobsFetchReportJson: async () => ({
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: {
        active: false,
        phaseKey: "executing_sources",
        phaseLabel: "Executing sources",
        mode: "indeterminate",
        ratio: 0,
        counts: {}
      },
      runtime: {},
      summary: {},
      sources: []
    })
    }
  });
  const timerStub = stubScheduledTimers();
  const controller = createAdminFetcherController(fixture.options);

  try {
    const meta = controller.getRestorableFetcherRunMeta({
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z",
      finishedAt: "",
      taskProgress: { active: false }
    });

    assert.deepEqual(meta, {
      runId: "fetch_stale_2",
      startedAt: "2026-03-08T10:00:00.000Z"
    });

    controller.restartFetcherCompletionWatch(meta);

    assert.equal(fixture.state.adminBusyState.fetcherWatch, true);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("hidden"), false);
    assert.equal(fixture.refs.adminFetcherProgressEl.classList.contains("indeterminate"), true);
    assert.ok(timerStub.scheduled.length >= 2);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    timerStub.restore();
  }
});

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
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return {
            text: "[2026-03-08T10:00:01.000Z] [jobs_fetcher] START source=Studio A\n[2026-03-08T10:00:02.000Z] [jobs_fetcher] WARN source=Studio B HTTP 403\n",
            nextOffset: 120
          };
        }
        if (path === "/ops/task-live/fetch") {
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
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
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
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
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
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
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
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
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
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
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
  let controller;
  try {
    controller = createAdminFetcherController({
      state,
      refs,
      getBridge: async path => {
        if (String(path).startsWith("/fetcher/log?offset=")) {
          return { text: "", nextOffset: 0 };
        }
        if (path === "/ops/task-live/fetch") {
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

    assert.equal(manualReportLoads, 1);
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
      if (String(path).startsWith("/fetcher/log?offset=")) {
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
