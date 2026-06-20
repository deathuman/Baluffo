import test from "node:test";
import assert from "node:assert/strict";

import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createClassList,
  createElement,
  createFetcherControllerFixture
} from "./helpers/admin-controller-test-helpers.mjs";

function createOpsState() {
  return {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
}

function createOpsRefs() {
  return {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
}

function createOpsController(overrides = {}) {
  const state = overrides.state || createOpsState();
  const refs = overrides.refs || createOpsRefs();
  return createAdminOpsController({
    state,
    refs,
    getBridge: overrides.getBridge,
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 500,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    renderAdminRegistryConflicts() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000,
    ...overrides,
    state,
    refs
  });
}

async function flushBackground() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setTimeout(resolve, 0));
  await Promise.resolve();
}

test("admin ops abort acceptance renders aborting row and keeps polling compact", async () => {
  const state = createOpsState();
  const refs = createOpsRefs();
  const calls = [];
  const renderedCurrentRows = [];
  let abortHandler = null;
  const previousConfirm = globalThis.confirm;
  globalThis.confirm = () => true;
  const controller = createOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(String(path));
      if (path === "/tasks/run-jobs-pipeline-status") {
        return {
          active: true,
          runId: "pipeline_abort_1",
          stage: "aborting",
          progress: { phaseKey: "aborting", phaseLabel: "Aborting..." },
          activeChildren: [{
            taskType: "fetch",
            type: "fetch",
            runId: "fetch_abort_1",
            active: true,
            taskProgress: { active: true, phaseLabel: "Fetching job listings" }
          }]
        };
      }
      if (path === "/ops/task-state?view=summary") {
        return {
          tasks: [
            {
              taskType: "pipeline",
              type: "pipeline",
              runId: "pipeline_abort_1",
              active: true,
              stage: "aborting",
              taskProgress: { active: true, phaseKey: "aborting", phaseLabel: "Aborting..." },
              summary: { abortRequestedAt: "2026-06-06T09:02:00.000Z" }
            },
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_abort_1",
              parentTaskType: "pipeline",
              parentRunId: "pipeline_abort_1",
              active: true,
              taskProgress: { active: true, phaseLabel: "Fetching job listings" }
            }
          ],
          count: 2,
          summary: true
        };
      }
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, summaryView: true, kpis: {} };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async (path, payload) => {
      calls.push(String(path));
      assert.deepEqual(payload, {
        taskType: "pipeline",
        runId: "pipeline_abort_1",
        reason: "admin_ops_abort"
      });
      return { abortAccepted: true, gatewayAccepted: true };
    },
    renderAdminOpsHistory(_el, runModel, options) {
      renderedCurrentRows.push(runModel.currentRows);
      abortHandler = options.onAbortRun;
    }
  });

  try {
    controller.applyBootstrapPayload({
      tasks: {
        current: [{
          taskType: "pipeline",
          type: "pipeline",
          runId: "pipeline_abort_1",
          active: true,
          startedAt: "2026-06-06T09:00:00.000Z"
        }],
        recent: []
      },
      registrySummary: {},
      schedule: {},
      app: { version: "0.2.66" }
    });
    await abortHandler({ taskType: "pipeline", runId: "pipeline_abort_1" });
    await flushBackground();
    await abortHandler({ taskType: "pipeline", runId: "pipeline_abort_1" });

    assert.equal(calls.filter(path => path === "/tasks/abort").length, 1);
    assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
    assert.ok(calls.includes("/ops/task-state?view=summary"));
    assert.ok(calls.includes("/ops/fetch-kpis?view=summary"));
    assert.equal(calls.includes("/admin/ops-tab-counts?view=summary"), false);
    assert.equal(calls.includes("/ops/dashboard-health?view=summary"), false);
    assert.equal(state.latestOpsTaskStatePayload.tasks[0].stage, "aborting");
    assert.match(JSON.stringify(renderedCurrentRows.at(-1) || []), /aborting|Aborting/);
  } finally {
    controller.stopOpsHealthPolling();
    globalThis.confirm = previousConfirm;
  }
});

test("admin ops abort clears pending abort when compact status proves idle", async () => {
  const state = createOpsState();
  const refs = createOpsRefs();
  const calls = [];
  const scheduled = [];
  const previousConfirm = globalThis.confirm;
  const previousSetTimeout = globalThis.setTimeout;
  const previousClearTimeout = globalThis.clearTimeout;
  globalThis.confirm = () => true;
  globalThis.setTimeout = (callback, delay = 0) => {
    scheduled.push({ callback, delay: Number(delay) || 0 });
    return scheduled.length;
  };
  globalThis.clearTimeout = () => {};
  let abortHandler = null;
  const controller = createOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(String(path));
      if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, summaryView: true, kpis: {} };
      if (path === "/ops/dashboard-health?view=summary") return { alerts: [], kpis: {}, schedule: {}, status: "healthy", summaryView: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      if (path === "/admin/ops-tab-counts?view=summary") return { ok: true, summaryView: true, badges: {} };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => {
      calls.push("/tasks/abort");
      return { abortAccepted: true, gatewayAccepted: true };
    },
    renderAdminOpsHistory(_el, _runModel, options) {
      abortHandler = options.onAbortRun;
    }
  });

  try {
    controller.applyBootstrapPayload({
      tasks: {
        current: [{
          taskType: "pipeline",
          type: "pipeline",
          runId: "pipeline_abort_1",
          active: true,
          startedAt: "2026-06-06T09:00:00.000Z"
        }],
        recent: []
      },
      registrySummary: {},
      schedule: {},
      app: { version: "0.2.81" }
    });

    await abortHandler({ taskType: "pipeline", runId: "pipeline_abort_1" });
    await Promise.resolve();
    await Promise.resolve();

    assert.equal(Object.keys(state.adminOpsAbortRequests || {}).length, 0);
    assert.equal(state.opsActiveAdminWorkLastActive, false);
    assert.equal(state.opsActivePipelineOrFetchLastActive, false);
    assert.equal(
      calls.filter(path => path === "/tasks/run-jobs-pipeline-status").length <= 2,
      true
    );
    assert.equal(
      scheduled.filter(item => item.delay === 0).length <= 1,
      true
    );
  } finally {
    controller.stopOpsHealthPolling();
    globalThis.confirm = previousConfirm;
    globalThis.setTimeout = previousSetTimeout;
    globalThis.clearTimeout = previousClearTimeout;
  }
});

test("admin fetcher controller backs off task-live timeouts without clearing progress", async () => {
  const scheduled = [];
  const previousSetTimeout = global.setTimeout;
  const previousClearTimeout = global.clearTimeout;
  global.setTimeout = (callback, delay = 0) => {
    scheduled.push({ callback, delay: Number(delay) || 0 });
    return scheduled.length;
  };
  global.clearTimeout = () => {};

  let controller;
  try {
    const fixture = createFetcherControllerFixture();
    let taskLiveCalls = 0;
    let reportCalls = 0;
    fixture.options.getBridge = async path => {
      if (String(path).startsWith("/fetcher/log?offset=") || String(path).startsWith("/fetcher/log?view=tail")) {
        return { text: "", nextOffset: 0 };
      }
      if (path === "/ops/task-live/fetch?view=summary") {
        taskLiveCalls += 1;
        throw new Error("Bridge error (HTTP 504)");
      }
      return {};
    };
    fixture.options.fetchJobsFetchReportJson = async () => {
      reportCalls += 1;
      return {};
    };
    controller = createAdminFetcherController(fixture.options);

    controller.startFetcherCompletionWatch();
    fixture.refs.adminFetcherProgressLabelEl.textContent = "10/551 sources resolved";
    const firstCompletion = scheduled.find(item => item.delay === 0);
    await firstCompletion.callback();
    const secondCompletion = scheduled.at(-1);
    await secondCompletion.callback();

    assert.equal(taskLiveCalls, 2);
    assert.equal(reportCalls, 0);
    assert.equal(String(fixture.refs.adminFetcherProgressLabelEl.textContent || ""), "10/551 sources resolved");
    assert.equal(secondCompletion.delay, 500);
    assert.equal(scheduled.at(-1).delay, 1000);
  } finally {
    controller?.stopFetcherCompletionWatch?.();
    global.setTimeout = previousSetTimeout;
    global.clearTimeout = previousClearTimeout;
  }
});
