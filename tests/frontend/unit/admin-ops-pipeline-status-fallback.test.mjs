import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createClassList,
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function flushAdminOpsBackground() {
  await Promise.resolve();
  await Promise.resolve();
}

test("admin ops controller renders active pipeline from status when dashboard health is delayed", async () => {
  const state = {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
  const dashboardHealth = createDeferred();
  const renderedCurrentRows = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") {
        return {
          active: true,
          runId: "pipeline_live_1",
          startedAt: "2026-06-06T09:00:00.000Z",
          stage: "fetch",
          progress: { label: "Fetching job listings" }
        };
      }
      if (path === "/ops/dashboard-health?view=summary") return dashboardHealth.promise;
      if (path === "/ops/task-state?view=summary") throw new Error("task-state delayed");
      if (path === "/registry/conflicts?view=summary") {
        return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      }
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: {}, summaryView: true };
      if (path === "/admin/ops-tab-counts?view=summary") return { ok: true, summaryView: true, badges: {} };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory(_el, runModel) {
      renderedCurrentRows.push(runModel.currentRows);
    },
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
    renderScheduler: renderScheduler.schedule
  });

  const loadPromise = controller.loadOpsHealthData({ summary: true });
  await flushAdminOpsBackground();
  renderScheduler.flush();

  assert.equal(renderedCurrentRows.at(-1)?.[0]?.runId, "pipeline_live_1");
  assert.equal(state.adminBusyState.livePipelineRunning, true);

  dashboardHealth.reject(new Error("dashboard delayed"));
  await loadPromise;
  controller.stopOpsHealthPolling();
});

test("admin ops controller does not let delayed pipeline status replace richer task-state rows", async () => {
  const state = {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
  const pipelineStatus = createDeferred();
  const renderedCurrentRows = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") return pipelineStatus.promise;
      if (path === "/ops/dashboard-health?view=summary") {
        return { alerts: [], kpis: {}, schedule: {}, status: "healthy", summaryView: true };
      }
      if (path === "/ops/task-state?view=summary") {
        return {
          tasks: [
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_live_1",
              active: true,
              startedAt: "2026-06-06T09:00:01.000Z"
            }
          ],
          count: 1,
          summary: true
        };
      }
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: {}, summaryView: true };
      if (path === "/admin/ops-tab-counts?view=summary") return { ok: true, summaryView: true, badges: {} };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory(_el, runModel) {
      renderedCurrentRows.push(runModel.currentRows);
    },
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
    renderScheduler: renderScheduler.schedule
  });

  await controller.loadOpsHealthData({ summary: true });
  await flushAdminOpsBackground();
  renderScheduler.flush();
  assert.equal(renderedCurrentRows.at(-1)?.[0]?.taskType, "fetch");

  pipelineStatus.resolve({
    active: true,
    runId: "pipeline_live_1",
    startedAt: "2026-06-06T09:00:00.000Z",
    stage: "fetch"
  });
  await flushAdminOpsBackground();
  renderScheduler.flush();

  assert.equal(renderedCurrentRows.at(-1)?.[0]?.taskType, "fetch");
  assert.equal(state.latestOpsTaskStatePayload.tasks[0].taskType, "fetch");
  controller.stopOpsHealthPolling();
});

test("admin ops controller keeps pipeline fallback when bootstrap has no current rows", async () => {
  const state = {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
  const renderedCurrentRows = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") {
        return {
          active: true,
          runId: "pipeline_live_1",
          startedAt: "2026-06-06T09:00:00.000Z",
          stage: "fetch",
          progress: { label: "Fetching job listings" }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory(_el, runModel) {
      renderedCurrentRows.push(runModel.currentRows);
    },
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
    idlePollIntervalMs: 1000
  });

  await controller.loadPipelineStatusFallbackData();
  assert.equal(renderedCurrentRows.at(-1)?.[0]?.runId, "pipeline_live_1");

  controller.applyBootstrapPayload({
    tasks: { current: [], recent: [] },
    registrySummary: {},
    schedule: {},
    app: { version: "0.2.61" }
  });

  assert.equal(renderedCurrentRows.at(-1)?.[0]?.runId, "pipeline_live_1");
  assert.equal(state.latestOpsTaskStatePayload.source, "pipeline-status");
  controller.stopOpsHealthPolling();
});

test("admin ops controller applies active pipeline status even after bootstrap advances render token", async () => {
  const state = {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
  const pipelineStatus = createDeferred();
  const renderedCurrentRows = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") return pipelineStatus.promise;
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: Boolean((taskState?.tasks || []).length),
      liveTypes: (taskState?.tasks || []).map(row => String(row?.taskType || row?.type || "").toLowerCase())
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory(_el, runModel) {
      renderedCurrentRows.push(runModel.currentRows);
    },
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
    idlePollIntervalMs: 1000
  });

  const fallbackPromise = controller.loadPipelineStatusFallbackData();
  controller.applyBootstrapPayload({
    tasks: { current: [], recent: [] },
    registrySummary: {},
    schedule: {},
    app: { version: "0.2.61" }
  });
  pipelineStatus.resolve({
    active: true,
    runId: "pipeline_live_2",
    stage: "fetch",
    progress: { label: "Fetching job listings" }
  });
  await fallbackPromise;

  assert.equal(renderedCurrentRows.at(-1)?.[0]?.runId, "pipeline_live_2");
  assert.equal(state.adminBusyState.livePipelineRunning, true);
  controller.stopOpsHealthPolling();
});
