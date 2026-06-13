import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createClassList,
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushAdminOpsBackground() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setTimeout(resolve, 0));
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
  const renderedCurrentRows = [];
  const renderedKpis = [];
  const watcherCalls = [];
  const calls = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/tasks/run-jobs-pipeline-status") {
        return {
          active: true,
          runId: "pipeline_live_1",
          startedAt: "2026-06-06T09:00:00.000Z",
          stage: "fetch",
          progress: { label: "Fetching job listings" },
          activeChildren: [
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_live_1",
              active: true,
              startedAt: "2026-06-06T09:00:01.000Z",
              status: "running",
              controlPlaneSource: "pipeline-status",
              displayOnly: true,
              taskProgress: { active: true, phaseLabel: "Fetch running" },
              summary: { controlPlane: true }
            }
          ]
        };
      }
      if (path === "/ops/dashboard-health?view=summary") throw new Error("dashboard should not load while pipeline is active");
      if (path === "/ops/task-state?view=summary") {
        return {
          tasks: [
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_live_1",
              parentTaskType: "pipeline",
              parentRunId: "pipeline_live_1",
              active: true,
              startedAt: "2026-06-06T09:00:01.000Z",
              status: "running",
              taskProgress: {
                active: true,
                phaseLabel: "Executing sources",
                mode: "determinate",
                ratio: 0.12,
                counts: { completedSources: 12, sourceCount: 100 }
              }
            },
            {
              taskType: "pipeline",
              type: "pipeline",
              runId: "pipeline_live_1",
              active: true,
              startedAt: "2026-06-06T09:00:00.000Z"
            }
          ],
          count: 2,
          summary: true
        };
      }
      if (path === "/registry/conflicts?view=summary") {
        return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      }
      if (path === "/ops/fetch-kpis?view=summary") {
        return {
          ok: true,
          kpis: {
            sevenDayFetchSuccessRate: 0,
            avgFetchDurationMs7d: 5220280,
            pendingApprovalsCount: 813
          },
          summaryView: true
        };
      }
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
    renderAdminOpsKpis(_el, _kpis, _status, options = {}) {
      renderedKpis.push(options);
    },
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
    attachToActiveFetchRun() {
      watcherCalls.push("attach-fetch");
    },
    loadLatestFetcherReport() {
      watcherCalls.push("load-fetch-report");
      return Promise.resolve(null);
    },
    attachToActiveDiscoveryRun() {
      watcherCalls.push("attach-discovery");
    },
    loadLatestDiscoveryReport() {
      watcherCalls.push("load-discovery-report");
      return Promise.resolve(null);
    },
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000,
    renderScheduler: renderScheduler.schedule
  });

  const loadPromise = controller.loadOpsHealthData({ summary: true });
  await flushAdminOpsBackground();
  renderScheduler.flush();

  assert.deepEqual(
    renderedCurrentRows.at(-1)?.map(row => row.runId),
    ["fetch_live_1", "pipeline_live_1"]
  );
  assert.equal(renderedCurrentRows.at(-1)?.[0]?.displayOnly, undefined);
  assert.equal(renderedCurrentRows.at(-1)?.[0]?.taskProgress?.ratio, 0.12);
  assert.equal(state.adminBusyState.livePipelineRunning, true);
  assert.equal(state.adminBusyState.liveFetchRunning, true);
  assert.ok(calls.includes("/ops/task-state?view=summary"));
  assert.ok(calls.includes("/ops/fetch-kpis?view=summary"));
  assert.equal(calls.includes("/admin/ops-tab-counts?view=summary"), false);
  assert.equal(renderedKpis.at(-1)?.fetchKpiPendingLabel, "Not available");
  assert.deepEqual(watcherCalls, ["attach-fetch", "load-fetch-report"]);

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
  let pipelineActive = false;
  const renderedCurrentRows = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") {
        return pipelineActive
          ? {
              active: true,
              runId: "pipeline_live_1",
              startedAt: "2026-06-06T09:00:00.000Z",
              stage: "fetch",
              activeChildren: [
                {
                  taskType: "fetch",
                  type: "fetch",
                  runId: "fetch_live_1",
                  active: true,
                  startedAt: "2026-06-06T09:00:01.000Z"
                }
              ]
            }
          : { active: false, stage: "idle" };
      }
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
              parentTaskType: "pipeline",
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

  pipelineActive = true;
  await controller.loadPipelineStatusFallbackData();
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
