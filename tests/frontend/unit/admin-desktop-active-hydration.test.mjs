import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { renderAdminOpsKpis, renderAdminOpsSchedule } from "../../../frontend/admin/render.js";
import {
  createClassList,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushBackground() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setTimeout(resolve, 0));
  await Promise.resolve();
}

function createState() {
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

function createRefs() {
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

function createController({ state, refs, getBridge }) {
  return createAdminOpsController({
    state,
    refs,
    getBridge,
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
    renderAdminOpsKpis(_el, kpis, status, options) {
      renderAdminOpsKpis(refs.adminOpsKpisEl, kpis, status, options);
    },
    renderAdminOpsSchedule(_el, schedule) {
      renderAdminOpsSchedule(refs.adminOpsScheduleEl, schedule);
    },
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
    activeHydrationPolicy: "desktop",
    renderScheduler: callback => {
      callback();
      return () => {};
    }
  });
}

test("desktop active pipeline renders compact KPI and schedule summaries", async () => {
  const state = createState();
  const refs = createRefs();
  const calls = [];
  const controller = createController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/tasks/run-jobs-pipeline-status") {
        return { active: true, runId: "pipeline_live_1", stage: "fetch" };
      }
      if (path === "/ops/task-state?view=summary") {
        return {
          tasks: [
            { taskType: "fetch", type: "fetch", runId: "fetch_live_1", parentTaskType: "pipeline", active: true },
            { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
          ],
          count: 2,
          summary: true
        };
      }
      if (path === "/ops/fetch-kpis?view=summary") {
        return {
          ok: true,
          status: "healthy",
          alerts: [],
          alertsEvaluated: true,
          summaryView: true,
          kpis: {
            lastSuccessfulFetchAge: "42m",
            sevenDayFetchSuccessRate: 0.875,
            avgFetchDurationMs7d: 7800000,
            failedSourceRatioLatest: 0.125,
            pendingApprovalsCount: 4
          }
        };
      }
      if (path === "/tasks/jobs-pipeline-schedule") {
        return {
          ok: true,
          savedConfig: { enabled: true, intervalHours: 12 },
          status: {
            enabled: true,
            intervalHours: 12,
            nextRunAt: "",
            nextAfterCurrentCompletes: true,
            scheduleStatusRefreshing: true
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
    }
  });

  await controller.loadOpsHealthData({ summary: true });
  await flushBackground();
  controller.stopOpsHealthPolling();

  assert.ok(calls.includes("/ops/fetch-kpis?view=summary"));
  assert.ok(calls.includes("/tasks/jobs-pipeline-schedule"));
  assert.equal(calls.includes("/ops/dashboard-health?view=summary"), false);
  assert.equal(calls.some(path => String(path).startsWith("/registry/")), false);
  assert.match(refs.adminOpsKpisEl.innerHTML, /87\.5%/);
  assert.match(refs.adminOpsKpisEl.innerHTML, /4/);
  assert.doesNotMatch(refs.adminOpsKpisEl.innerHTML, /Updating while job is running\./);
  assert.match(refs.adminOpsScheduleEl.innerHTML, /next after this pipeline finishes/);
  assert.doesNotMatch(refs.adminOpsScheduleEl.innerHTML, /loading schedule/);
  assert.doesNotMatch(refs.adminOpsScheduleEl.innerHTML, /data-ui="admin-pipeline-schedule-enabled"[^>]*disabled/);
});
