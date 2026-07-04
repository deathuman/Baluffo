import test from "node:test";
import assert from "node:assert/strict";

import { createOpsHealthController } from "../../../frontend/admin/app/ops/health.js";
import {
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin ops health cache preserves actionable degraded active schedule fallback", async () => {
  const refs = {
    adminOpsScheduleEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement()
  };
  const state = { adminBusyState: {}, latestOpsHealthCache: null };
  const renderedSchedules = [];
  const controller = createOpsHealthController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health?view=summary") {
        return {
          ok: true,
          degraded: true,
          source: "container-gateway-fallback",
          scheduleDelayed: true,
          alerts: [],
          kpis: {},
          schedule: {
            pipeline: {
              enabled: true,
              intervalHours: 11,
              pending: false,
              due: false,
              nextRunAt: "",
              nextAfterCurrentCompletes: true,
              pipeline: { active: true, stage: "fetch" }
            }
          }
        };
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [] };
      if (path === "/tasks/jobs-pipeline-schedule") {
        return {
          ok: true,
          degraded: true,
          savedConfig: { enabled: true, intervalHours: 11 },
          status: {
            enabled: true,
            pending: false,
            due: false,
            nextRunAt: "",
            nextAfterCurrentCompletes: true,
            pipeline: { active: true, stage: "fetch" }
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      historyRuns: [],
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      liveTypes: new Set()
    }),
    getOpsPollIntervalMs: () => 1000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule(_el, schedule) {
      renderedSchedules.push(schedule);
    },
    renderAdminOpsDedupLists() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    idlePollIntervalMs: 1000,
    taskStateController: {
      getActiveTaskRows() {
        return [];
      },
      getTaskType() {
        return "";
      },
      syncLiveBusyFlags() {},
      maybeAttachLiveTaskRows() {},
      resolveTaskStatePayload() {
        return { tasks: [] };
      },
      resetLifecycleTaskState() {}
    },
    getBridgeStatus: () => "online",
    awaitBridgeReady: async () => true,
    renderScheduler: createDeferredRenderScheduler()
  });

  await controller.loadOpsHealthData({ summary: true, silent: true });

  assert.equal(
    state.latestOpsHealthCache.schedule.pipeline.nextAfterCurrentCompletes,
    true
  );
  assert.equal(renderedSchedules.at(-1).pipeline.nextAfterCurrentCompletes, true);
});
