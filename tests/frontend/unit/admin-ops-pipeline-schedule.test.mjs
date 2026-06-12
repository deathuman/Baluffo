import test from "node:test";
import assert from "node:assert/strict";
import { createOpsHealthController } from "../../../frontend/admin/app/ops/health.js";
import {
  createDeferredRenderScheduler,
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushAdminOpsBackground() {
  await Promise.resolve();
  await Promise.resolve();
}

test("admin ops pipeline schedule controls post normalized settings", async () => {
  const listeners = {};
  const enabledEl = { checked: true };
  const intervalEl = { value: "12" };
  const saveButton = { disabled: false };
  const refs = {
    adminOpsScheduleEl: {
      addEventListener(type, handler) {
        listeners[type] = handler;
      },
      querySelector(selector) {
        if (selector === '[data-ui="admin-pipeline-schedule-enabled"]') return enabledEl;
        if (selector === '[data-ui="admin-pipeline-schedule-interval"]') return intervalEl;
        return null;
      }
    },
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement()
  };
  const state = { adminBusyState: {} };
  const posts = [];
  const toasts = [];
  createOpsHealthController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
      if (path === "/ops/dashboard-health") {
        return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [] };
      if (path === "/registry/conflicts?view=summary") {
        return { summary: { conflictCount: 0 }, summaryView: true };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async (path, payload) => {
      posts.push({ path, payload });
      return {
        ok: true,
        savedConfig: { enabled: true, intervalHours: 12 },
        status: { enabled: true }
      };
    },
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
    renderAdminOpsSchedule() {},
    renderAdminOpsDedupLists() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast(message, tone) {
      toasts.push({ message, tone });
    },
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

  listeners.click({
    target: {
      closest(selector) {
        return selector === '[data-action="save-pipeline-schedule"]' ? saveButton : null;
      }
    },
    preventDefault() {}
  });
  await flushAdminOpsBackground();
  await flushAdminOpsBackground();
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.deepEqual(posts, [
    {
      path: "/tasks/jobs-pipeline-schedule",
      payload: { enabled: true, intervalHours: 12 }
    }
  ]);
  assert.equal(toasts.at(-1).tone, "success");
  assert.equal(saveButton.disabled, false);

  intervalEl.value = "169";
  listeners.click({
    target: {
      closest(selector) {
        return selector === '[data-action="save-pipeline-schedule"]' ? saveButton : null;
      }
    },
    preventDefault() {}
  });
  await flushAdminOpsBackground();

  assert.equal(posts.length, 1);
  assert.equal(toasts.at(-1).tone, "error");
  assert.match(toasts.at(-1).message, /between 1 and 168/i);
});
