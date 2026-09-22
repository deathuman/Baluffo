import test from "node:test";
import assert from "node:assert/strict";

import { createOpsHealthController } from "../../../frontend/admin/app/ops/health.js";
import {
  createElement,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

function createTaskStateController() {
  return {
    getActiveTaskRows: () => [],
    getTaskType: () => "",
    syncLiveBusyFlags() {},
    maybeAttachLiveTaskRows() {},
    resolveTaskStatePayload: () => ({ tasks: [] }),
    resetLifecycleTaskState() {}
  };
}

test("admin idle health poll refreshes through the real health controller wiring", async () => {
  const timers = stubScheduledTimers();
  const calls = [];
  const state = { adminBusyState: {}, latestOpsHealthCache: null };
  const refs = {
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsDedupListsEl: createElement(),
    adminOpsTrendsEl: createElement()
  };

  try {
    const controller = createOpsHealthController({
      state,
      refs,
      getBridge: async path => {
        calls.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/dashboard-health?view=summary") {
          return { ok: true, status: "healthy", alerts: [], kpis: {}, schedule: {}, summaryView: true };
        }
        if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
        if (path === "/tasks/jobs-pipeline-schedule") return { pipeline: {} };
        if (path === "/ops/history?limit=2") return { runs: [], count: 0, summaryView: true };
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({}),
      deriveAdminRunsModel: () => ({ historyRuns: [], currentRows: [], visibleCompletedRows: [], olderCompletedRows: [], liveTypes: new Set() }),
      getOpsPollIntervalMs: () => 1000,
      renderAdminOpsAlerts() {},
      renderAdminOpsKpis() {},
      renderAdminOpsSchedule() {},
      renderAdminOpsDedupLists() {},
      renderAdminOpsFetcherMetrics() {},
      renderAdminOpsTrends() {},
      renderAdminOpsHistory() {},
      setBusyFlag(key, value) { state.adminBusyState[key] = value; },
      showToast() {},
      getErrorMessage: err => String(err?.message || err || "unknown error"),
      adminDispatch: { dispatch() {} },
      adminActions: { OPS_REFRESHED: "ops/refreshed" },
      escapeHtml: value => String(value || ""),
      idlePollIntervalMs: 1000,
      taskStateController: createTaskStateController(),
      awaitBridgeReady: async () => true,
      renderOpsHealthSnapshot() {}
    });

    controller.scheduleOpsHealthPolling(1000);
    assert.equal(timers.scheduled.length, 1);
    timers.scheduled[0]();
    for (let index = 0; index < 8; index += 1) await Promise.resolve();
    await new Promise(resolve => setImmediate(resolve));

    assert.ok(calls.includes("/ops/dashboard-health?view=summary"));
    assert.equal(state.latestOpsHealthCache.status, "healthy");
  } finally {
    timers.restore();
  }
});
