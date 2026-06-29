import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin ops controller copies run diagnostics through renderer callback", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestFetcherReportCache: {},
    latestSourcePolicyRecommendationsPayload: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const refs = {
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  const copied = [];
  const toasts = [];
  const originalNavigator = globalThis.navigator;
  Object.defineProperty(globalThis, "navigator", {
    configurable: true,
    value: { clipboard: { writeText: async text => copied.push(text) } }
  });
  let copyPromise = Promise.resolve();
  let copyRunDiagnostics = null;
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state?view=summary") return { tasks: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      if (path === "/source-policy/recommendations") return { recommendations: { pairs: [] } };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      currentRows: [{ taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true, isLive: true }],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: true,
      liveTypes: ["fetch"]
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory(_el, _runModel, options) {
      copyRunDiagnostics = options.onCopyRunDiagnostics;
    },
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast(message, level) {
      toasts.push({ message, level });
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    loadDiscoveryData: async () => {},
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000,
    renderScheduler: renderScheduler.schedule
  });

  try {
    await controller.loadOpsHealthData();
    await Promise.resolve();
    await Promise.resolve();
    renderScheduler.flush();
    assert.equal(typeof copyRunDiagnostics, "function");
    copyPromise = copyRunDiagnostics({
      kind: "admin_run_diagnostics",
      title: "Fetcher",
      taskType: "fetch",
      runId: "fetch_live_1"
    });
    await copyPromise;
    controller.stopOpsHealthPolling();
  } finally {
    Object.defineProperty(globalThis, "navigator", { configurable: true, value: originalNavigator });
  }

  assert.equal(copied.length, 1);
  assert.match(copied[0], /admin_run_diagnostics/);
  assert.match(copied[0], /fetch_live_1/);
  assert.deepEqual(toasts, [{ message: "Fetcher run diagnostics copied.", level: "success" }]);
});
