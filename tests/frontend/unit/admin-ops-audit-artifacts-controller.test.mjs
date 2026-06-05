import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement,
  stubScheduledTimers,
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushAdminOpsBackground() {
  await Promise.resolve();
  await Promise.resolve();
}

test("admin ops controller lazy-loads discovery audit artifacts into metrics", async () => {
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
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
  const calls = [];
  const renderedMetrics = [];
  const auditPayload = {
    ok: true,
    artifacts: [{ name: "sheet-directory", exists: true, relativePath: "sheet-directory-discovery-audit.json" }]
  };
  const taskFailureAttemptsPayload = {
    ok: true,
    fetch: { hardFailureCount: 0, partialWarningCount: 1 },
    discovery: { failureRecordCount: 12, actionableDiagnosticCount: 4 }
  };
  const performanceProfilePayload = {
    ok: true,
    routeTimings: { routes: [{ label: "GET /ops/dashboard-health", count: 1, p95Ms: 10 }] },
    operationTimings: { operations: [{ label: "ops.dashboard.history", count: 1, p95Ms: 4 }] }
  };
  const renderScheduler = createDeferredRenderScheduler();
  const detailTimers = [];
  const timers = stubScheduledTimers({
    setTimeoutImpl(callback, ms) {
      if (ms === 1250) {
        detailTimers.push(callback);
      } else if (ms === 300) {
        callback();
      }
      return { unref() {} };
    }
  });
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [] };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return { latestRun: {} };
      if (path === "/ops/discovery-audit-artifacts") return auditPayload;
      if (path === "/ops/task-failure-attempts") return taskFailureAttemptsPayload;
      if (path === "/ops/performance-profile") return performanceProfilePayload;
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics(_el, metrics) {
      renderedMetrics.push(metrics);
    },
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
    renderScheduler: renderScheduler.schedule
  });

  try {
    await controller.loadOpsHealthData();
    assert.equal(calls.includes("/ops/discovery-audit-artifacts"), false);
    assert.equal(detailTimers.length, 1);
    detailTimers.forEach(callback => callback());
    for (let index = 0; index < 12; index += 1) {
      await Promise.resolve();
    }
    await flushAdminOpsBackground();
    renderScheduler.flush();
  } finally {
    controller.stopOpsHealthPolling();
    timers.restore();
  }

  assert.equal(refs.adminOpsAlertsEl.classList.contains("missing"), false);
  assert.equal(calls.includes("/ops/discovery-audit-artifacts"), true);
  assert.equal(calls.includes("/ops/task-failure-attempts"), true);
  assert.equal(calls.includes("/ops/performance-profile"), true);
  assert.equal(renderedMetrics.at(-1)?.discoveryAuditArtifacts, auditPayload);
  assert.equal(renderedMetrics.at(-1)?.taskFailureAttempts, taskFailureAttemptsPayload);
  assert.equal(renderedMetrics.at(-1)?.performanceProfile, performanceProfilePayload);
});
