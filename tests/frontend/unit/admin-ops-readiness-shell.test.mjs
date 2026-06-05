import assert from "node:assert/strict";
import test from "node:test";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function flush() {
  await Promise.resolve();
  await Promise.resolve();
}

function createFixture({ getBridge, state: stateOverrides = {} } = {}) {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: { runs: [] },
    latestOpsTaskStatePayload: { tasks: [] },
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    },
    ...stateOverrides
  };
  const refs = {
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement({ textContent: "No run trend data yet." }),
    adminSourcePolicyReviewEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
  const calls = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (getBridge) return getBridge(path);
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
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
    renderAdminOpsAlerts(el, alerts) {
      if (el) el.innerHTML = `alerts:${alerts.length}`;
    },
    renderAdminOpsKpis(el, _kpis, status) {
      if (el) el.innerHTML = `kpis:${status}`;
    },
    renderAdminOpsSchedule(el) {
      if (el) el.innerHTML = "schedule";
    },
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends(el, runs) {
      if (el) el.textContent = `trends:${runs.length}`;
    },
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
    renderScheduler: callback => {
      callback();
      return () => {};
    }
  });
  return { controller, refs, state, calls };
}

test("admin ops first dashboard health wait shows neutral shell then real data", async () => {
  const dashboardHealth = createDeferred();
  const { controller, refs, calls } = createFixture({
    getBridge: path => {
      if (path === "/ops/dashboard-health") return dashboardHealth.promise;
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    }
  });

  const loadPromise = controller.loadOpsHealthData();
  await flush();

  assert.equal(refs.adminOpsTrendsEl.textContent, "No run trend data yet.");
  assert.equal(refs.adminOpsTrendsEl.textContent.includes("Loading operations health"), false);
  assert.deepEqual(calls, ["/ops/dashboard-health"]);

  dashboardHealth.resolve({ alerts: [], kpis: {}, schedule: {}, status: "healthy" });
  await loadPromise;
  await flush();
  controller.stopOpsHealthPolling();

  assert.equal(refs.adminOpsKpisEl.innerHTML, "kpis:healthy");
  assert.equal(refs.adminOpsTrendsEl.textContent, "trends:0");
});

test("admin ops first dashboard health failure renders explicit unavailable state", async () => {
  const { controller, refs } = createFixture({
    getBridge: path => {
      if (path === "/ops/dashboard-health") throw new Error("Bridge request timed out");
      throw new Error(`unexpected path ${path}`);
    }
  });

  await controller.loadOpsHealthData();
  controller.stopOpsHealthPolling();

  assert.equal(refs.adminOpsTrendsEl.textContent, "Ops health unavailable: Bridge request timed out");
  assert.match(refs.adminOpsAlertsEl.innerHTML, /Ops health unavailable: Bridge request timed out/);
});

test("admin ops poll refresh with cache does not regress to loading placeholder", async () => {
  const dashboardHealth = createDeferred();
  const { controller, refs } = createFixture({
    state: {
      latestOpsHealthCache: { alerts: [], kpis: {}, schedule: {}, status: "healthy" }
    },
    getBridge: path => {
      if (path === "/ops/dashboard-health") return dashboardHealth.promise;
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    }
  });
  refs.adminOpsTrendsEl.textContent = "cached trend";

  const loadPromise = controller.loadOpsHealthData({ fromPoll: true });
  await flush();

  assert.equal(refs.adminOpsTrendsEl.textContent, "cached trend");
  dashboardHealth.resolve({ alerts: [], kpis: {}, schedule: {}, status: "healthy" });
  await loadPromise;
  controller.stopOpsHealthPolling();
});
