import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushAdminOpsBackground() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setTimeout(resolve, 0));
  await Promise.resolve();
}

function createDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

function createOpsRefs() {
  return {
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminRegistryConflictsReviewEl: createElement()
  };
}

function createOpsState(extra = {}) {
  return {
    latestOpsHealthCache: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    },
    ...extra
  };
}

function createBaseControllerOptions(overrides = {}) {
  const state = overrides.state || createOpsState();
  return {
    state,
    refs: overrides.refs || createOpsRefs(),
    getBridge: overrides.getBridge,
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
    ...overrides
  };
}

test("admin ops summary auto-loads bounded fetch KPI cards", async () => {
  const state = createOpsState();
  const renderedKpis = [];
  const calls = [];
  const controller = createAdminOpsController(createBaseControllerOptions({
    state,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/dashboard-health?view=summary") return { alerts: [], kpis: { pendingApprovalsCount: 812 }, schedule: {}, status: "healthy", summaryView: true };
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: { lastSuccessfulFetchAge: "4m", sevenDayFetchSuccessRate: 0.91 }, summaryView: true };
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    },
    renderAdminOpsKpis(_el, kpis) {
      renderedKpis.push({ ...kpis });
    },
    renderScheduler: createDeferredRenderScheduler().schedule
  }));

  await controller.loadOpsHealthData({ summary: true });
  await flushAdminOpsBackground();
  controller.stopOpsHealthPolling();

  assert.ok(calls.includes("/ops/fetch-kpis?view=summary"));
  assert.equal(renderedKpis.at(-1).lastSuccessfulFetchAge, "4m");
  assert.equal(renderedKpis.at(-1).sevenDayFetchSuccessRate, 0.91);
});

test("admin ops summary polls preserve previously loaded fetch KPI values", async () => {
  const state = createOpsState({
    latestOpsHealthCache: {
      alerts: [],
      status: "healthy",
      kpis: {
        lastSuccessfulFetchAge: "4m",
        sevenDayFetchSuccessRate: 0.91,
        avgFetchDurationMs7d: 12000,
        failedSourceRatioLatest: 0.22
      }
    }
  });
  const controller = createAdminOpsController(createBaseControllerOptions({
    state,
    getBridge: async path => {
      if (path === "/ops/dashboard-health?view=summary") return { alerts: [], kpis: { sevenDayFetchSuccessRate: 0, avgFetchDurationMs7d: 0, failedSourceRatioLatest: 0 }, schedule: {}, status: "healthy", summaryView: true };
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    }
  }));

  await controller.loadOpsHealthData({ summary: true, fromPoll: true });
  await flushAdminOpsBackground();
  controller.stopOpsHealthPolling();

  assert.equal(state.latestOpsHealthCache.kpis.sevenDayFetchSuccessRate, 0.91);
  assert.equal(state.latestOpsHealthCache.kpis.avgFetchDurationMs7d, 12000);
  assert.equal(state.latestOpsHealthCache.kpis.failedSourceRatioLatest, 0.22);
});

test("admin ops lightweight polls cannot downgrade authoritative warning state", async () => {
  const state = createOpsState({
    latestOpsHealthCache: {
      alertsEvaluated: true,
      alerts: [
        { id: "stale_fetch", severity: "warning", message: "Last fetch is stale." }
      ],
      status: "warning",
      kpis: {
        lastSuccessfulFetchAge: "26h",
        sevenDayFetchSuccessRate: 0.91
      }
    }
  });
  const controller = createAdminOpsController(createBaseControllerOptions({
    state,
    getBridge: async path => {
      if (path === "/ops/dashboard-health?view=summary") {
        return {
          alerts: [],
          kpis: {},
          schedule: {},
          status: "healthy",
          summaryView: true
        };
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    }
  }));

  await controller.loadOpsHealthData({ summary: true, fromPoll: true });
  await flushAdminOpsBackground();
  controller.stopOpsHealthPolling();

  assert.equal(state.latestOpsHealthCache.status, "warning");
  assert.equal(state.latestOpsHealthCache.alerts[0].id, "stale_fetch");
  assert.equal(state.latestOpsHealthCache.kpis.lastSuccessfulFetchAge, "26h");
});

test("admin ops evaluated summary can clear a previous warning state", async () => {
  const state = createOpsState({
    latestOpsHealthCache: {
      alertsEvaluated: true,
      alerts: [
        { id: "stale_fetch", severity: "warning", message: "Last fetch is stale." }
      ],
      status: "warning",
      kpis: {}
    }
  });
  const controller = createAdminOpsController(createBaseControllerOptions({
    state,
    getBridge: async path => {
      if (path === "/ops/dashboard-health?view=summary") {
        return {
          alerts: [],
          alertsEvaluated: true,
          kpis: {},
          schedule: {},
          status: "healthy",
          summaryView: true
        };
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    }
  }));

  await controller.loadOpsHealthData({ summary: true, fromPoll: true });
  await flushAdminOpsBackground();
  controller.stopOpsHealthPolling();

  assert.equal(state.latestOpsHealthCache.status, "healthy");
  assert.deepEqual(state.latestOpsHealthCache.alerts, []);
});

test("admin ops fetch KPI coalescing applies to the latest render token", async () => {
  const state = createOpsState();
  const renderedKpis = [];
  const fetchKpis = createDeferred();
  let fetchKpisCalls = 0;
  const controller = createAdminOpsController(createBaseControllerOptions({
    state,
    getBridge: async path => {
      if (path === "/ops/dashboard-health?view=summary") return { alerts: [], kpis: {}, schedule: {}, status: "healthy", summaryView: true };
      if (path === "/ops/fetch-kpis?view=summary") {
        fetchKpisCalls += 1;
        return fetchKpis.promise;
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    },
    renderAdminOpsKpis(_el, kpis) {
      renderedKpis.push({ ...kpis });
    }
  }));

  await controller.loadOpsHealthData({ summary: true });
  await controller.loadOpsHealthData({ summary: true });
  fetchKpis.resolve({ ok: true, kpis: { lastSuccessfulFetchAge: "3m" }, summaryView: true });
  await flushAdminOpsBackground();
  controller.stopOpsHealthPolling();

  assert.equal(fetchKpisCalls, 1);
  assert.equal(renderedKpis.at(-1).lastSuccessfulFetchAge, "3m");
});

test("admin registry and sync disclosure backfills missing summary data", async () => {
  const state = createOpsState({
    latestOpsHealthCache: {
      alerts: [],
      status: "healthy",
      kpis: {
        registrySync: {
          activeCount: 12,
          pendingCount: 4,
          hiddenPendingCount: 0,
          deferredPendingCount: 0
        }
      }
    }
  });
  const calls = [];
  const renderedKpis = [];
  const controller = createAdminOpsController(createBaseControllerOptions({
    state,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/dashboard-health?view=summary") {
        return {
          alerts: [],
          alertsEvaluated: true,
          kpis: {
            registrySync: {
              activeCount: 12,
              pendingCount: 4,
              lastSyncStatus: "ok",
              lastSyncAt: "2026-03-08T08:00:00.000Z"
            }
          },
          schedule: {},
          status: "healthy",
          summaryView: true
        };
      }
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: {}, summaryView: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      throw new Error(`unexpected path ${path}`);
    },
    renderAdminOpsKpis(_el, kpis) {
      renderedKpis.push(JSON.parse(JSON.stringify(kpis || {})));
    }
  }));

  await controller.loadRegistrySyncDiagnosticsData({ silent: false });
  controller.stopOpsHealthPolling();

  assert.ok(calls.includes("/ops/dashboard-health?view=summary"));
  assert.equal(state.latestOpsHealthCache.kpis.registrySync.activeCount, 12);
  assert.equal(state.latestOpsHealthCache.kpis.registrySync.lastSyncStatus, "ok");
  assert.equal(renderedKpis.at(-1).registrySync.pendingCount, 4);
});

test("admin bootstrap active rows start task-state polling without manual ops refresh", async () => {
  const state = createOpsState();
  const calls = [];
  const controller = createAdminOpsController(createBaseControllerOptions({
    state,
    getBridge: async path => {
      calls.push(path);
      if (path === "/tasks/run-jobs-pipeline-status") {
        return {
          active: true,
          runId: "pipeline_1",
          stage: "fetch",
          progress: { label: "Fetching job listings" }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    deriveAdminRunsModel: () => ({
      currentRows: [],
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: true,
      liveTypes: ["pipeline"]
    }),
    getOpsPollIntervalMs: () => 1
  }));

  controller.applyBootstrapPayload({
    app: { version: "0.2.59" },
    tasks: {
      current: [{ taskType: "pipeline", runId: "pipeline_1", active: true }],
      recent: []
    },
    registrySummary: {},
    schedule: {}
  });
  await flushAdminOpsBackground();

  assert.ok(state.pipelineStatusPollTimer);
  assert.equal(calls.includes("/ops/task-state?view=summary"), false);
  assert.equal(state.adminBusyState.livePipelineRunning, true);
  controller.stopOpsHealthPolling();
});
