import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createClassList,
  createDeferredRenderScheduler,
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushAdminOpsBackground() {
  await Promise.resolve();
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

function createOpsControllerForBridgeStatus({
  state = { adminBusyState: {} },
  refs = {},
  getBridge,
  onBridgeStatusChange = () => {}
} = {}) {
  return createAdminOpsController({
    state,
    refs: {
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
      ...refs
    },
    getBridge,
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
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange,
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });
}

test("admin bridge status pill uses lightweight ops health instead of registry summary", async () => {
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList() })
  };
  const calls = [];
  const statuses = [];
  const controller = createOpsControllerForBridgeStatus({
    refs,
    getBridge: async (path, options = {}) => {
      calls.push({ path, options });
      if (path === "/ops/health") return { service: "baluffo-bridge", status: "healthy" };
      throw new Error(`unexpected path ${path}`);
    },
    onBridgeStatusChange(status) {
      statuses.push(status);
    }
  });

  await controller.pollBridgeStatus();

  assert.deepEqual(calls.map(call => call.path), ["/ops/health"]);
  assert.equal(calls[0].options.timeoutMs, 5000);
  assert.equal(refs.adminBridgeStatusBadgeEl.textContent, "Bridge Online");
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("online"), true);
  assert.deepEqual(statuses, ["online"]);
});

test("admin bridge status pill treats one failed health poll as checking, not offline", async () => {
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList() })
  };
  const statuses = [];
  let fail = false;
  const controller = createOpsControllerForBridgeStatus({
    refs,
    getBridge: async path => {
      if (path !== "/ops/health") throw new Error(`unexpected path ${path}`);
      if (fail) throw new Error("Bridge request timed out");
      return { service: "baluffo-bridge", status: "healthy" };
    },
    onBridgeStatusChange(status) {
      statuses.push(status);
    }
  });

  await controller.pollBridgeStatus();
  fail = true;
  await controller.pollBridgeStatus();

  assert.equal(refs.adminBridgeStatusBadgeEl.textContent, "Bridge Checking");
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("checking"), true);
  assert.deepEqual(statuses, ["online"]);

  await controller.pollBridgeStatus();

  assert.equal(refs.adminBridgeStatusBadgeEl.textContent, "Bridge Offline");
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("offline"), true);
  assert.deepEqual(statuses, ["online", "offline"]);
});

test("admin ops controller preserves optimistic rows while history lags", async () => {
  const cases = [
    {
      label: "discovery",
      optimisticKey: "discoveryOptimisticRun",
      busyKey: "liveDiscoveryRunning",
      runId: "discovery_123"
    },
    {
      label: "fetch",
      optimisticKey: "fetchOptimisticRun",
      busyKey: "liveFetchRunning",
      runId: "fetch_123"
    }
  ];

  for (const { label, optimisticKey, busyKey, runId } of cases) {
    const state = {
      latestOpsHealthCache: null,
      discoveryOptimisticRun: null,
      fetchOptimisticRun: null,
      [optimisticKey]: {
        runId,
        startedAt: "2026-03-08T10:01:00.000Z"
      },
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
    const runModels = [];
    let optimisticApplied = 0;
    const renderScheduler = createDeferredRenderScheduler();
    const controller = createAdminOpsController({
      state,
      refs,
      getBridge: async path => {
        if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
        if (path === "/ops/history?limit=80") return { runs: [] };
        if (path === "/ops/task-state?view=summary") return { tasks: [] };
        if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
        throw new Error(`unexpected path ${path}`);
      },
      postBridge: async () => ({}),
      deriveAdminRunsModel: () => {
        optimisticApplied += 1;
        return {
          currentRows: [],
          visibleCompletedRows: [],
          olderCompletedRows: [],
          hasLiveRuns: false,
          liveTypes: []
        };
      },
      getOpsPollIntervalMs: () => 5000,
      renderAdminOpsAlerts() {},
      renderAdminOpsKpis() {},
      renderAdminOpsSchedule() {},
      renderAdminOpsFetcherMetrics() {},
      renderAdminOpsTrends() {},
      renderAdminOpsHistory(_el, runModel) {
        runModels.push(runModel);
      },
      loadSyncStatus: async () => {},
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

    await controller.loadOpsHealthData();
    await flushAdminOpsBackground();
    renderScheduler.flush();
    controller.stopOpsHealthPolling();

    assert.ok(optimisticApplied >= 1, label);
    assert.equal(state.adminBusyState[busyKey], false, label);
    assert.ok(runModels.length >= 1, label);
    assert.equal(runModels.at(-1).currentRows.length, 0, label);
  }
});

test("admin ops controller startup uses summary ops routes before deferred detail", async () => {
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
  let historyRenderCount = 0;
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      if (path === "/ops/history?limit=80") return new Promise(() => {});
      if (path === "/ops/fetcher-metrics?windowRuns=80") return new Promise(() => {});
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
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {
      historyRenderCount += 1;
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

  await controller.loadOpsHealthData();
  await flushAdminOpsBackground();
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.deepEqual(calls, [
    "/ops/dashboard-health",
    "/ops/task-state?view=summary",
    "/registry/conflicts?view=summary"
  ]);
  assert.equal(historyRenderCount, 1);
});

test("admin ops controller renders health before summary requests settle", async () => {
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
  const taskState = createDeferred();
  const registrySummary = createDeferred();
  const calls = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/task-state?view=summary") return taskState.promise;
      if (path === "/registry/conflicts?view=summary") return registrySummary.promise;
      if (path === "/ops/history?limit=80") return new Promise(() => {});
      if (path === "/ops/fetcher-metrics?windowRuns=80") return new Promise(() => {});
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
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends(el) {
      if (el) el.textContent = "Health rendered";
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

  await controller.loadOpsHealthData();

  assert.equal(refs.adminOpsTrendsEl.textContent, "Health rendered");
  assert.equal(state.adminBusyState.opsLoad, false);
  assert.deepEqual(calls.slice(0, 3), [
    "/ops/dashboard-health",
    "/ops/task-state?view=summary",
    "/registry/conflicts?view=summary"
  ]);

  taskState.resolve({ tasks: [], count: 0, summary: true });
  registrySummary.resolve({ summary: { conflictCount: 0 }, summaryView: true });
  await flushAdminOpsBackground();
  controller.stopOpsHealthPolling();
});

test("admin ops controller renders bridge task-state without reattaching from history-only rows", async () => {
  const cases = [
    {
      label: "fetch",
      taskType: "fetch",
      busyKey: "liveFetchRunning",
      watcherKey: "fetcherWatch",
      liveTypes: ["fetch"],
      runId: "fetch_123"
    },
    {
      label: "discovery",
      taskType: "discovery",
      busyKey: "liveDiscoveryRunning",
      watcherKey: "discoveryWatch",
      liveTypes: ["discovery"],
      runId: "discovery_123"
    }
  ];

  for (const { label, taskType, busyKey, watcherKey, liveTypes, runId } of cases) {
    const state = {
      latestOpsHealthCache: null,
      fetchOptimisticRun: null,
      discoveryOptimisticRun: null,
      adminBusyState: {
        opsLoad: false,
        fetcherWatch: false,
        discoveryWatch: false,
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
    const runModels = [];
    const calls = [];
    const renderScheduler = createDeferredRenderScheduler();
    let controller;
    try {
      controller = createAdminOpsController({
        state,
        refs,
        getBridge: async path => {
          if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
          if (path === "/ops/history?limit=80") return { runs: [] };
          if (path === "/ops/task-state?view=summary") return {
            tasks: [
              {
                taskType,
                type: taskType,
                runId,
                active: true,
                startedAt: "2026-03-08T10:01:00.000Z",
                status: "running"
              }
            ]
          };
          if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
          throw new Error(`unexpected path ${path}`);
        },
        postBridge: async () => ({}),
        deriveAdminRunsModel: ({ taskState }) => ({
          currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
          visibleCompletedRows: [],
          olderCompletedRows: [],
          hasLiveRuns: true,
          liveTypes
        }),
        getOpsPollIntervalMs: () => 5000,
        renderAdminOpsAlerts() {},
        renderAdminOpsKpis() {},
        renderAdminOpsSchedule() {},
        renderAdminOpsFetcherMetrics() {},
        renderAdminOpsTrends() {},
        renderAdminOpsHistory(_el, runModel) {
          runModels.push(runModel);
        },
        loadSyncStatus: async () => {},
        setBusyFlag(key, value) {
          state.adminBusyState[key] = value;
        },
        showToast() {},
        getErrorMessage: err => String(err?.message || err || "unknown"),
        adminDispatch: { dispatch() {} },
        adminActions: { OPS_REFRESHED: "ops/refreshed" },
        escapeHtml: value => String(value || ""),
        onBridgeStatusChange() {},
        loadDiscoveryData: async () => {
          calls.push("loadDiscoveryData");
        },
        bridgeStatusPollIntervalMs: 1000,
        idlePollIntervalMs: 1000,
        renderScheduler: renderScheduler.schedule
      });

      await controller.loadOpsHealthData();
      await flushAdminOpsBackground();
      renderScheduler.flush();

      assert.equal(state.adminBusyState[busyKey], true, label);
      assert.equal(state.adminBusyState[watcherKey], false, label);
      assert.equal(runModels.length, 1, label);
      assert.equal(runModels[0].currentRows.length, 1, label);
      assert.deepEqual(calls, [], label);
    } finally {
      controller?.stopOpsHealthPolling?.();
    }
  }
});

test("admin ops controller quietly auto-attaches active fetch and discovery task-state rows", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
    adminBusyState: {
      opsLoad: false,
      fetcherWatch: false,
      discoveryWatch: false,
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
  const calls = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state?view=summary") {
        return {
          tasks: [
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_live_attach_1",
              active: true,
              startedAt: "2026-03-08T10:01:00.000Z",
              status: "running"
            },
            {
              taskType: "discovery",
              type: "discovery",
              runId: "discovery_live_attach_1",
              active: true,
              startedAt: "2026-03-08T10:02:00.000Z",
              status: "running"
            }
          ]
        };
      }
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ taskState }) => ({
      currentRows: (taskState?.tasks || []).map(row => ({ ...row, isLive: true })),
      visibleCompletedRows: [],
      olderCompletedRows: [],
      hasLiveRuns: true,
      liveTypes: ["fetch", "discovery"]
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends() {},
    renderAdminOpsHistory() {},
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    loadDiscoveryData: async () => {},
    attachToActiveFetchRun(runMeta, options) {
      calls.push(`fetch:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
    },
    loadLatestFetcherReport: async options => {
      calls.push(`fetchReport:${String(Boolean(options?.silent))}:${String(Boolean(options?.hydrateActiveProgress))}`);
      return {};
    },
    attachToActiveDiscoveryRun(runMeta, options) {
      calls.push(`discovery:${String(runMeta?.runId || "")}:${String(options?.announceStart)}`);
    },
    loadLatestDiscoveryReport: async options => {
      calls.push(`discoveryReport:${String(Boolean(options?.silent))}`);
      return {};
    },
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000,
    renderScheduler: renderScheduler.schedule
  });

  await controller.loadOpsHealthData();
  await flushAdminOpsBackground();
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.ok(calls.includes("fetch:fetch_live_attach_1:false"));
  assert.ok(calls.includes("fetchReport:true:true"));
  assert.ok(calls.includes("discovery:discovery_live_attach_1:false"));
  assert.ok(calls.includes("discoveryReport:true"));
});

test("admin ops controller trusts empty lifecycle task-state samples immediately", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
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
  const taskStatePayloads = [
    {
      tasks: [
        {
          taskType: "fetch",
          type: "fetch",
          runId: "fetch_live_stable_1",
          active: true,
          startedAt: "2026-03-08T10:01:00.000Z",
          status: "running"
        }
      ]
    },
    { tasks: [] },
    { tasks: [] }
  ];
  const renderedCurrentCounts = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state?view=summary") return taskStatePayloads.shift() || { tasks: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
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
      renderedCurrentCounts.push(runModel.currentRows.length);
    },
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
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

  await controller.loadOpsHealthData();
  await flushAdminOpsBackground();
  renderScheduler.flush();
  await controller.loadOpsHealthData();
  await flushAdminOpsBackground();
  renderScheduler.flush();
  await controller.loadOpsHealthData();
  await flushAdminOpsBackground();
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.deepEqual(renderedCurrentCounts, [1, 0, 0]);
  assert.equal(state.adminBusyState.liveFetchRunning, false);
});

test("admin ops controller clears live rows on task-state polling failure", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
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
  let taskStateCallCount = 0;
  const renderedCurrentCounts = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state?view=summary") {
        taskStateCallCount += 1;
        if (taskStateCallCount > 1) {
          throw new Error("transient task-state error");
        }
        return {
          tasks: [
            {
              taskType: "fetch",
              type: "fetch",
              runId: "fetch_live_error_hold_1",
              active: true,
              startedAt: "2026-03-08T10:01:00.000Z",
              status: "running"
            }
          ]
        };
      }
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
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
      renderedCurrentCounts.push(runModel.currentRows.length);
    },
    loadSyncStatus: async () => {},
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
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

  await controller.loadOpsHealthData();
  await flushAdminOpsBackground();
  renderScheduler.flush();
  await controller.loadOpsHealthData();
  await flushAdminOpsBackground();
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.deepEqual(renderedCurrentCounts, [1, 0]);
  assert.equal(state.adminBusyState.liveFetchRunning, false);
});

test("admin ops controller skips stale deferred detail renders after a newer refresh", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestOpsHistoryPayload: null,
    latestTaskStatePayload: null,
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
  const histories = [
    { runs: [{ id: "old_run", taskType: "fetch" }] },
    { runs: [{ id: "new_run", taskType: "fetch" }] }
  ];
  const renderedRunIds = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return histories.shift() || { runs: [] };
      if (path === "/ops/task-state?view=summary") return { tasks: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return {};
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ historyRuns }) => ({
      currentRows: [],
      visibleCompletedRows: historyRuns.map(row => ({ runId: row.id })),
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
    renderAdminOpsHistory(_el, runModel) {
      renderedRunIds.push(runModel.visibleCompletedRows[0]?.runId || "");
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    showToast() {},
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

  await controller.loadOpsHealthData();
  await controller.loadOpsHealthData();
  await new Promise(resolve => setTimeout(resolve, 750));
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.equal(renderedRunIds.at(-1), "new_run");
  assert.equal(renderedRunIds.includes("old_run"), false);
});

test("admin ops controller ignores stale task-state summary responses", async () => {
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
  const firstTaskState = createDeferred();
  const secondTaskState = createDeferred();
  const taskStateResponses = [firstTaskState, secondTaskState];
  const renderedRunIds = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/task-state?view=summary") return taskStateResponses.shift()?.promise || { tasks: [] };
      if (path === "/registry/conflicts?view=summary") return { summary: { conflictCount: 0 }, conflicts: [], summaryView: true };
      if (path === "/ops/history?limit=80") return new Promise(() => {});
      if (path === "/ops/fetcher-metrics?windowRuns=80") return new Promise(() => {});
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: payload => ({
      currentRows: Array.isArray(payload?.taskState?.tasks) ? payload.taskState.tasks : [],
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
    renderAdminOpsHistory(_el, runModel) {
      renderedRunIds.push(runModel.currentRows[0]?.runId || "");
    },
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

  await controller.loadOpsHealthData();
  await controller.loadOpsHealthData();
  secondTaskState.resolve({ tasks: [{ taskType: "fetch", runId: "new_run", active: true }] });
  await flushAdminOpsBackground();
  renderScheduler.flush();
  firstTaskState.resolve({ tasks: [{ taskType: "fetch", runId: "old_run", active: true }] });
  await flushAdminOpsBackground();
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.equal(renderedRunIds.at(-1), "new_run");
  assert.equal(renderedRunIds.includes("old_run"), false);
});
