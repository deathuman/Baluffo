import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { mergeOpsHistoryPayload } from "../../../frontend/admin/app/ops/health.js";
import {
  createClassList,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

function createDeferred() {
  let resolve;
  const promise = new Promise(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function createController({ getBridge, onHistory, onTrends } = {}) {
  const state = { adminBusyState: {} };
  return createAdminOpsController({
    state,
    refs: {
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
      adminOpsHistoryEl: createElement(),
      adminOpsTrendsEl: createElement()
    },
    getBridge,
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ historyRuns }) => ({
      currentRows: [],
      visibleCompletedRows: Array.isArray(historyRuns) ? historyRuns : [],
      olderCompletedRows: [],
      hasLiveRuns: false,
      liveTypes: []
    }),
    getOpsPollIntervalMs: () => 5000,
    renderAdminOpsAlerts() {},
    renderAdminOpsKpis() {},
    renderAdminOpsSchedule() {},
    renderAdminOpsFetcherMetrics() {},
    renderAdminOpsTrends: onTrends || (() => {}),
    renderAdminOpsHistory: onHistory || (() => {}),
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    idlePollIntervalMs: 1000,
    awaitBridgeReady: async () => true,
    renderScheduler: task => task()
  });
}

test("admin lightweight history loader fetches bounded history without full diagnostics", async () => {
  const calls = [];
  let renderedModel = null;
  let trendRuns = null;
  const controller = createController({
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/history?limit=20") {
        return { runs: [{ runId: "pipeline_1", type: "pipeline", status: "completed" }] };
      }
      throw new Error(`unexpected path ${path}`);
    },
    onHistory(_el, model) {
      renderedModel = model;
    },
    onTrends(_el, runs) {
      trendRuns = runs;
    }
  });

  await controller.loadOpsHistoryData({ limit: 20, silent: true });

  assert.deepEqual(calls, ["/ops/history?limit=20"]);
  assert.equal(renderedModel?.visibleCompletedRows?.[0]?.runId, "pipeline_1");
  assert.equal(trendRuns?.[0]?.runId, "pipeline_1");
});

test("admin recent history loader defaults to two completed runs", async () => {
  const calls = [];
  const controller = createController({
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/history?limit=2") {
        return { runs: [{ runId: "pipeline_recent", type: "pipeline", status: "completed" }] };
      }
      throw new Error(`unexpected path ${path}`);
    }
  });

  await controller.loadOpsHistoryData({ silent: true });

  assert.deepEqual(calls, ["/ops/history?limit=2"]);
});

test("admin activity startup renders loading until authoritative history succeeds", async () => {
  const calls = [];
  const history = createDeferred();
  const state = { adminBusyState: {} };
  const historyEl = createElement({ dataset: {} });
  const controller = createAdminOpsController({
    state,
    refs: {
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
      adminOpsHistoryEl: historyEl,
      adminOpsTrendsEl: createElement()
    },
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/history?limit=2") return history.promise;
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ historyRuns }) => ({
      currentRows: [],
      visibleCompletedRows: Array.isArray(historyRuns) ? historyRuns : [],
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
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    idlePollIntervalMs: 1000,
    awaitBridgeReady: async () => true,
    renderScheduler: task => task()
  });

  const load = controller.loadOpsHistoryData({ force: true, silent: true });
  await Promise.resolve();

  assert.deepEqual(calls, ["/ops/history?limit=2"]);
  assert.match(historyEl.innerHTML, /Loading recent activity/);
  history.resolve({ runs: [{ runId: "pipeline_recent", type: "pipeline", status: "completed" }] });
  await load;

  assert.match(historyEl.innerHTML, /Pipeline/);
  assert.doesNotMatch(historyEl.innerHTML, /No run history yet/);
});

test("admin activity does not treat degraded bootstrap empty recent rows as loaded history", async () => {
  let renderedOptions = null;
  let renderedModel = null;
  const controller = createController({
    getBridge: async path => {
      throw new Error(`unexpected path ${path}`);
    },
    onHistory(_el, model, options) {
      renderedModel = model;
      renderedOptions = options;
    }
  });

  controller.applyBootstrapPayload({
    ok: true,
    degraded: true,
    source: "container-gateway-fallback",
    tasks: { current: [], recent: [] },
    ops: { degraded: true, schedule: {} }
  });

  assert.equal(renderedModel.visibleCompletedRows.length, 0);
  assert.equal(renderedOptions.historyLoaded, false);
});

test("admin activity route failure renders retrying instead of no history", async () => {
  const refsHistoryEl = createElement({ dataset: {} });
  const state = { adminBusyState: {} };
  const controller = createAdminOpsController({
    state,
    refs: {
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList() }),
      adminOpsHistoryEl: refsHistoryEl,
      adminOpsTrendsEl: createElement()
    },
    getBridge: async path => {
      if (path === "/ops/history?limit=2") throw new Error("history timeout");
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: ({ historyRuns }) => ({
      currentRows: [],
      visibleCompletedRows: Array.isArray(historyRuns) ? historyRuns : [],
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
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    idlePollIntervalMs: 1000,
    awaitBridgeReady: async () => true,
    renderScheduler: task => task()
  });

  await controller.loadOpsHistoryData({ force: true, silent: true });

  assert.match(refsHistoryEl.innerHTML, /Activity delayed; retrying/);
  assert.doesNotMatch(refsHistoryEl.innerHTML, /No run history yet/);
});

test("admin older history request waits for smaller in-flight recent request", async () => {
  const calls = [];
  const recent = createDeferred();
  const controller = createController({
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/history?limit=2") return recent.promise;
      if (path === "/ops/history?limit=80") return { runs: [] };
      throw new Error(`unexpected path ${path}`);
    }
  });

  const recentLoad = controller.loadOpsHistoryData({ silent: true });
  const fullLoad = controller.loadOpsHistoryData({ limit: 80, silent: true });
  await Promise.resolve();
  assert.deepEqual(calls, ["/ops/history?limit=2"]);

  recent.resolve({ runs: [] });
  await recentLoad;
  await fullLoad;

  assert.deepEqual(calls, ["/ops/history?limit=2", "/ops/history?limit=80"]);
});

test("admin history merges a later recent refresh into the full cache", () => {
  const existing = {
    runs: Array.from({ length: 80 }, (_, index) => ({
      runId: `run_${index}`,
      status: index === 0 ? "running" : "completed",
      open: index === 0
    }))
  };
  const merged = mergeOpsHistoryPayload(existing, {
    runs: [
      { runId: "run_0", status: "completed" },
      { runId: "run_new", status: "running", open: true }
    ]
  });

  assert.equal(merged.runs.length, 80);
  assert.equal(merged.runs[0].runId, "run_0");
  assert.equal(merged.runs[0].status, "completed");
  assert.equal(merged.runs.some(row => row.runId === "run_new"), true);
  assert.equal(new Set(merged.runs.map(row => row.runId)).size, 80);
});
