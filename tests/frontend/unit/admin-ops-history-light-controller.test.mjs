import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
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
