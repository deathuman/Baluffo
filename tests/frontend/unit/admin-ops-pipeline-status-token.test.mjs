import test from "node:test";
import assert from "node:assert/strict";

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

test("admin ops applies active pipeline status after bootstrap advances render token", async () => {
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
  const pipelineStatus = createDeferred();
  const renderedCurrentRows = [];
  const controller = createAdminOpsController({
    state,
    refs: {
      adminOpsAlertsEl: createElement(),
      adminOpsKpisEl: createElement(),
      adminOpsScheduleEl: createElement(),
      adminOpsFetcherMetricsEl: createElement(),
      adminOpsHistoryEl: createElement(),
      adminOpsTrendsEl: createElement(),
      adminRegistryConflictsReviewEl: createElement()
    },
    getBridge: async path => {
      if (path === "/tasks/run-jobs-pipeline-status") return pipelineStatus.promise;
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
      renderedCurrentRows.push(runModel.currentRows);
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
    idlePollIntervalMs: 1000
  });

  const fallbackPromise = controller.loadPipelineStatusFallbackData();
  controller.applyBootstrapPayload({
    tasks: { current: [], recent: [] },
    registrySummary: {},
    schedule: {},
    app: { version: "0.2.61" }
  });
  pipelineStatus.resolve({
    active: true,
    runId: "pipeline_live_2",
    stage: "fetch",
    progress: { label: "Fetching job listings" }
  });
  await fallbackPromise;

  assert.equal(renderedCurrentRows.at(-1)?.[0]?.runId, "pipeline_live_2");
  assert.equal(state.adminBusyState.livePipelineRunning, true);
  controller.stopOpsHealthPolling();
});
