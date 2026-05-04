import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createElement,
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin ops controller renders health metrics and dedup lists separately", async () => {
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
    adminOpsDedupListsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement()
  };
  const calls = [];
  const fetcherMetricsPayload = {
    latestRun: {
      dedupEvidence: {
        dedupAuditGate: { status: "blocked", lifecycleUxReady: false }
      }
    },
    history: {}
  };
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/health") return { alerts: [], kpis: {}, schedule: {}, status: "healthy" };
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state") return { tasks: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") return fetcherMetricsPayload;
      if (path === "/registry/conflicts") return { summary: { conflictCount: 0 }, conflicts: [] };
      if (path === "/source-policy/recommendations") return { recommendations: { pairs: [] } };
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
    renderAdminOpsFetcherMetrics(el, payload) {
      calls.push({ kind: "health", el, payload });
    },
    renderAdminOpsDedupLists(el, payload) {
      calls.push({ kind: "dedup", el, payload });
    },
    renderAdminSourcePolicyReview() {},
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
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000
  });

  await controller.loadOpsHealthData();
  controller.stopOpsHealthPolling();

  assert.equal(calls.filter(call => call.kind === "health").length, 1);
  assert.equal(calls.filter(call => call.kind === "dedup").length, 1);
  assert.equal(calls.find(call => call.kind === "health").el, refs.adminOpsFetcherMetricsEl);
  assert.equal(calls.find(call => call.kind === "dedup").el, refs.adminOpsDedupListsEl);
  assert.deepEqual(
    calls.find(call => call.kind === "dedup").payload.latestRun.dedupEvidence,
    fetcherMetricsPayload.latestRun.dedupEvidence
  );
});
