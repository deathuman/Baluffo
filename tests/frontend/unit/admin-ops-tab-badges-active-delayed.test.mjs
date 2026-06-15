import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

function createTabButton(key) {
  return createElement({
    dataset: { opsTab: key }
  });
}

test("admin ops tab badges render delayed markers while active pipeline suppresses count route", async () => {
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
  const keys = ["overview", "discovery", "source-policy", "registry-conflicts", "dedup"];
  const badges = Object.fromEntries(
    keys.map(key => [key, createElement({ dataset: { opsTab: key } })])
  );
  const refs = {
    adminOpsTabBtnEls: keys.map(createTabButton),
    adminOpsTabBadgeEls: keys.map(key => badges[key]),
    adminOpsTabOverviewEl: createElement(),
    adminOpsTabDiscoveryEl: createElement(),
    adminOpsTabSourcePolicyEl: createElement(),
    adminOpsTabRegistryConflictsEl: createElement(),
    adminOpsTabDedupEl: createElement(),
    adminSyncStatusEl: createElement(),
    adminSyncConfigHintEl: createElement(),
    adminOpsAlertsEl: createElement(),
    adminOpsKpisEl: createElement(),
    adminOpsScheduleEl: createElement(),
    adminOpsFetcherMetricsEl: createElement(),
    adminOpsHistoryEl: createElement(),
    adminOpsTrendsEl: createElement(),
    adminSourcePolicyReviewEl: createElement(),
    adminRegistryConflictsReviewEl: createElement(),
    adminDiscoveryReviewEl: createElement(),
    adminOpsDedupListsEl: createElement()
  };
  const calls = [];
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(String(path));
      if (path === "/tasks/run-jobs-pipeline-status") {
        return {
          active: true,
          runId: "pipeline_live_1",
          stage: "fetch",
          activeChildren: [{ taskType: "fetch", runId: "fetch_live_1", active: true }]
        };
      }
      if (path === "/ops/task-state?view=summary") {
        return {
          tasks: [
            { taskType: "fetch", type: "fetch", runId: "fetch_live_1", active: true },
            { taskType: "pipeline", type: "pipeline", runId: "pipeline_live_1", active: true }
          ],
          count: 2,
          summary: true
        };
      }
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: {}, summaryView: true };
      if (path === "/tasks/jobs-pipeline-schedule") return { ok: true, schedule: { enabled: true, intervalHours: 12 } };
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
    renderAdminOpsHistory() {},
    renderAdminOpsDedupLists() {},
    renderAdminSourcePolicyReview() {},
    loadSyncStatus: async () => {},
    attachToActiveFetchRun() {},
    loadLatestFetcherSummary: async () => null,
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

  await controller.loadOpsHealthData({ summary: true });
  await new Promise(resolve => setTimeout(resolve, 0));
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.equal(calls.includes("/admin/ops-tab-counts?view=summary"), false);
  for (const key of keys) {
    assert.equal(badges[key].textContent, "-");
  }
  assert.equal(badges.discovery.attributes["data-tooltip"], "Delayed while job update is running.");
});
