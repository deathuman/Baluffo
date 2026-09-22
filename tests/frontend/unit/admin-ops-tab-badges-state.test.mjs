// Terminal badge state: `loaded: false` conflates "still fetching" with "will never
// arrive". Only an `error` code separates them, and the pending-state case this is
// contrasted against stays in admin-ops-tab-badges-controller.test.mjs.
import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";
import { createOpsTabButton as createTabButton } from "./helpers/admin-controller-test-helpers.mjs";

test("admin ops tab badges distinguish a failed count from one still loading", async () => {
  // Both arrive as `loaded: false`; only the presence of an `error` code separates
  // "still fetching" from "will never arrive". Rendering them alike is what left a
  // permanently-pending badge on screen.
  const badgeFor = key => createElement({ dataset: { opsTab: key } });
  const refs = {
    adminOpsTabBtnEls: [
      createTabButton("overview"), createTabButton("discovery"), createTabButton("source-policy"),
      createTabButton("registry-conflicts"), createTabButton("dedup")
    ],
    adminOpsTabBadgeEls: [
      badgeFor("overview"), badgeFor("discovery"), badgeFor("source-policy"),
      badgeFor("registry-conflicts"), badgeFor("dedup")
    ],
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
  const state = { adminBusyState: {}, latestOpsHealthCache: null };
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/admin/ops-tab-counts?view=summary") {
        return {
          ok: true,
          summaryView: true,
          badges: {
            overview: { count: 0, tone: "neutral", title: "No active alerts", loaded: true },
            discovery: { count: 0, tone: "neutral", title: "No discovery review items", loaded: true },
            "source-policy": { count: 0, tone: "neutral", title: "Loading Source Policy Review count", loaded: false },
            "registry-conflicts": { count: 0, tone: "neutral", title: "No registry conflicts", loaded: true },
            dedup: {
              count: 0, tone: "neutral", title: "Dedup count unavailable",
              loaded: false, error: "dedup_evidence_missing"
            }
          }
        };
      }
      if (path === "/ops/dashboard-health?view=summary") {
        return { alerts: [], kpis: {}, schedule: {}, status: "healthy", summaryView: true };
      }
      if (path === "/ops/task-state?view=summary") return { tasks: [], count: 0, summary: true };
      if (path === "/registry/conflicts?view=summary") {
        return { summary: { conflictCount: 0 }, summaryStatus: "ready", conflicts: [], summaryView: true };
      }
      if (path === "/ops/fetch-kpis?view=summary") return { ok: true, kpis: {}, summaryView: true };
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async () => ({}),
    deriveAdminRunsModel: () => ({
      currentRows: [], visibleCompletedRows: [], olderCompletedRows: [], hasLiveRuns: false, liveTypes: []
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
    setBusyFlag() {},
    showToast() {},
    getErrorMessage: err => String(err?.message || err || "unknown"),
    adminDispatch: { dispatch() {} },
    adminActions: { OPS_REFRESHED: "ops/refreshed" },
    escapeHtml: value => String(value || ""),
    onBridgeStatusChange() {},
    bridgeStatusPollIntervalMs: 1000,
    idlePollIntervalMs: 1000,
    renderScheduler: createDeferredRenderScheduler().schedule
  });

  await controller.loadOpsHealthData({ summary: true });
  await new Promise(resolve => setTimeout(resolve, 0));
  controller.stopOpsHealthPolling();

  const text = key => refs.adminOpsTabBadgeEls.find(el => el.dataset.opsTab === key).textContent;
  assert.equal(
    text("source-policy"),
    "...",
    "a count with no error code is still loading and keeps the pending dots",
  );
  assert.equal(
    text("dedup"),
    "-",
    "a count that failed carries an error code and must not keep spinning",
  );
});
