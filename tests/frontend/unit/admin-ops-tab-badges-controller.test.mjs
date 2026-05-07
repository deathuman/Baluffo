import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

function createTabButton(key) {
  const listeners = {};
  return createElement({
    dataset: { opsTab: key },
    tabIndex: 0,
    addEventListener(type, handler) {
      listeners[type] = handler;
    },
    click() {
      listeners.click?.();
    }
  });
}

test("admin ops controller updates tab badges from loaded review payloads", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestDiscoveryReportCache: {
      candidateReview: {
        totalCandidates: 4
      }
    },
    latestSourcePolicyRecommendationsPayload: null,
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const overviewBtn = createTabButton("overview");
  const discoveryBtn = createTabButton("discovery");
  const sourcePolicyBtn = createTabButton("source-policy");
  const registryConflictsBtn = createTabButton("registry-conflicts");
  const dedupBtn = createTabButton("dedup");
  const overviewBadge = createElement({ dataset: { opsTab: "overview" } });
  const discoveryBadge = createElement({ dataset: { opsTab: "discovery" } });
  const sourcePolicyBadge = createElement({ dataset: { opsTab: "source-policy" } });
  const registryConflictsBadge = createElement({ dataset: { opsTab: "registry-conflicts" } });
  const dedupBadge = createElement({ dataset: { opsTab: "dedup" } });
  const refs = {
    adminOpsTabBtnEls: [overviewBtn, discoveryBtn, sourcePolicyBtn, registryConflictsBtn, dedupBtn],
    adminOpsTabBadgeEls: [overviewBadge, discoveryBadge, sourcePolicyBadge, registryConflictsBadge, dedupBadge],
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
  const renderScheduler = createDeferredRenderScheduler();
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      if (path === "/ops/dashboard-health") {
        return {
          alerts: [
            { id: "critical-health", severity: "critical", message: "Critical alert" }
          ],
          kpis: {},
          schedule: {},
          status: "warning"
        };
      }
      if (path === "/ops/history?limit=80") return { runs: [] };
      if (path === "/ops/task-state") return { tasks: [] };
      if (path === "/ops/fetcher-metrics?windowRuns=80") {
        return {
          latestRun: {
            dedupEvidence: {
              dedupAuditGate: {
                status: "blocked",
                blockers: ["provider_static_disagreement_needs_review"],
                warnings: ["current_run_primary_url_merges_present"],
                currentRunHighRiskReviewQueueCount: 0,
                carriedHighRiskReviewQueueCount: 0
              },
              providerStaticDisagreementExamples: [
                { title: "Studio", company: "Example", reviewStatus: "new" }
              ],
              providerStaticTitleCompanyCollisionExamples: [
                { title: "Studio", company: "Example", reviewStatus: "new" }
              ],
              reviewQueue: [
                { title: "Queued", company: "Example", reviewStatus: "new" }
              ]
            }
          }
        };
      }
      if (path === "/source-policy/recommendations") {
        return {
          recommendations: {
            pairs: [
              { reviewState: "new" },
              { reviewState: "reviewed" }
            ]
          },
          providerCoverageLinkBackfill: {
            reviewCandidates: [
              {
                apiEligible: true,
                recommendedApiPayload: { action: "apply_migration_identity_link" }
              }
            ],
            linkedCandidates: [
              {
                adminBackfillOwned: true,
                migrationLinkedBy: "admin_provider_link_backfill",
                migrationSourceIdentity: "static:alpha",
                staticSourceId: "static:alpha"
              }
            ],
            blockedCandidates: [
              {
                providerSourceId: "provider:blocked",
                staticSourceId: "static:blocked"
              }
            ]
          }
        };
      }
      if (path === "/registry/conflicts") {
        return {
          summary: { conflictCount: 1 },
          conflicts: [{ familyKey: "Studio", winner: { name: "Winner" }, rows: [] }]
        };
      }
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
    renderAdminOpsHistory() {},
    renderAdminOpsDedupLists() {},
    renderAdminSourcePolicyReview() {},
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
  renderScheduler.flush();
  controller.stopOpsHealthPolling();

  assert.equal(overviewBadge.textContent, "1");
  assert.equal(overviewBadge.attributes["data-badge-tone"], "critical");
  assert.equal(discoveryBadge.textContent, "4");
  assert.equal(discoveryBadge.attributes["data-badge-tone"], "warning");
  assert.equal(sourcePolicyBadge.textContent, "4");
  assert.equal(sourcePolicyBadge.attributes["data-badge-tone"], "critical");
  assert.equal(registryConflictsBadge.textContent, "1");
  assert.equal(registryConflictsBadge.attributes["data-badge-tone"], "warning");
  assert.equal(dedupBadge.textContent, "3");
  assert.equal(dedupBadge.attributes["data-badge-tone"], "critical");
});
