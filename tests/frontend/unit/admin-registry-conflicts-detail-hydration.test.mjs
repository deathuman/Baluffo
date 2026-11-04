import test from "node:test";
import assert from "node:assert/strict";

import { createOverviewHydration } from "../../../frontend/admin/app/ops/hydrate-overview.js";

function buildHydration(getBridge) {
  const state = { adminOpsActiveTab: "registry-conflicts" };
  const refs = { adminRegistryConflictsReviewEl: { innerHTML: "" } };
  const renders = [];
  const hydration = createOverviewHydration({
    state,
    refs,
    getBridge,
    getErrorMessage: err => String(err?.message || err || "unknown error"),
    showToast: () => {},
    escapeHtml: value => String(value),
    getObjectValue: value => (
      value && typeof value === "object" && !Array.isArray(value) ? value : {}
    ),
    isLoadedDiscoveryReport: () => false,
    isLoadedDedupPayload: () => false,
    maybeUnrefTimer: timer => {
      clearTimeout(timer);
      return timer;
    },
    loadLatestDiscoveryReport: async () => ({}),
    getCachedSourcePolicyPayload: () => ({}),
    getCachedRegistryConflictsPayload: () => ({}),
    renderDeferredOverviewDetails: () => {},
    renderSourcePolicyReviewQueue: () => {},
    renderRegistryConflictsQueue: payload => renders.push(payload),
    renderDiscoveryReviewPanel: () => {},
    renderAdminOpsDedupListsImpl: () => {},
    buildFetcherMetricsPayload: () => ({}),
    handleDedupReviewAction: () => {},
    rerenderOpsTabBadges: () => {},
    currentRenderToken: () => 1,
    isStaleRenderToken: () => false,
    OPS_HISTORY_DETAIL_PATH: "/ops/history?limit=80",
    OPS_FETCHER_METRICS_DETAIL_PATH: "/ops/fetcher-metrics?windowRuns=80",
    OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH: "/ops/discovery-audit-artifacts",
    OPS_TASK_FAILURE_ATTEMPTS_PATH: "/ops/task-failure-attempts",
    OPS_PERFORMANCE_PROFILE_PATH: "/ops/performance-profile",
    SOURCE_POLICY_DETAIL_PATH: "/source-policy/recommendations",
    REGISTRY_CONFLICTS_DETAIL_PATH: "/registry/conflicts"
  });
  return { hydration, state, refs, renders };
}

test("registry conflicts detail shows a building state instead of a timeout error", async () => {
  const calls = [];
  const { hydration, state, refs, renders } = buildHydration(async (path, options) => {
    calls.push({ path, options });
    return {
      detailStatus: "building",
      fullCacheStatus: "building",
      refreshInFlight: true,
      summaryView: true,
      summary: { conflictCount: 394 },
      conflicts: []
    };
  });

  const payload = await hydration.loadRegistryConflictsDetail();

  assert.equal(payload.detailStatus, "building");
  assert.equal(calls.length, 1);
  assert.equal(calls[0].options.timeoutMs, 60000);
  assert.equal(state.registryConflictsDetailLoaded, false);
  assert.equal(state.registryConflictsDetailRetryCount, 1);
  assert.match(refs.adminRegistryConflictsReviewEl.innerHTML, /Building the registry conflict list/);
  assert.equal(renders.length, 0);
});

test("registry conflicts detail renders stale cards and schedules a refresh", async () => {
  const { hydration, state, renders } = buildHydration(async () => ({
    fullCacheStatus: "stale",
    fullCacheAgeSeconds: 120,
    refreshInFlight: true,
    summary: { conflictCount: 1 },
    conflicts: [{ familyKey: "family-a" }]
  }));

  await hydration.loadRegistryConflictsDetail();

  assert.equal(state.registryConflictsDetailLoaded, true);
  assert.equal(state.registryConflictsStaleRefreshCount, 1);
  assert.equal(renders.length, 1);
  assert.equal(renders[0].fullCacheStatus, "stale");
});
