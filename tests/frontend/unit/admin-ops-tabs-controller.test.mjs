import test from "node:test";
import assert from "node:assert/strict";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import {
  createDeferredRenderScheduler,
  createElement,
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

test("admin ops tabs switch overview discovery source-policy and dedup panels locally", () => {
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
  const overviewBtn = createTabButton("overview");
  const discoveryBtn = createTabButton("discovery");
  const sourcePolicyBtn = createTabButton("source-policy");
  const dedupBtn = createTabButton("dedup");
  const refs = {
    adminOpsTabBtnEls: [overviewBtn, discoveryBtn, sourcePolicyBtn, dedupBtn],
    adminOpsTabOverviewEl: createElement(),
    adminOpsTabDiscoveryEl: createElement(),
    adminOpsTabSourcePolicyEl: createElement(),
    adminOpsTabDedupEl: createElement()
  };
  const renderScheduler = createDeferredRenderScheduler();

  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async () => ({}),
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

  assert.equal(overviewBtn.attributes["aria-selected"], "true");
  assert.equal(discoveryBtn.attributes["aria-selected"], "false");
  assert.equal(sourcePolicyBtn.attributes["aria-selected"], "false");
  assert.equal(refs.adminOpsTabOverviewEl.hidden, false);
  assert.equal(refs.adminOpsTabDiscoveryEl.hidden, true);
  assert.equal(refs.adminOpsTabSourcePolicyEl.hidden, true);
  assert.equal(refs.adminOpsTabDedupEl.hidden, true);

  discoveryBtn.click();

  assert.equal(state.adminOpsActiveTab, "discovery");
  assert.equal(overviewBtn.attributes["aria-selected"], "false");
  assert.equal(discoveryBtn.attributes["aria-selected"], "true");
  assert.equal(refs.adminOpsTabOverviewEl.hidden, true);
  assert.equal(refs.adminOpsTabDiscoveryEl.hidden, false);
  assert.equal(refs.adminOpsTabSourcePolicyEl.hidden, true);
  assert.equal(refs.adminOpsTabDedupEl.hidden, true);

  sourcePolicyBtn.click();

  assert.equal(state.adminOpsActiveTab, "source-policy");
  assert.equal(overviewBtn.attributes["aria-selected"], "false");
  assert.equal(discoveryBtn.attributes["aria-selected"], "false");
  assert.equal(sourcePolicyBtn.attributes["aria-selected"], "true");
  assert.equal(refs.adminOpsTabOverviewEl.hidden, true);
  assert.equal(refs.adminOpsTabDiscoveryEl.hidden, true);
  assert.equal(refs.adminOpsTabSourcePolicyEl.hidden, false);
  assert.equal(refs.adminOpsTabDedupEl.hidden, true);

  controller.selectOpsTab("dedup");

  assert.equal(dedupBtn.attributes["aria-selected"], "true");
  assert.equal(refs.adminOpsTabDedupEl.hidden, false);
  assert.equal(refs.adminOpsTabSourcePolicyEl.hidden, true);
});

test("admin ops discovery tab loads and renders candidate review details", async () => {
  const state = {
    latestOpsHealthCache: null,
    latestDiscoveryReportCache: {
      summaryView: true,
      summary: { queuedCandidateCount: 46 }
    },
    adminBusyState: {
      opsLoad: false,
      liveFetchRunning: false,
      liveDiscoveryRunning: false,
      liveSyncRunning: false,
      livePipelineRunning: false
    }
  };
  const discoveryBtn = createTabButton("discovery");
  const refs = {
    adminOpsTabBtnEls: [createTabButton("overview"), discoveryBtn],
    adminOpsTabOverviewEl: createElement(),
    adminOpsTabDiscoveryEl: createElement(),
    adminDiscoveryReviewEl: createElement(),
    adminOpsTabBadgeEls: []
  };
  const calls = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async () => ({}),
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
    loadSyncStatus: async () => {},
    loadLatestDiscoveryReport: async () => {
      calls.push("loadLatestDiscoveryReport");
      return {
        candidateReview: {
          totalCandidates: 1,
          recommendationCounts: { promote_candidate: 1 },
          topCandidates: [
            {
              name: "Review Studio",
              adapter: "greenhouse",
              jobsFound: 4,
              rankScore: 90,
              promotionRecommendation: "promote_candidate"
            }
          ]
        }
      };
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
    idlePollIntervalMs: 1000
  });

  await controller.selectOpsTab("discovery");

  assert.deepEqual(calls, ["loadLatestDiscoveryReport"]);
  assert.match(refs.adminDiscoveryReviewEl.innerHTML, /Discovery Review Quality/);
  assert.match(refs.adminDiscoveryReviewEl.innerHTML, /Review Studio/);
  assert.doesNotMatch(refs.adminDiscoveryReviewEl.innerHTML, /No discovery review evidence loaded yet/);
});

test("admin ops dedup tab loads dedup list details on view", async () => {
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
    adminOpsTabBtnEls: [createTabButton("overview"), createTabButton("dedup")],
    adminOpsTabOverviewEl: createElement(),
    adminOpsTabDedupEl: createElement(),
    adminOpsDedupListsEl: createElement(),
    adminOpsTabBadgeEls: []
  };
  const calls = [];
  const controller = createAdminOpsController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/ops/fetcher-metrics?windowRuns=80") {
        return {
          latestRun: {
            dedupEvidence: {
              dedupAuditGate: { status: "blocked", blockers: ["provider_static_disagreement_needs_review"] }
            }
          }
        };
      }
      return {};
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
    renderAdminOpsDedupLists(el, payload) {
      calls.push("renderAdminOpsDedupLists");
      el.innerHTML = payload?.latestRun?.dedupEvidence?.dedupAuditGate?.status || "";
    },
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

  await controller.selectOpsTab("dedup");

  assert.ok(calls.includes("/ops/fetcher-metrics?windowRuns=80"));
  assert.ok(calls.includes("renderAdminOpsDedupLists"));
  assert.equal(refs.adminOpsDedupListsEl.innerHTML, "blocked");
});
