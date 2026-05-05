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
