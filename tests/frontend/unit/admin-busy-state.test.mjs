import test from "node:test";
import assert from "node:assert/strict";

import { syncAdminBusyUi, toAdminViewState } from "../../../frontend/admin/app/busy-state.js";
import { FETCHER_PRESET_META } from "../../../frontend/admin/app/fetcher.js";

function createClassList(initial = []) {
  const values = new Set(initial);
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
    toggle(token, force) {
      if (force === true) {
        values.add(token);
        return true;
      }
      if (force === false) {
        values.delete(token);
        return false;
      }
      if (values.has(token)) {
        values.delete(token);
        return false;
      }
      values.add(token);
      return true;
    },
    contains(token) {
      return values.has(token);
    }
  };
}

function createElement(text = "") {
  return {
    textContent: text,
    disabled: false,
    dataset: {},
    attributes: {},
    classList: createClassList(),
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    }
  };
}

function buildRefs() {
  return {
    adminRunFetcherBtnEl: createElement("Run Jobs Fetcher"),
    adminRunFetcherIncrementalBtnEl: createElement("Run Incremental"),
    adminRunFetcherUncappedBtnEl: createElement("Uncapped Run"),
    adminRunFetcherForceBtnEl: createElement("Force Ignore Circuit"),
    adminRetryFailedBtnEl: createElement("Retry Failed Sources"),
    adminRefreshReportBtnEl: createElement("Load Latest Report"),
    adminRefreshBtnEl: createElement("Refresh"),
    adminSyncTestBtnEl: createElement("Test"),
    adminSyncPullBtnEl: createElement("Pull"),
    adminSyncPushBtnEl: createElement("Push"),
    adminSyncEnabledEl: createElement(),
    adminRunDiscoveryBtnEl: createElement("Run Discovery"),
    adminRunDiscoveryUncappedBtnEl: createElement("Uncapped Run"),
    adminLoadDiscoveryBtnEl: createElement("Load Discovery Report"),
    adminApproveSourcesBtnEl: createElement("Approve"),
    adminRejectSourcesBtnEl: createElement("Reject"),
    adminDeleteSourcesBtnEl: createElement("Delete"),
    adminRestoreRejectedBtnEl: createElement("Restore"),
    adminDemoteActiveBtnEl: createElement("Demote"),
    adminBulkBusyMessageEl: createElement(),
    adminAddManualSourceBtnEl: createElement("Add Source"),
    adminManualSourceUrlEl: createElement(),
    adminSourceFilterBtnEls: [],
    adminFetcherProgressBadgeEl: createElement(),
    adminDiscoveryProgressBadgeEl: createElement(),
    adminOpsProgressBadgeEl: createElement(),
    adminContentEl: createElement()
  };
}

test("syncAdminBusyUi disables uncapped fetcher and discovery buttons while their tasks are running", () => {
  const busyState = {
    fetcherRun: false,
    fetcherWatch: true,
    fetcherReportLoad: false,
    liveFetchRunning: true,
    discoveryRun: false,
    discoveryWatch: true,
    discoveryLoad: false,
    discoveryWrite: false,
    manualAdd: false,
    manualCheck: false,
    liveDiscoveryRunning: true,
    syncRun: false,
    liveSyncRunning: false,
    opsLoad: false,
    livePipelineRunning: false
  };
  const refs = buildRefs();

  syncAdminBusyUi({
    busyState,
    viewState: toAdminViewState(busyState, { isUnlocked: true }),
    fetcherPresetMeta: FETCHER_PRESET_META,
    refs,
    onSyncDiscoveryLogDisclosure() {}
  });

  assert.equal(refs.adminRunFetcherUncappedBtnEl.disabled, true);
  assert.equal(refs.adminRunFetcherUncappedBtnEl.attributes["aria-disabled"], "true");
  assert.equal(refs.adminRunFetcherUncappedBtnEl.textContent, FETCHER_PRESET_META.uncapped.busyLabel);
  assert.equal(refs.adminRunDiscoveryUncappedBtnEl.disabled, true);
  assert.equal(refs.adminRunDiscoveryUncappedBtnEl.attributes["aria-disabled"], "true");
  assert.equal(refs.adminRunDiscoveryUncappedBtnEl.textContent, "Uncapped Discovery Running...");
});

test("syncAdminBusyUi restores uncapped buttons after tasks are idle", () => {
  const busyState = {
    fetcherRun: false,
    fetcherWatch: false,
    fetcherReportLoad: false,
    liveFetchRunning: false,
    discoveryRun: false,
    discoveryWatch: false,
    discoveryLoad: false,
    discoveryWrite: false,
    manualAdd: false,
    manualCheck: false,
    liveDiscoveryRunning: false,
    syncRun: false,
    liveSyncRunning: false,
    opsLoad: false,
    livePipelineRunning: false
  };
  const refs = buildRefs();

  refs.adminRunFetcherUncappedBtnEl.dataset.idleLabel = "Uncapped Run";
  refs.adminRunDiscoveryUncappedBtnEl.dataset.idleLabel = "Uncapped Run";
  refs.adminRunFetcherUncappedBtnEl.textContent = FETCHER_PRESET_META.uncapped.busyLabel;
  refs.adminRunDiscoveryUncappedBtnEl.textContent = "Uncapped Discovery Running...";

  syncAdminBusyUi({
    busyState,
    viewState: toAdminViewState(busyState, { isUnlocked: true }),
    fetcherPresetMeta: FETCHER_PRESET_META,
    refs,
    onSyncDiscoveryLogDisclosure() {}
  });

  assert.equal(refs.adminRunFetcherUncappedBtnEl.disabled, false);
  assert.equal(refs.adminRunFetcherUncappedBtnEl.attributes["aria-disabled"], "false");
  assert.equal(refs.adminRunFetcherUncappedBtnEl.textContent, "Uncapped Run");
  assert.equal(refs.adminRunDiscoveryUncappedBtnEl.disabled, false);
  assert.equal(refs.adminRunDiscoveryUncappedBtnEl.attributes["aria-disabled"], "false");
  assert.equal(refs.adminRunDiscoveryUncappedBtnEl.textContent, "Uncapped Run");
});

test("syncAdminBusyUi shows one registry busy message and preserves bulk labels", () => {
  const busyState = {
    fetcherRun: false,
    fetcherWatch: false,
    fetcherReportLoad: false,
    liveFetchRunning: false,
    discoveryRun: false,
    discoveryWatch: true,
    discoveryLoad: false,
    discoveryWrite: false,
    manualAdd: false,
    manualCheck: false,
    liveDiscoveryRunning: false,
    syncRun: false,
    liveSyncRunning: false,
    opsLoad: false,
    livePipelineRunning: false
  };
  const refs = buildRefs();
  refs.adminApproveSourcesBtnEl.textContent = "Approve Selected";
  refs.adminRejectSourcesBtnEl.textContent = "Reject Selected";
  refs.adminRestoreRejectedBtnEl.textContent = "Restore Selected";
  refs.adminDemoteActiveBtnEl.textContent = "Demote zero-jobs to Pending";
  refs.adminDeleteSourcesBtnEl.textContent = "Delete Selected";

  syncAdminBusyUi({
    busyState,
    viewState: toAdminViewState(busyState, { isUnlocked: true }),
    fetcherPresetMeta: FETCHER_PRESET_META,
    refs,
    onSyncDiscoveryLogDisclosure() {}
  });

  assert.equal(refs.adminApproveSourcesBtnEl.disabled, true);
  assert.equal(refs.adminRejectSourcesBtnEl.disabled, true);
  assert.equal(refs.adminRestoreRejectedBtnEl.disabled, true);
  assert.equal(refs.adminDemoteActiveBtnEl.disabled, true);
  assert.equal(refs.adminDeleteSourcesBtnEl.disabled, true);
  assert.equal(refs.adminApproveSourcesBtnEl.textContent, "Approve Selected");
  assert.equal(refs.adminRejectSourcesBtnEl.textContent, "Reject Selected");
  assert.equal(refs.adminRestoreRejectedBtnEl.textContent, "Restore Selected");
  assert.equal(refs.adminDemoteActiveBtnEl.textContent, "Demote zero-jobs to Pending");
  assert.equal(refs.adminDeleteSourcesBtnEl.textContent, "Delete Selected");
  assert.match(refs.adminBulkBusyMessageEl.textContent, /Source registry actions are paused/);
  assert.equal(refs.adminBulkBusyMessageEl.classList.contains("hidden"), false);
});

test("syncAdminBusyUi pauses source actions during standalone fetch", () => {
  const busyState = {
    fetcherRun: false,
    fetcherWatch: true,
    fetcherReportLoad: false,
    liveFetchRunning: true,
    discoveryRun: false,
    discoveryWatch: false,
    discoveryLoad: false,
    discoveryWrite: false,
    manualAdd: false,
    manualCheck: false,
    liveDiscoveryRunning: false,
    syncRun: false,
    liveSyncRunning: false,
    opsLoad: false,
    livePipelineRunning: false
  };
  const refs = buildRefs();
  refs.adminSourceFilterBtnEls = [createElement("Pending"), createElement("Active")];

  syncAdminBusyUi({
    busyState,
    viewState: toAdminViewState(busyState, { isUnlocked: true }),
    fetcherPresetMeta: FETCHER_PRESET_META,
    refs,
    onSyncDiscoveryLogDisclosure() {}
  });

  assert.equal(refs.adminApproveSourcesBtnEl.disabled, true);
  assert.equal(refs.adminAddManualSourceBtnEl.disabled, true);
  assert.equal(refs.adminManualSourceUrlEl.disabled, true);
  assert.equal(refs.adminSourceFilterBtnEls.every(btn => btn.disabled), true);
  assert.match(refs.adminBulkBusyMessageEl.textContent, /Source registry actions are paused/);
});

test("syncAdminBusyUi treats source-table loading as local loading, not a running Discovery task", () => {
  const busyState = {
    fetcherRun: false,
    fetcherWatch: false,
    fetcherReportLoad: false,
    liveFetchRunning: false,
    discoveryRun: false,
    discoveryWatch: false,
    discoveryLoad: true,
    discoveryWrite: false,
    manualAdd: false,
    manualCheck: false,
    liveDiscoveryRunning: false,
    syncRun: false,
    liveSyncRunning: false,
    opsLoad: false,
    livePipelineRunning: false
  };
  const refs = buildRefs();

  syncAdminBusyUi({
    busyState,
    viewState: toAdminViewState(busyState, { isUnlocked: true }),
    fetcherPresetMeta: FETCHER_PRESET_META,
    refs,
    onSyncDiscoveryLogDisclosure() {}
  });

  assert.equal(refs.adminRunDiscoveryBtnEl.disabled, false);
  assert.equal(refs.adminRunDiscoveryBtnEl.textContent, "Run Discovery");
  assert.equal(refs.adminAddManualSourceBtnEl.disabled, false);
  assert.equal(refs.adminAddManualSourceBtnEl.textContent, "Add Source");
  assert.equal(refs.adminApproveSourcesBtnEl.disabled, false);
  assert.equal(refs.adminBulkBusyMessageEl.classList.contains("hidden"), true);
  assert.equal(refs.adminLoadDiscoveryBtnEl.disabled, true);
  assert.equal(refs.adminLoadDiscoveryBtnEl.textContent, "Loading Discovery...");
  assert.equal(refs.adminDiscoveryProgressBadgeEl.textContent, "Loading Source Data");
});
