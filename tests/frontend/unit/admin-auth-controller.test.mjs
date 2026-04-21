import test from "node:test";
import assert from "node:assert/strict";
import { createAdminAuthController } from "../../../frontend/admin/app/auth.js";
import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";
import { createAdminFetcherController } from "../../../frontend/admin/app/fetcher.js";
import { createAdminOpsController } from "../../../frontend/admin/app/ops.js";
import { applyAdminTaskProgress } from "../../../frontend/admin/app/progress-ui.js";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createAdminSyncController } from "../../../frontend/admin/app/sync.js";
import { createRestoreActiveRunWatches } from "../../../frontend/admin/app/live-task.js";
import { appendAdminLogRow } from "../../../frontend/admin/render.js";
import {
  FakeInputElement,
  createClassList,
  createDiscoveryControllerFixture,
  createElement,
  createFetcherControllerFixture,
  createRegistryControllerFixture,
  stubDateNow,
  stubScheduledTimers,
  withDom
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin auth controller initializes the composed admin view immediately", async () => {
  const dispatched = [];
  const toasts = [];
  const calls = [];
  const refs = {
    adminContentEl: createElement({ classList: createClassList(["hidden"]) }),
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["hidden"]) }),
    adminSyncStatusEl: createElement()
  };

  const controller = createAdminAuthController({
    refs,
    adminDispatch: {
      dispatch(action) {
        dispatched.push(action);
      }
    },
    adminActions: {
      UNLOCKED: "unlocked",
      LOCKED: "locked"
    },
    emitAdminStartupMetric() {},
    markAdminFirstInteractive() {},
    syncAdminBusyUi() {
      calls.push("syncAdminBusyUi");
    },
    syncDiscoveryLogDisclosure() {
      calls.push("syncDiscoveryLogDisclosure");
    },
    resetBusyFlags() {
      calls.push("resetBusyFlags");
    },
    setSourceFilter(value) {
      calls.push(`setSourceFilter:${value}`);
    },
    setSourceStatus(text) {
      refs.adminSourceStatusEl = { textContent: text };
    },
    setFetcherLogPlaceholder(message) {
      calls.push(`fetcherPlaceholder:${message}`);
    },
    attachToActiveFetchRun(runMeta) {
      calls.push(`attachToActiveFetchRun:${String(runMeta?.runId || "")}`);
    },
    setDiscoveryLogPlaceholder(message) {
      calls.push(`discoveryPlaceholder:${message}`);
    },
    clearOptimisticFetchRun() {
      calls.push("clearOptimisticFetchRun");
    },
    clearOptimisticDiscoveryRun() {
      calls.push("clearOptimisticDiscoveryRun");
    },
    setManualSourceFeedback(message) {
      calls.push(`manualFeedback:${message}`);
    },
    setOpsPlaceholders(message = "") {
      calls.push(`opsPlaceholder:${message}`);
    },
    setBridgeStatusBadge(stateValue, label) {
      calls.push(`bridge:${stateValue}:${label}`);
    },
    renderUsersEmpty(message) {
      calls.push(`renderEmpty:${message}`);
    },
    startBridgeStatusWatch() {
      calls.push("startBridgeStatusWatch");
    },
    stopBridgeStatusWatch() {
      calls.push("stopBridgeStatusWatch");
    },
    scheduleOpsHealthPolling(delay) {
      calls.push(`scheduleOpsHealthPolling:${delay}`);
    },
    stopOpsHealthPolling() {
      calls.push("stopOpsHealthPolling");
    },
    refreshOverview: async () => {
      calls.push("refreshOverview");
    },
    loadLatestFetcherReport: async options => {
      calls.push(`loadLatestFetcherReport:${String(Boolean(options?.silent))}`);
    },
    loadDiscoveryData: async () => {
      calls.push("loadDiscoveryData");
    },
    loadOpsHealthData: async () => {
      calls.push("loadOpsHealthData");
    },
    loadSyncStatus: async options => {
      calls.push(`loadSyncStatus:${String(Boolean(options?.silent))}:${String(Boolean(options?.forceForm))}`);
    },
    loadDiscoveryConfig: async options => {
      calls.push(`loadDiscoveryConfig:${String(Boolean(options?.silent))}:${String(Boolean(options?.forceForm))}`);
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    }
  });

  const initReady = controller.initAdminPage();
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.equal(initReady, true);
  assert.equal(refs.adminContentEl.classList.contains("hidden"), false);
  assert.equal(refs.adminBridgeStatusBadgeEl.classList.contains("hidden"), false);
  assert.deepEqual(dispatched.map(item => item.type), []);
  assert.ok(calls.includes("resetBusyFlags"));
  assert.ok(calls.includes("startBridgeStatusWatch"));
  assert.ok(calls.includes("refreshOverview"));
  assert.ok(calls.includes("loadDiscoveryData"));
  assert.ok(calls.includes("loadOpsHealthData"));
  assert.ok(calls.includes("loadDiscoveryConfig:true:true"));
  assert.ok(calls.includes("scheduleOpsHealthPolling:900"));
  assert.equal(refs.adminSyncStatusEl.textContent, "Loading sync status...");
  assert.equal(toasts.length, 0);
});


test("admin auth controller session view model tracks bridge badge state", async () => {
  const refs = {
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["online"]) }),
    adminContentEl: createElement()
  };
  const controller = createAdminAuthController({
    refs,
    emitAdminStartupMetric() {},
    markAdminFirstInteractive() {},
    syncAdminBusyUi() {},
    syncDiscoveryLogDisclosure() {},
    resetBusyFlags() {},
    setSourceFilter() {},
    setSourceStatus() {},
    setFetcherLogPlaceholder() {},
    setDiscoveryLogPlaceholder() {},
    clearOptimisticFetchRun() {},
    clearOptimisticDiscoveryRun() {},
    setManualSourceFeedback() {},
    setOpsPlaceholders() {},
    setBridgeStatusBadge() {},
    renderUsersEmpty() {},
    startBridgeStatusWatch() {},
    stopBridgeStatusWatch() {},
    scheduleOpsHealthPolling() {},
    stopOpsHealthPolling() {},
    refreshOverview: async () => {},
    loadLatestFetcherReport: async () => {},
    loadDiscoveryData: async () => {},
    loadOpsHealthData: async () => {},
    loadSyncStatus: async () => {},
    loadDiscoveryConfig: async () => {},
    logAdminError() {},
    showToast() {}
  });

  assert.deepEqual(controller.toAdminSessionViewModel(), {
    isUnlocked: true,
    apiReady: true,
    bridgeStatus: "online"
  });
});

