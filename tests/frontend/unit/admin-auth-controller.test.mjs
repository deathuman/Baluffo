import test from "node:test";
import assert from "node:assert/strict";
import { createAdminAuthController } from "../../../frontend/admin/app/auth.js";
import {
  createClassList,
  createElement
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin auth controller initializes the composed admin view immediately", async () => {
  const dispatched = [];
  const toasts = [];
  const calls = [];
  const perfCalls = [];
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
    markAdminStep(name, payload = {}) {
      perfCalls.push({ type: "mark", name, payload });
    },
    measureAdminStep(name, startMark, endMark, payload = {}) {
      perfCalls.push({ type: "measure", name, startMark, endMark, payload });
    },
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
    setOpsReadinessShell() {
      calls.push("opsReadinessShell");
    },
    setBridgeStatusBadge(stateValue, label) {
      calls.push(`bridge:${stateValue}:${label}`);
    },
    renderUsersEmpty(message) {
      calls.push(`renderEmpty:${message}`);
    },
    startBridgeStatusWatch(options = {}) {
      calls.push(`startBridgeStatusWatch:${String(Boolean(options?.deferInitial))}:${String(Number(options?.initialDelayMs || 0))}`);
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
    refreshOverview: async options => {
      calls.push(`refreshOverview:${String(options?.detail || "")}:${String(Boolean(options?.scheduleFullRefresh))}`);
    },
    loadLatestFetcherReport: async options => {
      calls.push(`loadLatestFetcherReport:${String(Boolean(options?.silent))}`);
    },
    loadDiscoveryData: async options => {
      calls.push(`loadDiscoveryData:${String(Boolean(options?.background))}:${String(Boolean(options?.suppressPlaceholders))}`);
    },
    loadOpsHealthData: async options => {
      calls.push(`loadOpsHealthData:${String(Boolean(options?.summary))}`);
    },
    loadSyncStatus: async options => {
      calls.push(`loadSyncStatus:${String(Boolean(options?.silent))}:${String(Boolean(options?.forceForm))}:${String(options?.includeLive !== false)}:${String(Boolean(options?.summary))}`);
    },
    loadAdminBootstrap: async () => {
      calls.push("loadAdminBootstrap");
      return { ok: true };
    },
    loadPipelineScheduleData: async options => {
      calls.push(`loadPipelineScheduleData:${String(Boolean(options?.force))}:${String(Boolean(options?.silent))}`);
    },
    loadOpsHistoryData: async options => {
      calls.push(`loadOpsHistoryData:${String(Boolean(options?.force))}:${String(Boolean(options?.silent))}`);
    },
    loadDiscoveryConfig: async options => {
      calls.push(`loadDiscoveryConfig:${String(Boolean(options?.silent))}:${String(Boolean(options?.forceForm))}`);
    },
    loadPipelineStatusFallbackData: async () => {
      calls.push("loadPipelineStatusFallbackData");
      return { active: false };
    },
    awaitLocalDataReady: async () => {
      calls.push("awaitLocalDataReady");
      return true;
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
  assert.equal(calls.some(item => item.startsWith("startBridgeStatusWatch:")), false);
  assert.equal(calls.includes("awaitLocalDataReady"), false);
  assert.equal(calls.includes("refreshOverview:summary:true"), false);
  assert.equal(calls.some(item => item.startsWith("loadDiscoveryData:")), false);
  assert.equal(calls.includes("loadOpsHealthData:true"), false);
  assert.ok(calls.includes("loadAdminBootstrap"));
  assert.ok(calls.includes("loadPipelineStatusFallbackData"));
  assert.equal(calls.includes("loadPipelineScheduleData:true:true"), false);
  assert.equal(calls.includes("loadOpsHistoryData:true:true"), false);
  assert.equal(calls.filter(item => item === "opsReadinessShell").length, 2);
  assert.equal(calls.includes("opsPlaceholder:Loading operations health..."), false);
  assert.equal(calls.some(item => item.startsWith("loadDiscoveryConfig:")), false);
  assert.equal(calls.some(item => item.startsWith("loadSyncStatus:")), false);
  assert.equal(calls.includes("scheduleOpsHealthPolling:900"), false);
  assert.equal(refs.adminSyncStatusEl.textContent, "");
  assert.ok(calls.includes("fetcherPlaceholder:Latest fetch report not loaded yet. Use Load latest fetch report to populate this panel."));
  assert.ok(calls.includes("discoveryPlaceholder:Latest discovery report not loaded yet. Use Load Discovery Report to populate this panel."));
  assert.equal(calls.some(item => /Loading latest jobs fetch report|Loading source discovery data/.test(item)), false);
  assert.equal(toasts.length, 0);
  const perfNames = perfCalls.map(item => `${item.type}:${item.name}`);
  for (const expected of [
    "mark:admin_auth_init_start",
    "mark:admin_bootstrap_fetch_start",
    "mark:admin_auth_init_end",
    "measure:admin_auth_init",
    "mark:admin_bootstrap_fetch_done",
    "measure:admin_bootstrap_fetch"
  ]) {
    assert.ok(perfNames.includes(expected), expected);
  }
  assert.ok(
    perfNames.indexOf("mark:admin_bootstrap_fetch_start") < perfNames.indexOf("mark:admin_bootstrap_fetch_done"),
    "bootstrap fetch should record start before completion"
  );
  assert.deepEqual(
    perfCalls.find(item => item.name === "admin_bootstrap_fetch")?.payload,
    { ok: true }
  );
});

test("admin bootstrap does not wait on the old local data readiness path", async () => {
  let resolveReady;
  const readyPromise = new Promise(resolve => {
    resolveReady = resolve;
  });
  const calls = [];
  let firstInteractiveCount = 0;
  const refs = {
    adminContentEl: createElement({ classList: createClassList(["hidden"]) }),
    adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["hidden"]) }),
    adminSyncStatusEl: createElement()
  };

  const controller = createAdminAuthController({
    refs,
    emitAdminStartupMetric() {},
    markAdminFirstInteractive() {
      firstInteractiveCount += 1;
    },
    markAdminStep() {},
    measureAdminStep() {},
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
    setOpsReadinessShell() {},
    setBridgeStatusBadge() {},
    startBridgeStatusWatch() {},
    refreshOverview: async () => {
      calls.push("refreshOverview");
    },
    loadDiscoveryData: async () => {},
    loadOpsHealthData: async () => {},
    loadSyncStatus: async () => {},
    loadDiscoveryConfig: async () => {},
    loadPipelineStatusFallbackData: async () => {
      calls.push("loadPipelineStatusFallbackData");
      return { active: false };
    },
    loadAdminBootstrap: async () => {
      calls.push("loadAdminBootstrap");
    },
    awaitLocalDataReady: () => readyPromise,
    logAdminError() {},
    showToast() {}
  });

  assert.equal(controller.initAdminPage(), true);
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(firstInteractiveCount, 1);
  assert.equal(refs.adminContentEl.classList.contains("hidden"), false);
  assert.equal(calls.includes("refreshOverview"), false);
  assert.ok(calls.includes("loadAdminBootstrap"));
  assert.ok(calls.includes("loadPipelineStatusFallbackData"));

  resolveReady(true);
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.equal(calls.includes("refreshOverview"), false);
});

test("admin auth does not schedule deferred diagnostics during startup", async () => {
  const originalSetTimeout = globalThis.setTimeout;
  const timers = [];
  globalThis.setTimeout = (callback, delayMs) => {
    timers.push({ callback, delayMs });
    return timers.length;
  };
  try {
    const calls = [];
    const refs = {
      adminContentEl: createElement({ classList: createClassList(["hidden"]) }),
      adminBridgeStatusBadgeEl: createElement({ classList: createClassList(["hidden"]) }),
      adminSyncStatusEl: createElement()
    };
    const controller = createAdminAuthController({
      refs,
      emitAdminStartupMetric() {},
      markAdminFirstInteractive() {},
      markAdminStep() {},
      measureAdminStep() {},
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
      setOpsReadinessShell() {},
      setBridgeStatusBadge() {},
      startBridgeStatusWatch() {},
      refreshOverview: async () => {},
      loadOpsHealthData: async options => {
        calls.push(`ops:${String(Boolean(options?.summary))}`);
      },
      loadPipelineStatusFallbackData: async () => {
        calls.push("pipelineStatus");
        return { active: false };
      },
      loadSyncStatus: async () => {},
      loadAdminBootstrap: async () => {
        calls.push("bootstrap");
      },
      awaitLocalDataReady: async () => true,
      loadPostInteractiveDiagnostics: async () => {
        calls.push("deferredDiagnostics");
      },
      logAdminError() {},
      showToast() {}
    });

    assert.equal(controller.initAdminPage(), true);
    await new Promise(resolve => originalSetTimeout(resolve, 0));
    assert.deepEqual(calls.sort(), ["bootstrap", "pipelineStatus"].sort());
    assert.equal(timers.length, 0);
  } finally {
    globalThis.setTimeout = originalSetTimeout;
  }
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
    loadPipelineStatusFallbackData: async () => ({}),
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
