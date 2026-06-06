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
    loadDiscoveryConfig: async options => {
      calls.push(`loadDiscoveryConfig:${String(Boolean(options?.silent))}:${String(Boolean(options?.forceForm))}`);
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
  assert.ok(calls.includes("startBridgeStatusWatch:true:1500"));
  assert.ok(calls.includes("awaitLocalDataReady"));
  assert.ok(calls.includes("refreshOverview:summary:true"));
  assert.equal(calls.some(item => item.startsWith("loadDiscoveryData:")), false);
  assert.ok(calls.includes("loadOpsHealthData:true"));
  assert.equal(calls.filter(item => item === "opsReadinessShell").length, 2);
  assert.equal(calls.includes("opsPlaceholder:Loading operations health..."), false);
  assert.equal(calls.some(item => item.startsWith("loadDiscoveryConfig:")), false);
  assert.ok(calls.includes("loadSyncStatus:true:true:false:true"));
  assert.equal(calls.includes("scheduleOpsHealthPolling:900"), false);
  assert.equal(refs.adminSyncStatusEl.textContent, "");
  assert.ok(calls.includes("fetcherPlaceholder:"));
  assert.ok(calls.includes("discoveryPlaceholder:"));
  assert.equal(calls.some(item => /Loading latest jobs fetch report|Loading source discovery data/.test(item)), false);
  assert.equal(toasts.length, 0);
  const perfNames = perfCalls.map(item => `${item.type}:${item.name}`);
  for (const expected of [
    "mark:admin_auth_init_start",
    "mark:admin_ops_health_fetch_start",
    "mark:admin_sync_fetch_start",
    "mark:admin_auth_init_end",
    "measure:admin_auth_init",
    "mark:admin_overview_fetch_start",
    "mark:admin_overview_fetch_done",
    "measure:admin_overview_fetch"
  ]) {
    assert.ok(perfNames.includes(expected), expected);
  }
  assert.ok(
    perfNames.indexOf("measure:admin_auth_init") < perfNames.indexOf("mark:admin_overview_fetch_start"),
    "overview fetch should wait until the initial Admin shell is measured"
  );
  assert.deepEqual(
    perfCalls.find(item => item.name === "admin_overview_fetch")?.payload,
    { ok: true }
  );
});

test("admin overview waits for local data readiness without blocking the shell", async () => {
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
    refreshOverview: async options => {
      calls.push(`refreshOverview:${String(options?.detail || "")}:${String(Boolean(options?.scheduleFullRefresh))}`);
    },
    loadDiscoveryData: async () => {},
    loadOpsHealthData: async () => {},
    loadSyncStatus: async () => {},
    loadDiscoveryConfig: async () => {},
    awaitLocalDataReady: () => readyPromise,
    logAdminError() {},
    showToast() {}
  });

  assert.equal(controller.initAdminPage(), true);
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(firstInteractiveCount, 1);
  assert.equal(refs.adminContentEl.classList.contains("hidden"), false);
  assert.equal(calls.includes("refreshOverview:summary:true"), false);

  resolveReady(true);
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.ok(calls.includes("refreshOverview:summary:true"));
});

test("admin auth schedules full diagnostics after first summary render", async () => {
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
      loadSyncStatus: async () => {},
      awaitLocalDataReady: async () => true,
      loadPostInteractiveDiagnostics: async () => {
        calls.push("deferredDiagnostics");
      },
      logAdminError() {},
      showToast() {}
    });

    assert.equal(controller.initAdminPage(), true);
    await new Promise(resolve => originalSetTimeout(resolve, 0));
    assert.deepEqual(calls, ["ops:true"]);
    assert.equal(timers.length, 1);
    assert.equal(timers[0].delayMs, 1800);

    timers[0].callback();
    await Promise.resolve();
    await Promise.resolve();
    assert.deepEqual(calls, ["ops:true", "deferredDiagnostics"]);
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
