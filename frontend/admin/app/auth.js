export function createAdminAuthController({
  refs,
  emitAdminStartupMetric,
  markAdminFirstInteractive,
  syncAdminBusyUi,
  syncDiscoveryLogDisclosure,
  resetBusyFlags,
  setSourceFilter,
  setSourceStatus,
  setFetcherLogPlaceholder,
  restoreActiveRunWatches,
  setDiscoveryLogPlaceholder,
  clearOptimisticFetchRun,
  clearOptimisticDiscoveryRun,
  setManualSourceFeedback,
  setOpsPlaceholders,
  setOpsReadinessShell = setOpsPlaceholders,
  setBridgeStatusBadge,
  _renderUsersEmpty,
  startBridgeStatusWatch,
  _stopBridgeStatusWatch,
  _stopOpsHealthPolling,
  refreshOverview,
  loadDiscoveryConfig,
  loadOpsHealthData,
  loadSyncStatus,
  loadPostInteractiveDiagnostics,
  awaitLocalDataReady = async () => true,
  markAdminStep,
  measureAdminStep,
  logAdminError,
  _showToast
}) {
  function markStep(name, payload) {
    if (typeof markAdminStep === "function") {
      markAdminStep(name, payload);
    }
  }

  function measureStep(name, startMark, endMark, payload) {
    if (typeof measureAdminStep === "function") {
      measureAdminStep(name, startMark, endMark, payload);
    }
  }

  function runInitialTask({
    start,
    end,
    measure,
    errorContext,
    task,
    afterSettled = null
  }) {
    markStep(start);
    Promise.resolve()
      .then(task)
      .then(() => {
        markStep(end, { ok: true });
        measureStep(measure, start, end, { ok: true });
      })
      .catch(err => {
        markStep(end, { ok: false, error: String(err?.message || err || "unknown error") });
        measureStep(measure, start, end, { ok: false });
        logAdminError(errorContext, err);
      })
      .finally(() => {
        if (typeof afterSettled === "function") {
          afterSettled();
        }
      });
  }

  function startInitialOverviewLoad() {
    Promise.resolve()
      .then(() => awaitLocalDataReady())
      .then(ready => {
        if (ready === false) {
          throw new Error("Local data API unavailable.");
        }
        runInitialTask({
          start: "admin_overview_fetch_start",
          end: "admin_overview_fetch_done",
          measure: "admin_overview_fetch",
          errorContext: "Failed to refresh admin overview",
          task: () => refreshOverview({ detail: "summary", scheduleFullRefresh: true })
        });
      })
      .catch(err => {
        logAdminError("Failed to prepare admin overview", err);
      });
  }

  let postInteractiveDiagnosticsScheduled = false;

  function schedulePostInteractiveDiagnostics() {
    if (postInteractiveDiagnosticsScheduled || typeof loadPostInteractiveDiagnostics !== "function") return;
    postInteractiveDiagnosticsScheduled = true;
    const run = () => {
      Promise.resolve()
        .then(() => loadPostInteractiveDiagnostics())
        .catch(err => {
          logAdminError("Failed to load deferred admin diagnostics", err);
        });
    };
    const delayMs = 5000;
    if (typeof globalThis.setTimeout === "function") {
      globalThis.setTimeout(run, delayMs);
      return;
    }
    run();
  }

  function initAdminPage() {
    markStep("admin_auth_init_start");
    syncAdminBusyUi();
    syncDiscoveryLogDisclosure();
    setSourceFilter("all");
    setFetcherLogPlaceholder("");
    setDiscoveryLogPlaceholder("");
    setManualSourceFeedback("", "muted");
    setOpsReadinessShell();
    setBridgeStatusBadge("checking", "Bridge Checking");
    emitAdminStartupMetric("admin_init_ready");
    emitAdminStartupMetric("admin_ready");
    markAdminFirstInteractive("ready");
    clearOptimisticFetchRun();
    clearOptimisticDiscoveryRun();
    resetBusyFlags();
    if (refs.adminBridgeStatusBadgeEl) refs.adminBridgeStatusBadgeEl.classList.remove("hidden");
    if (refs.adminContentEl) refs.adminContentEl.classList.remove("hidden");
    setSourceStatus("");
    setOpsReadinessShell();
    if (refs.adminSyncStatusEl) refs.adminSyncStatusEl.textContent = "";
    startBridgeStatusWatch({ deferInitial: true, initialDelayMs: 1500 });
    startInitialOverviewLoad();
    runInitialTask({
      start: "admin_ops_health_fetch_start",
      end: "admin_ops_health_fetch_done",
      measure: "admin_ops_health_fetch",
      errorContext: "Failed to load ops health data",
      task: () => loadOpsHealthData({ summary: true }),
      afterSettled: schedulePostInteractiveDiagnostics
    });
    runInitialTask({
      start: "admin_sync_fetch_start",
      end: "admin_sync_fetch_done",
      measure: "admin_sync_fetch",
      errorContext: "Failed to load sync status",
      task: () => loadSyncStatus({ silent: true, forceForm: true, includeLive: false, summary: true })
    });
    markStep("admin_auth_init_end");
    measureStep("admin_auth_init", "admin_auth_init_start", "admin_auth_init_end");
    return true;
  }

  function toAdminSessionViewModel() {
    return {
      isUnlocked: true,
      apiReady: true,
      bridgeStatus: refs.adminBridgeStatusBadgeEl?.classList.contains("online")
        ? "online"
        : refs.adminBridgeStatusBadgeEl?.classList.contains("offline")
          ? "offline"
          : "checking"
    };
  }

  return {
    initAdminPage,
    restoreActiveRunWatches,
    toAdminSessionViewModel
  };
}
