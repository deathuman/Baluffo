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
  loadAdminBootstrap,
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

  function initAdminPage() {
    markStep("admin_auth_init_start");
    syncAdminBusyUi();
    syncDiscoveryLogDisclosure();
    setSourceFilter("all");
    setFetcherLogPlaceholder("Latest fetch report not loaded yet. Use Load latest fetch report to populate this panel.");
    setDiscoveryLogPlaceholder("Latest discovery report not loaded yet. Use Load Discovery Report to populate this panel.");
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
    setBridgeStatusBadge("checking", "Bridge Checking");
    runInitialTask({
      start: "admin_bootstrap_fetch_start",
      end: "admin_bootstrap_fetch_done",
      measure: "admin_bootstrap_fetch",
      errorContext: "Failed to load admin bootstrap data",
      task: async () => {
        if (typeof loadAdminBootstrap !== "function") {
          await startInitialOverviewLoad();
          return null;
        }
        try {
          const payload = await loadAdminBootstrap();
          setBridgeStatusBadge("online", "Bridge Online");
          return payload;
        } catch (err) {
          setBridgeStatusBadge("offline", "Bridge Offline");
          setOpsPlaceholders(`Admin bootstrap unavailable: ${String(err?.message || err || "unknown error")}`);
          throw err;
        }
      }
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
