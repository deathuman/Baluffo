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
  setBridgeStatusBadge,
  _renderUsersEmpty,
  startBridgeStatusWatch,
  _stopBridgeStatusWatch,
  _stopOpsHealthPolling,
  refreshOverview,
  loadDiscoveryData,
  loadDiscoveryConfig,
  loadOpsHealthData,
  loadSyncStatus,
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
    task
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
      });
  }

  function initAdminPage() {
    markStep("admin_auth_init_start");
    syncAdminBusyUi();
    syncDiscoveryLogDisclosure();
    setSourceFilter("all");
    setFetcherLogPlaceholder("Loading latest jobs fetch report...");
    setDiscoveryLogPlaceholder("Loading source discovery data...");
    setManualSourceFeedback("", "muted");
    setOpsPlaceholders();
    setBridgeStatusBadge("checking", "Bridge Checking");
    emitAdminStartupMetric("admin_init_ready");
    emitAdminStartupMetric("admin_ready");
    markAdminFirstInteractive("ready");
    clearOptimisticFetchRun();
    clearOptimisticDiscoveryRun();
    resetBusyFlags();
    if (refs.adminBridgeStatusBadgeEl) refs.adminBridgeStatusBadgeEl.classList.remove("hidden");
    if (refs.adminContentEl) refs.adminContentEl.classList.remove("hidden");
    setSourceStatus("Loading admin overview...");
    setOpsPlaceholders("Loading operations health...");
    if (refs.adminSyncStatusEl) refs.adminSyncStatusEl.textContent = "Loading sync status...";
    startBridgeStatusWatch();
    runInitialTask({
      start: "admin_overview_fetch_start",
      end: "admin_overview_fetch_done",
      measure: "admin_overview_fetch",
      errorContext: "Failed to refresh admin overview",
      task: () => refreshOverview()
    });
    runInitialTask({
      start: "admin_discovery_fetch_start",
      end: "admin_discovery_fetch_done",
      measure: "admin_discovery_fetch",
      errorContext: "Failed to load discovery data",
      task: () => loadDiscoveryData()
    });
    runInitialTask({
      start: "admin_discovery_config_fetch_start",
      end: "admin_discovery_config_fetch_done",
      measure: "admin_discovery_config_fetch",
      errorContext: "Failed to load discovery config",
      task: () => loadDiscoveryConfig({ silent: true, forceForm: true })
    });
    runInitialTask({
      start: "admin_ops_health_fetch_start",
      end: "admin_ops_health_fetch_done",
      measure: "admin_ops_health_fetch",
      errorContext: "Failed to load ops health data",
      task: () => loadOpsHealthData()
    });
    runInitialTask({
      start: "admin_sync_fetch_start",
      end: "admin_sync_fetch_done",
      measure: "admin_sync_fetch",
      errorContext: "Failed to load sync status",
      task: () => loadSyncStatus({ silent: true, forceForm: true })
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
