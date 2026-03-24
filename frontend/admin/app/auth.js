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
  setDiscoveryLogPlaceholder,
  clearOptimisticFetchRun,
  clearOptimisticDiscoveryRun,
  setManualSourceFeedback,
  setOpsPlaceholders,
  setBridgeStatusBadge,
  _renderUsersEmpty,
  startBridgeStatusWatch,
  _stopBridgeStatusWatch,
  scheduleOpsHealthPolling,
  _stopOpsHealthPolling,
  refreshOverview,
  loadLatestFetcherReport,
  loadDiscoveryData,
  loadDiscoveryConfig,
  loadOpsHealthData,
  loadSyncStatus,
  logAdminError,
  _showToast
}) {
  function initAdminPage() {
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
    scheduleOpsHealthPolling(900);
    refreshOverview().catch(err => {
      logAdminError("Failed to refresh admin overview", err);
    });
    loadLatestFetcherReport({ silent: true }).catch(err => {
      logAdminError("Failed to load jobs fetch report", err);
    });
    loadDiscoveryData().catch(err => {
      logAdminError("Failed to load discovery data", err);
    });
    loadDiscoveryConfig({ silent: true, forceForm: true }).catch(err => {
      logAdminError("Failed to load discovery config", err);
    });
    loadOpsHealthData().catch(err => {
      logAdminError("Failed to load ops health data", err);
    });
    loadSyncStatus({ silent: true, forceForm: true }).catch(err => {
      logAdminError("Failed to load sync status", err);
    });
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
    toAdminSessionViewModel
  };
}
