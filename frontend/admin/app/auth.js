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
  attachToActiveFetchRun,
  restartFetcherCompletionWatch,
  getRestorableFetcherRunMeta,
  setDiscoveryLogPlaceholder,
  clearOptimisticFetchRun,
  clearOptimisticDiscoveryRun,
  loadLatestDiscoveryReport,
  attachToActiveDiscoveryRun,
  restartDiscoveryCompletionWatch,
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
  let restoreActiveRunWatchesPromise = null;

  function getFetchRestoreMeta(fetchReport) {
    if (typeof getRestorableFetcherRunMeta === "function") {
      const meta = getRestorableFetcherRunMeta(fetchReport);
      if (meta) return meta;
    }
    const active = Boolean(
      fetchReport
      && !String(fetchReport.finishedAt || "").trim()
      && Boolean(fetchReport?.taskProgress?.active)
    );
    if (!active) return null;
    return {
      runId: fetchReport.runId,
      startedAt: fetchReport.startedAt
    };
  }

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
    loadLatestFetcherReport({ silent: true })
      .then(report => {
        const fetchMeta = getFetchRestoreMeta(report);
        if (fetchMeta && typeof attachToActiveFetchRun === "function") {
          attachToActiveFetchRun(fetchMeta);
        }
      })
      .catch(err => {
        logAdminError("Failed to load jobs fetch report", err);
      });
    if (typeof loadLatestDiscoveryReport === "function") {
      loadLatestDiscoveryReport({ silent: true })
        .then(report => {
          const active = Boolean(
            report
            && !String(report.finishedAt || "").trim()
            && Boolean(report?.taskProgress?.active)
          );
          if (active && typeof attachToActiveDiscoveryRun === "function") {
            attachToActiveDiscoveryRun({
              runId: report.runId,
              startedAt: report.startedAt
            });
          }
        })
        .catch(err => {
          logAdminError("Failed to load discovery report", err);
        });
    }
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

  async function restoreActiveRunWatches() {
    if (restoreActiveRunWatchesPromise) {
      return restoreActiveRunWatchesPromise;
    }

    restoreActiveRunWatchesPromise = (async () => {
      const fetchReport = await loadLatestFetcherReport({ silent: true }).catch(() => null);
      const fetchRestoreMeta = getFetchRestoreMeta(fetchReport);
      if (fetchRestoreMeta && typeof restartFetcherCompletionWatch === "function") {
        restartFetcherCompletionWatch(fetchRestoreMeta);
      }

      const discoveryReport = await loadLatestDiscoveryReport({ silent: true }).catch(() => null);
      const discoveryActive = Boolean(
        discoveryReport
        && !String(discoveryReport.finishedAt || "").trim()
        && Boolean(discoveryReport?.taskProgress?.active)
      );
      if (discoveryActive && typeof restartDiscoveryCompletionWatch === "function") {
        restartDiscoveryCompletionWatch({
          runId: discoveryReport.runId,
          startedAt: discoveryReport.startedAt
        });
      }
    })();

    try {
      return await restoreActiveRunWatchesPromise;
    } finally {
      restoreActiveRunWatchesPromise = null;
    }
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
