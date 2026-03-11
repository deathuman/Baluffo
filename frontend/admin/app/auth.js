export function createAdminAuthController({
  state,
  refs,
  services,
  adminDispatch,
  adminActions,
  emitAdminStartupMetric,
  markAdminFirstInteractive,
  syncAdminBusyUi,
  syncDiscoveryLogDisclosure,
  resetBusyFlags,
  setSourceFilter,
  setSourceStatus,
  setFetcherLogPlaceholder,
  setDiscoveryLogPlaceholder,
  setManualSourceFeedback,
  setOpsPlaceholders,
  setBridgeStatusBadge,
  renderUsersEmpty,
  startBridgeStatusWatch,
  stopBridgeStatusWatch,
  scheduleOpsHealthPolling,
  stopOpsHealthPolling,
  refreshOverview,
  loadLatestFetcherReport,
  loadDiscoveryData,
  loadOpsHealthData,
  loadSyncStatus,
  getErrorMessage,
  logAdminError,
  showToast
}) {
  function stopAdminApiReadyPoll() {
    if (!state.adminApiReadyPollTimer) return;
    clearTimeout(state.adminApiReadyPollTimer);
    state.adminApiReadyPollTimer = null;
  }

  function scheduleAdminApiReadyPoll(delayMs = 600) {
    stopAdminApiReadyPoll();
    state.adminApiReadyPollTimer = setTimeout(() => {
      state.adminApiReadyPollTimer = null;
      if (state.adminPin) return;
      if (services.adminPageService.isAvailable()) {
        if (refs.adminUnlockBtnEl) {
          refs.adminUnlockBtnEl.disabled = false;
          refs.adminUnlockBtnEl.setAttribute("aria-disabled", "false");
          refs.adminUnlockBtnEl.title = "";
        }
        setSourceStatus("Enter admin PIN to access user overview.");
        return;
      }
      scheduleAdminApiReadyPoll(delayMs);
    }, Math.max(200, Number(delayMs) || 600));
  }

  function initAdminPage() {
    syncAdminBusyUi();
    syncDiscoveryLogDisclosure();
    setSourceFilter(state.activeSourceFilter);
    setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
    setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
    setManualSourceFeedback("Unlock admin to add a manual source.", "muted");
    setOpsPlaceholders();
    setBridgeStatusBadge("checking", "Bridge Checking");
    if (!services.adminPageService.isAvailable()) {
      emitAdminStartupMetric("admin_init_waiting");
      setSourceStatus("Local storage provider is starting...");
      if (refs.adminPinGateEl) refs.adminPinGateEl.classList.remove("hidden");
      if (refs.adminContentEl) refs.adminContentEl.classList.add("hidden");
      if (refs.adminLockBtnEl) refs.adminLockBtnEl.classList.add("hidden");
      if (refs.adminUnlockBtnEl) {
        refs.adminUnlockBtnEl.disabled = true;
        refs.adminUnlockBtnEl.setAttribute("aria-disabled", "true");
        refs.adminUnlockBtnEl.title = "Waiting for local storage provider to initialize.";
      }
      scheduleAdminApiReadyPoll();
      renderUsersEmpty("Admin view is starting. Wait a moment and unlock.");
      return false;
    }
    emitAdminStartupMetric("admin_init_ready");
    if (refs.adminUnlockBtnEl) {
      refs.adminUnlockBtnEl.disabled = false;
      refs.adminUnlockBtnEl.setAttribute("aria-disabled", "false");
      refs.adminUnlockBtnEl.title = "";
    }
    emitAdminStartupMetric("admin_pin_gate_ready");
    markAdminFirstInteractive("pin_gate_ready");
    stopAdminApiReadyPoll();
    return true;
  }

  function unlockAdmin() {
    if (!services.adminPageService.isAvailable()) {
      setSourceStatus("Local storage provider is starting...");
      showToast("Admin service is still starting. Try again in a moment.", "info");
      scheduleAdminApiReadyPoll();
      return;
    }
    const nextPin = String(refs.adminPinInputEl?.value || "").trim();
    if (!nextPin) {
      showToast("Enter admin PIN.", "error");
      return;
    }
    let pinValid = false;
    try {
      pinValid = Boolean(services.adminService.verifyAdminPin(nextPin));
    } catch (err) {
      logAdminError("Admin PIN verification unavailable", err);
      setSourceStatus("Local storage provider is starting...");
      showToast("Admin service is still starting. Try again in a moment.", "info");
      scheduleAdminApiReadyPoll();
      return;
    }
    if (!pinValid) {
      showToast("Invalid admin PIN.", "error");
      setSourceStatus("Invalid PIN. Access denied.");
      return;
    }

    state.adminPin = nextPin;
    state.syncConfigDirty = false;
    resetBusyFlags();
    adminDispatch.dispatch({ type: adminActions.UNLOCKED });
    setSourceStatus("Admin access granted.");
    if (refs.adminBridgeStatusBadgeEl) refs.adminBridgeStatusBadgeEl.classList.remove("hidden");
    if (refs.adminPinGateEl) refs.adminPinGateEl.classList.add("hidden");
    if (refs.adminContentEl) refs.adminContentEl.classList.remove("hidden");
    if (refs.adminLockBtnEl) refs.adminLockBtnEl.classList.remove("hidden");
    if (refs.adminPinInputEl) refs.adminPinInputEl.value = "";
    setFetcherLogPlaceholder("Loading latest jobs fetch report...");
    setDiscoveryLogPlaceholder("Loading source discovery data...");
    setManualSourceFeedback("", "muted");
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
    loadOpsHealthData().catch(err => {
      logAdminError("Failed to load ops health data", err);
    });
    loadSyncStatus({ silent: true, forceForm: true }).catch(err => {
      logAdminError("Failed to load sync status", err);
    });
  }

  function lockAdmin() {
    state.adminPin = "";
    state.syncConfigDirty = false;
    state.latestSyncStatusCache = null;
    resetBusyFlags();
    adminDispatch.dispatch({ type: adminActions.LOCKED });
    if (refs.adminPinGateEl) refs.adminPinGateEl.classList.remove("hidden");
    if (refs.adminContentEl) refs.adminContentEl.classList.add("hidden");
    if (refs.adminLockBtnEl) refs.adminLockBtnEl.classList.add("hidden");
    if (refs.adminBridgeStatusBadgeEl) refs.adminBridgeStatusBadgeEl.classList.add("hidden");
    if (refs.adminTotalsEl) refs.adminTotalsEl.innerHTML = "";
    if (refs.adminUsersListEl) refs.adminUsersListEl.innerHTML = "";
    stopBridgeStatusWatch();
    stopOpsHealthPolling();
    stopAdminApiReadyPoll();
    setBridgeStatusBadge("checking", "Bridge Locked");
    setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
    setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
    setManualSourceFeedback("Unlock admin to add a manual source.", "muted");
    setOpsPlaceholders();
    setSourceStatus("Enter admin PIN to access user overview.");
  }

  function toAdminSessionViewModel() {
    return {
      isUnlocked: Boolean(state.adminPin),
      apiReady: Boolean(services.adminPageService.isAvailable()),
      bridgeStatus: refs.adminBridgeStatusBadgeEl?.classList.contains("online")
        ? "online"
        : refs.adminBridgeStatusBadgeEl?.classList.contains("offline")
          ? "offline"
          : "checking"
    };
  }

  return {
    initAdminPage,
    unlockAdmin,
    lockAdmin,
    stopAdminApiReadyPoll,
    scheduleAdminApiReadyPoll,
    toAdminSessionViewModel
  };
}
