export function formatBytes(bytes) {
  const value = Number(bytes) || 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

export function createAdminOpsController({
  state,
  refs,
  getBridge,
  postBridge,
  normalizeOpsRuns,
  getOpsPollIntervalMs,
  renderAdminOpsAlerts,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsTrends,
  renderAdminOpsHistory,
  loadSyncStatus,
  setBusyFlag,
  showToast,
  getErrorMessage,
  adminDispatch,
  adminActions,
  escapeHtml,
  bridgeStatusPollIntervalMs,
  idlePollIntervalMs
}) {
  function setOpsPlaceholders(message = "Unlock admin to view operations health.") {
    if (refs.adminSyncStatusEl) {
      refs.adminSyncStatusEl.textContent = message;
    }
    if (refs.adminSyncConfigHintEl) {
      refs.adminSyncConfigHintEl.textContent = "GitHub App credentials are packaged with the app.";
    }
    if (refs.adminOpsAlertsEl) {
      refs.adminOpsAlertsEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
    }
    if (refs.adminOpsKpisEl) refs.adminOpsKpisEl.innerHTML = "";
    if (refs.adminOpsScheduleEl) refs.adminOpsScheduleEl.innerHTML = "";
    if (refs.adminOpsFetcherMetricsEl) refs.adminOpsFetcherMetricsEl.innerHTML = "";
    if (refs.adminOpsTrendsEl) refs.adminOpsTrendsEl.textContent = message;
    if (refs.adminOpsHistoryEl) {
      refs.adminOpsHistoryEl.innerHTML = `<div class="no-results">${escapeHtml(message)}</div>`;
    }
  }

  function stopOpsHealthPolling() {
    if (!state.opsHealthPollTimer) return;
    clearTimeout(state.opsHealthPollTimer);
    state.opsHealthPollTimer = null;
  }

  function scheduleOpsHealthPolling(delayMs) {
    stopOpsHealthPolling();
    if (!state.adminPin) return;
    const waitMs = Math.max(600, Number(delayMs) || 10000);
    state.opsHealthPollTimer = setTimeout(() => {
      loadOpsHealthData({ fromPoll: true }).catch(() => {});
    }, waitMs);
  }

  async function loadOpsHealthData(options = {}) {
    if (!state.adminPin) return;
    if (state.adminBusyState.opsLoad) {
      if (options?.fromPoll) scheduleOpsHealthPolling(idlePollIntervalMs);
      return;
    }
    setBusyFlag("opsLoad", true);
    const showLoadingState = !options?.fromPoll && !state.latestOpsHealthCache;
    if (showLoadingState && refs.adminOpsTrendsEl) refs.adminOpsTrendsEl.textContent = "Loading operations health...";
    try {
      const [health, historyPayload] = await Promise.all([
        getBridge("/ops/health"),
        getBridge("/ops/history?limit=80")
      ]);
      let fetcherMetrics = null;
      try {
        fetcherMetrics = await getBridge("/ops/fetcher-metrics?windowRuns=80");
      } catch {
        fetcherMetrics = null;
      }
      state.latestOpsHealthCache = health || null;
      const runModel = normalizeOpsRuns(historyPayload?.runs || [], Date.now());
      const liveTypes = new Set(Array.isArray(runModel?.liveTypes) ? runModel.liveTypes : []);
      setBusyFlag("liveFetchRunning", liveTypes.has("fetch"));
      setBusyFlag("liveDiscoveryRunning", liveTypes.has("discovery"));
      setBusyFlag("liveSyncRunning", liveTypes.has("sync"));
      setBusyFlag("livePipelineRunning", liveTypes.has("pipeline"));

      renderAdminOpsAlerts(refs.adminOpsAlertsEl, health?.alerts || [], {
        onAck: async alertId => {
          if (!alertId) return;
          try {
            await postBridge("/ops/alerts/ack", { id: alertId });
            await loadOpsHealthData();
          } catch (err) {
            showToast(`Could not dismiss alert: ${getErrorMessage(err)}`, "error");
          }
        }
      });
      renderAdminOpsKpis(refs.adminOpsKpisEl, health?.kpis || {}, String(health?.status || "healthy"));
      renderAdminOpsSchedule(refs.adminOpsScheduleEl, health?.schedule || {}, state.latestOpsHealthCache);
      renderAdminOpsFetcherMetrics(refs.adminOpsFetcherMetricsEl, fetcherMetrics || {});
      renderAdminOpsHistory(refs.adminOpsHistoryEl, runModel);
      renderAdminOpsTrends(refs.adminOpsTrendsEl, historyPayload?.runs || []);
      loadSyncStatus({ silent: true }).catch(() => {});
      adminDispatch.dispatch({ type: adminActions.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
      scheduleOpsHealthPolling(getOpsPollIntervalMs(Boolean(runModel?.hasLiveRuns)));
    } catch (err) {
      setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
      setBusyFlag("liveFetchRunning", false);
      setBusyFlag("liveDiscoveryRunning", false);
      setBusyFlag("liveSyncRunning", false);
      setBusyFlag("livePipelineRunning", false);
      scheduleOpsHealthPolling(idlePollIntervalMs);
    } finally {
      setBusyFlag("opsLoad", false);
    }
  }

  function setBridgeStatusBadge(stateValue, label) {
    if (!refs.adminBridgeStatusBadgeEl) return;
    const normalized = String(stateValue || "checking").toLowerCase();
    refs.adminBridgeStatusBadgeEl.classList.remove("online", "offline", "checking");
    refs.adminBridgeStatusBadgeEl.classList.add(
      normalized === "online" ? "online" : normalized === "offline" ? "offline" : "checking"
    );
    refs.adminBridgeStatusBadgeEl.textContent = label || "Bridge Checking";
    refs.adminBridgeStatusBadgeEl.classList.remove("refresh-pulse");
    void refs.adminBridgeStatusBadgeEl.offsetWidth;
    refs.adminBridgeStatusBadgeEl.classList.add("refresh-pulse");
  }

  function startBridgeStatusWatch() {
    stopBridgeStatusWatch();
    pollBridgeStatus({ forceChecking: true }).catch(() => {});
    state.bridgeStatusPollTimer = setInterval(() => {
      pollBridgeStatus().catch(() => {});
    }, bridgeStatusPollIntervalMs);
  }

  function stopBridgeStatusWatch() {
    if (!state.bridgeStatusPollTimer) return;
    clearInterval(state.bridgeStatusPollTimer);
    state.bridgeStatusPollTimer = null;
  }

  async function pollBridgeStatus(options = {}) {
    if (!state.adminPin) {
      setBridgeStatusBadge("checking", "Bridge Locked");
      return;
    }
    if (options.forceChecking) {
      setBridgeStatusBadge("checking", "Bridge Checking");
    }
    try {
      const summaryPayload = await getBridge("/registry/summary");
      const summary = summaryPayload?.summary || {};
      const activeCount = Number(summary?.activeCount || 0);
      const pendingCount = Number(summary?.pendingCount || 0);
      setBridgeStatusBadge("online", `Bridge Online (${activeCount} active, ${pendingCount} pending)`);
    } catch {
      setBridgeStatusBadge("offline", "Bridge Offline");
    }
  }

  return {
    setOpsPlaceholders,
    stopOpsHealthPolling,
    scheduleOpsHealthPolling,
    loadOpsHealthData,
    setBridgeStatusBadge,
    startBridgeStatusWatch,
    stopBridgeStatusWatch,
    pollBridgeStatus
  };
}
