import { deriveFetcherFailureSummary } from "../domain.js";
import { getTaskStateRows } from "../../shared/live-task.js";

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
  deriveAdminRunsModel,
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
  onBridgeStatusChange,
  loadDiscoveryData,
  bridgeStatusPollIntervalMs,
  idlePollIntervalMs
}) {
  let lastBridgeStatus = "checking";
  let lastDiscoveryRegistryRefreshAtMs = 0;

  function maybeUnrefTimer(timer) {
    timer?.unref?.();
    return timer;
  }

  function setOpsPlaceholders(message = "Operations health unavailable.") {
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
    const waitMs = Math.max(600, Number(delayMs) || 10000);
    state.opsHealthPollTimer = maybeUnrefTimer(setTimeout(() => {
      loadOpsHealthData({ fromPoll: true }).catch(() => {});
    }, waitMs));
  }

  async function loadOpsHealthData(options = {}) {
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
      let taskStatePayload = { tasks: [], count: 0 };
      try {
        const loadedTaskState = await getBridge("/ops/task-state");
        if (loadedTaskState && typeof loadedTaskState === "object") {
          taskStatePayload = loadedTaskState;
        }
      } catch {
        taskStatePayload = { tasks: [], count: 0 };
      }
      let fetcherMetrics = null;
      try {
        fetcherMetrics = await getBridge("/ops/fetcher-metrics?windowRuns=80");
      } catch {
        fetcherMetrics = null;
      }
      state.latestOpsHealthCache = health || null;
      const runModel = deriveAdminRunsModel(
        {
          taskState: taskStatePayload || {},
          historyRuns: historyPayload?.runs || []
        },
        Date.now()
      );
      const liveTaskRows = getTaskStateRows(taskStatePayload)
        .filter(row => row && typeof row === "object" && row.active);
      const liveTypes = new Set(
        liveTaskRows
          .map(row => String(row?.taskType || row?.type || "").toLowerCase())
          .filter(Boolean)
      );
      setBusyFlag("liveFetchRunning", liveTypes.has("fetch"));
      setBusyFlag("liveDiscoveryRunning", liveTypes.has("discovery"));
      setBusyFlag("liveSyncRunning", liveTypes.has("sync"));
      setBusyFlag("livePipelineRunning", liveTypes.has("pipeline"));
      const nowMs = Date.now();
      const discoveryLive = liveTypes.has("discovery");
      if (!discoveryLive) {
        lastDiscoveryRegistryRefreshAtMs = 0;
      } else if (typeof loadDiscoveryData === "function" && nowMs - lastDiscoveryRegistryRefreshAtMs >= 5000) {
        lastDiscoveryRegistryRefreshAtMs = nowMs;
        loadDiscoveryData().catch(() => {});
      }

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
      renderAdminOpsFetcherMetrics(
        refs.adminOpsFetcherMetricsEl,
        fetcherMetrics || {},
        deriveFetcherFailureSummary(state.latestFetcherReportCache || {})
      );
      renderAdminOpsHistory(refs.adminOpsHistoryEl, runModel);
      renderAdminOpsTrends(refs.adminOpsTrendsEl, historyPayload?.runs || []);
      loadSyncStatus({ silent: true }).catch(() => {});
      adminDispatch.dispatch({ type: adminActions.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
      scheduleOpsHealthPolling(getOpsPollIntervalMs(liveTypes.size > 0));
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
    state.bridgeStatusPollTimer = maybeUnrefTimer(setInterval(() => {
      pollBridgeStatus().catch(() => {});
    }, bridgeStatusPollIntervalMs));
  }

  function stopBridgeStatusWatch() {
    if (!state.bridgeStatusPollTimer) return;
    clearInterval(state.bridgeStatusPollTimer);
    state.bridgeStatusPollTimer = null;
  }

  async function pollBridgeStatus(options = {}) {
    if (options.forceChecking) {
      if (lastBridgeStatus !== "checking") {
        lastBridgeStatus = "checking";
        onBridgeStatusChange?.("checking");
      }
      setBridgeStatusBadge("checking", "Bridge Checking");
    }
    try {
      const summaryPayload = await getBridge("/registry/summary");
      const summary = summaryPayload?.summary || {};
      const activeCount = Number(summary?.activeCount || 0);
      const pendingCount = Number(summary?.pendingCount || 0);
      if (lastBridgeStatus !== "online") {
        lastBridgeStatus = "online";
        onBridgeStatusChange?.("online");
      }
      setBridgeStatusBadge("online", `Bridge Online (${activeCount} active, ${pendingCount} pending)`);
    } catch {
      if (lastBridgeStatus !== "offline") {
        lastBridgeStatus = "offline";
        onBridgeStatusChange?.("offline");
      }
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
