import { deriveFetcherFailureSummary } from "../domain.js";
import {
  renderAdminOpsAlerts,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsHistory,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsTrends
} from "../render.js";
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
  renderAdminOpsAlerts: renderAdminOpsAlertsImpl = renderAdminOpsAlerts,
  renderAdminOpsKpis: renderAdminOpsKpisImpl = renderAdminOpsKpis,
  renderAdminOpsSchedule: renderAdminOpsScheduleImpl = renderAdminOpsSchedule,
  renderAdminOpsFetcherMetrics: renderAdminOpsFetcherMetricsImpl = renderAdminOpsFetcherMetrics,
  renderAdminOpsTrends: renderAdminOpsTrendsImpl = renderAdminOpsTrends,
  renderAdminOpsHistory: renderAdminOpsHistoryImpl = renderAdminOpsHistory,
  loadSyncStatus,
  setBusyFlag,
  showToast,
  getErrorMessage,
  adminDispatch,
  adminActions,
  escapeHtml,
  onBridgeStatusChange,
  loadDiscoveryData,
  attachToActiveFetchRun,
  loadLatestFetcherReport,
  attachToActiveDiscoveryRun,
  loadLatestDiscoveryReport,
  bridgeStatusPollIntervalMs,
  idlePollIntervalMs
}) {
  let lastBridgeStatus = "checking";
  let lastDiscoveryRegistryRefreshAtMs = 0;

  function maybeUnrefTimer(timer) {
    timer?.unref?.();
    return timer;
  }

  function normalizeTaskStatePayload(payload) {
    const tasks = getTaskStateRows(payload)
      .filter(row => row && typeof row === "object");
    return {
      ...(payload && typeof payload === "object" && !Array.isArray(payload) ? payload : {}),
      tasks,
      count: tasks.length
    };
  }

  function getActiveTaskRows(payload) {
    return getTaskStateRows(payload)
      .filter(row => row && typeof row === "object" && Boolean(row.active));
  }

  function getTaskType(row) {
    return String(row?.taskType || row?.type || "").trim().toLowerCase();
  }

  function hasTaskRunMeta(row) {
    return Boolean(String(row?.runId || "").trim() || String(row?.startedAt || "").trim());
  }

  function isTerminalHistoryRun(row) {
    if (!row || typeof row !== "object") return false;
    if (String(row?.finishedAt || "").trim()) return true;
    const status = String(row?.status || "").trim().toLowerCase();
    return Boolean(
      status
      && !["started", "running", "queued", "pending"].includes(status)
    );
  }

  function matchesTaskHistoryRow(taskRow, historyRow) {
    if (!taskRow || !historyRow || typeof taskRow !== "object" || typeof historyRow !== "object") {
      return false;
    }
    const taskType = getTaskType(taskRow);
    const historyType = String(historyRow?.type || historyRow?.taskType || "").trim().toLowerCase();
    if (!taskType || taskType !== historyType) return false;
    const taskRunId = String(taskRow?.runId || "").trim();
    const historyRunId = String(historyRow?.runId || historyRow?.id || "").trim();
    if (taskRunId && historyRunId) {
      return taskRunId === historyRunId;
    }
    const taskStartedAt = String(taskRow?.startedAt || "").trim();
    const historyStartedAt = String(historyRow?.startedAt || "").trim();
    return Boolean(taskStartedAt && historyStartedAt && taskStartedAt === historyStartedAt);
  }

  function hasTerminalHistoryEvidence(taskRow, historyRuns) {
    return Array.isArray(historyRuns) && historyRuns.some(historyRow => (
      matchesTaskHistoryRow(taskRow, historyRow) && isTerminalHistoryRun(historyRow)
    ));
  }

  function syncLiveBusyFlags(liveTypes) {
    setBusyFlag("liveFetchRunning", liveTypes.has("fetch"));
    setBusyFlag("liveDiscoveryRunning", liveTypes.has("discovery"));
    setBusyFlag("liveSyncRunning", liveTypes.has("sync"));
    setBusyFlag("livePipelineRunning", liveTypes.has("pipeline"));
  }

  function rememberTaskStatePayload(payload) {
    state.latestTaskStatePayload = normalizeTaskStatePayload(payload);
    return state.latestTaskStatePayload;
  }

  function clearRetainedTaskState() {
    state.taskStateMissingStreakByType = {};
    state.latestTaskStatePayload = { tasks: [], count: 0 };
  }

  function mergeRetainedTaskStatePayload(candidatePayload, historyRuns) {
    const candidate = normalizeTaskStatePayload(candidatePayload);
    const previous = normalizeTaskStatePayload(state.latestTaskStatePayload);
    const mergedRows = [...getTaskStateRows(candidate)];
    const candidateActiveRows = getActiveTaskRows(candidate);
    const candidateActiveTypes = new Set(candidateActiveRows.map(getTaskType).filter(Boolean));
    const previousActiveRows = getActiveTaskRows(previous);
    const previousMissingStreaks = (
      state.taskStateMissingStreakByType && typeof state.taskStateMissingStreakByType === "object"
    )
      ? state.taskStateMissingStreakByType
      : {};
    const nextMissingStreaks = {};

    candidateActiveTypes.forEach(type => {
      nextMissingStreaks[type] = 0;
    });

    previousActiveRows.forEach(row => {
      const taskType = getTaskType(row);
      if (!taskType || candidateActiveTypes.has(taskType)) return;
      if (hasTerminalHistoryEvidence(row, historyRuns)) {
        nextMissingStreaks[taskType] = 0;
        return;
      }
      const nextMissingCount = Math.max(
        0,
        Number(previousMissingStreaks[taskType]) || 0
      ) + 1;
      nextMissingStreaks[taskType] = nextMissingCount;
      if (nextMissingCount < 2) {
        mergedRows.push(row);
      }
    });

    state.taskStateMissingStreakByType = nextMissingStreaks;
    return rememberTaskStatePayload({
      ...candidate,
      tasks: mergedRows,
      count: mergedRows.length
    });
  }

  function resolveTaskStatePayload(taskStateResult, historyRuns) {
    const previous = normalizeTaskStatePayload(state.latestTaskStatePayload);
    const previousLiveRows = getActiveTaskRows(previous);
    if (
      taskStateResult?.status !== "fulfilled"
      || !taskStateResult?.value
      || typeof taskStateResult.value !== "object"
      || Array.isArray(taskStateResult.value)
    ) {
      return previousLiveRows.length > 0
        ? previous
        : rememberTaskStatePayload({ tasks: [], count: 0 });
    }
    return mergeRetainedTaskStatePayload(taskStateResult.value, historyRuns);
  }

  function maybeAttachLiveTaskRows(liveTaskRows) {
    const fetchRow = liveTaskRows.find(row => getTaskType(row) === "fetch" && hasTaskRunMeta(row));
    if (fetchRow && !state.adminBusyState.fetcherWatch) {
      attachToActiveFetchRun?.({
        runId: fetchRow?.runId,
        startedAt: fetchRow?.startedAt
      }, {
        announceStart: false
      });
      loadLatestFetcherReport?.({ silent: true, hydrateActiveProgress: true }).catch(() => {});
    }

    const discoveryRow = liveTaskRows.find(row => getTaskType(row) === "discovery" && hasTaskRunMeta(row));
    if (discoveryRow && !state.adminBusyState.discoveryWatch) {
      attachToActiveDiscoveryRun?.({
        runId: discoveryRow?.runId,
        startedAt: discoveryRow?.startedAt
      }, {
        announceStart: false
      });
      loadLatestDiscoveryReport?.({ silent: true }).catch(() => {});
    }
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
      const [healthResult, historyResult, taskStateResult, fetcherMetricsResult] = await Promise.allSettled([
        getBridge("/ops/health"),
        getBridge("/ops/history?limit=80"),
        getBridge("/ops/task-state"),
        getBridge("/ops/fetcher-metrics?windowRuns=80")
      ]);
      const health = (
        healthResult.status === "fulfilled"
        && healthResult.value
        && typeof healthResult.value === "object"
        && !Array.isArray(healthResult.value)
      )
        ? healthResult.value
        : state.latestOpsHealthCache;
      const historyPayload = (
        historyResult.status === "fulfilled"
        && historyResult.value
        && typeof historyResult.value === "object"
        && !Array.isArray(historyResult.value)
      )
        ? historyResult.value
        : (
          state.latestOpsHistoryPayload
          && typeof state.latestOpsHistoryPayload === "object"
          && !Array.isArray(state.latestOpsHistoryPayload)
            ? state.latestOpsHistoryPayload
            : { runs: [] }
        );
      if (healthResult.status === "fulfilled" && health && typeof health === "object") {
        state.latestOpsHealthCache = health || null;
      }
      if (historyResult.status === "fulfilled" && historyPayload && typeof historyPayload === "object") {
        state.latestOpsHistoryPayload = historyPayload;
      }
      const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
      const taskStatePayload = resolveTaskStatePayload(taskStateResult, historyRuns);
      const fetcherMetrics = fetcherMetricsResult.status === "fulfilled"
        ? fetcherMetricsResult.value
        : null;
      const runModel = deriveAdminRunsModel(
        {
          taskState: taskStatePayload || {},
          historyRuns
        },
        Date.now()
      );
      const liveTaskRows = getActiveTaskRows(taskStatePayload);
      const liveTypes = new Set(
        liveTaskRows
          .map(row => getTaskType(row))
          .filter(Boolean)
      );
      syncLiveBusyFlags(liveTypes);
      maybeAttachLiveTaskRows(liveTaskRows);
      const nowMs = Date.now();
      const discoveryLive = liveTypes.has("discovery");
      if (!discoveryLive) {
        lastDiscoveryRegistryRefreshAtMs = 0;
      } else if (typeof loadDiscoveryData === "function" && nowMs - lastDiscoveryRegistryRefreshAtMs >= 5000) {
        lastDiscoveryRegistryRefreshAtMs = nowMs;
        loadDiscoveryData().catch(() => {});
      }

      renderAdminOpsAlertsImpl(refs.adminOpsAlertsEl, health?.alerts || [], {
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
      renderAdminOpsKpisImpl(refs.adminOpsKpisEl, health?.kpis || {}, String(health?.status || "healthy"));
      renderAdminOpsScheduleImpl(refs.adminOpsScheduleEl, health?.schedule || {}, state.latestOpsHealthCache);
      renderAdminOpsFetcherMetricsImpl(
        refs.adminOpsFetcherMetricsEl,
        fetcherMetrics || {},
        deriveFetcherFailureSummary(state.latestFetcherReportCache || {})
      );
      renderAdminOpsHistoryImpl(refs.adminOpsHistoryEl, runModel);
      renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
      loadSyncStatus({ silent: true }).catch(() => {});
      adminDispatch.dispatch({ type: adminActions.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
      scheduleOpsHealthPolling(getOpsPollIntervalMs(liveTypes.size > 0));
    } catch (err) {
      const retainedLiveTypes = new Set(
        getActiveTaskRows(state.latestTaskStatePayload)
          .map(row => getTaskType(row))
          .filter(Boolean)
      );
      if (lastBridgeStatus === "offline" || retainedLiveTypes.size === 0) {
        clearRetainedTaskState();
        setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
        syncLiveBusyFlags(new Set());
        scheduleOpsHealthPolling(idlePollIntervalMs);
      } else {
        syncLiveBusyFlags(retainedLiveTypes);
        scheduleOpsHealthPolling(getOpsPollIntervalMs(true));
      }
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
