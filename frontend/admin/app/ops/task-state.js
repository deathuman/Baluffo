import { getTaskStateRows } from "../../../shared/live-task.js";

export function createOpsTaskStateController({
  state,
  setBusyFlag,
  attachToActiveFetchRun,
  loadLatestFetcherReport,
  attachToActiveDiscoveryRun,
  loadLatestDiscoveryReport
}) {
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

  return {
    clearRetainedTaskState,
    getActiveTaskRows,
    getTaskType,
    maybeAttachLiveTaskRows,
    resolveTaskStatePayload,
    syncLiveBusyFlags
  };
}
