import { getTaskStateRows } from "../../../shared/live-task.js";

export function createOpsTaskStateController({
  state,
  setBusyFlag,
  attachToActiveFetchRun,
  loadLatestFetcherSummary,
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

  function syncLiveBusyFlags(liveTypes) {
    setBusyFlag("liveFetchRunning", liveTypes.has("fetch"));
    setBusyFlag("liveDiscoveryRunning", liveTypes.has("discovery"));
    setBusyFlag("liveSyncRunning", liveTypes.has("sync"));
    setBusyFlag("livePipelineRunning", liveTypes.has("pipeline"));
  }

  function rememberTaskStatePayload(payload) {
    state.latestTaskStatePayload = normalizeTaskStatePayload(payload);
    state.waitingForTaskState = false;
    return state.latestTaskStatePayload;
  }

  function resetLifecycleTaskState() {
    state.latestTaskStatePayload = { tasks: [], count: 0 };
    state.waitingForTaskState = false;
  }

  function acceptLifecycleTaskStatePayload(candidatePayload) {
    return rememberTaskStatePayload(normalizeTaskStatePayload(candidatePayload));
  }

  function resolveTaskStatePayload(taskStateResult) {
    state.waitingForTaskState = Boolean(
      taskStateResult?.status === "fulfilled"
      && (taskStateResult.value === null || taskStateResult.value === undefined)
    );
    if (
      taskStateResult?.status !== "fulfilled"
      || !taskStateResult?.value
      || typeof taskStateResult.value !== "object"
      || Array.isArray(taskStateResult.value)
    ) {
      const isRejected = taskStateResult?.status === "rejected";
      const message = isRejected
        ? String(taskStateResult.reason?.message || taskStateResult.reason || "Task state unavailable.")
        : "Task state unavailable.";
      // ponytail: track failed task-state fetch separately from "empty list"
      // so admin can show different placeholder for "bridge hiccup" vs "no active tasks".
      state.lastTaskStateError = message;
      return rememberTaskStatePayload({
        tasks: [],
        count: 0,
        taskStateUnavailable: true,
        diagnostics: [{ code: "task_state_unavailable", message }]
      });
    }
    state.lastTaskStateError = "";
    return acceptLifecycleTaskStatePayload(taskStateResult.value);
  }

  function maybeAttachLiveTaskRows(liveTaskRows) {
    const canAttachLiveWatch = row => String(row?.controlPlaneSource || "").trim() !== "pipeline-status";
    const hydrateActiveFetchProgress = () => {
      if (typeof loadLatestFetcherSummary === "function") {
        return loadLatestFetcherSummary({ silent: true }).catch(() => {});
      }
      return Promise.resolve(null);
    };
    const fetchRow = liveTaskRows.find(row => (
      getTaskType(row) === "fetch" && hasTaskRunMeta(row) && canAttachLiveWatch(row)
    ));
    if (fetchRow && !state.adminBusyState.fetcherWatch) {
      attachToActiveFetchRun?.({
        runId: fetchRow?.runId,
        startedAt: fetchRow?.startedAt
      }, {
        announceStart: false,
        initialReport: fetchRow
      });
      hydrateActiveFetchProgress();
    }

    const discoveryRow = liveTaskRows.find(row => (
      getTaskType(row) === "discovery" && hasTaskRunMeta(row) && canAttachLiveWatch(row)
    ));
    if (discoveryRow && !state.adminBusyState.discoveryWatch) {
      attachToActiveDiscoveryRun?.({
        runId: discoveryRow?.runId,
        startedAt: discoveryRow?.startedAt
      }, {
        announceStart: false,
        initialReport: discoveryRow
      });
      loadLatestDiscoveryReport?.({ silent: true, view: "summary" }).catch(() => {});
    }
  }

  return {
    getActiveTaskRows,
    getTaskType,
    maybeAttachLiveTaskRows,
    resetLifecycleTaskState,
    resolveTaskStatePayload,
    syncLiveBusyFlags
  };
}
