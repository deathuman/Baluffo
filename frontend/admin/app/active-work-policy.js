export const ACTIVE_PIPELINE_OR_FETCH_TASK_TYPES = new Set(["pipeline", "fetch"]);
export const ACTIVE_ADMIN_TASK_TYPES = new Set(["pipeline", "fetch", "discovery", "sync"]);
const TERMINAL_TASK_STATUSES = new Set([
  "ok",
  "success",
  "succeeded",
  "failed",
  "error",
  "canceled",
  "cancelled",
  "aborted"
]);
const INACTIVE_PIPELINE_STAGES = new Set([
  "idle",
  "complete",
  "completed",
  "error",
  "failed",
  "canceled",
  "cancelled",
  "aborted"
]);

function getTaskStateRows(payload) {
  return Array.isArray(payload?.tasks) ? payload.tasks : [];
}

export function getAdminTaskType(row) {
  return String(row?.taskType || row?.type || "").trim().toLowerCase();
}

export function isActiveAdminTaskRow(row, taskTypes = ACTIVE_ADMIN_TASK_TYPES) {
  if (!row || typeof row !== "object") return false;
  const taskType = getAdminTaskType(row);
  const status = String(row?.status || row?.lifecycleStatus || "").trim().toLowerCase();
  return taskTypes.has(taskType)
    && row.active !== false
    && !String(row.finishedAt || "").trim()
    && !TERMINAL_TASK_STATUSES.has(status);
}

export function hasActiveAdminTaskRows(taskStatePayload, taskTypes = ACTIVE_ADMIN_TASK_TYPES) {
  return getTaskStateRows(taskStatePayload).some(row => isActiveAdminTaskRow(row, taskTypes));
}

export function getActiveAdminTaskTypes(taskStatePayload, taskTypes = ACTIVE_ADMIN_TASK_TYPES) {
  return new Set(
    getTaskStateRows(taskStatePayload)
      .filter(row => isActiveAdminTaskRow(row, taskTypes))
      .map(row => getAdminTaskType(row))
  );
}

export function pipelineStatusStage(payload = {}) {
  return String(
    payload?.stage
      || payload?.pipelineStage
      || payload?.progress?.phaseKey
      || payload?.progress?.stage
      || payload?.currentStage
      || payload?.status
      || ""
  ).trim().toLowerCase();
}

function pipelineStatusHasActiveChild(payload = {}, taskType = "") {
  const normalizedTaskType = String(taskType || "").trim().toLowerCase();
  const children = Array.isArray(payload?.activeChildren) ? payload.activeChildren : [];
  return children.some(child => {
    const childType = String(child?.taskType || child?.type || child?.stage || "").trim().toLowerCase();
    const childStatus = String(child?.status || child?.lifecycleStatus || "").trim().toLowerCase();
    return child
      && child.active !== false
      && (!normalizedTaskType || childType === normalizedTaskType)
      && !String(child.finishedAt || "").trim()
      && !TERMINAL_TASK_STATUSES.has(childStatus);
  });
}

export function pipelineStatusIndicatesFetch(payload = {}) {
  return pipelineStatusIndicatesActive(payload)
    && (pipelineStatusStage(payload) === "fetch" || pipelineStatusHasActiveChild(payload, "fetch"));
}

export function pipelineStatusIndicatesDiscovery(payload = {}) {
  return pipelineStatusIndicatesActive(payload)
    && (pipelineStatusStage(payload) === "discovery" || pipelineStatusHasActiveChild(payload, "discovery"));
}

export function pipelineStatusIndicatesActive(payload = {}) {
  if (!payload || typeof payload !== "object") return false;
  if (payload.active === true) return true;
  if (pipelineStatusHasActiveChild(payload)) return true;
  if (payload.active === false) return false;
  const stage = pipelineStatusStage(payload);
  return Boolean(stage && !INACTIVE_PIPELINE_STAGES.has(stage));
}

function busyFlag(busyState = {}, key) {
  return Boolean(busyState?.[key]);
}

export function deriveAdminActiveWorkContext({
  state = null,
  busyState = state?.adminBusyState || {},
  taskStatePayload = state?.latestOpsTaskStatePayload || state?.latestTaskStatePayload || null,
  pipelineStatusPayload = state?.discoveryPipelineStatusPayload || null,
  livePipelineOrFetchRunning = false,
  fetcherLiveProgressState = state?.fetcherLiveProgressState || null,
  discoveryLiveProgressState = state?.discoveryLiveProgressState || null
} = {}) {
  const activeTaskTypes = getActiveAdminTaskTypes(taskStatePayload);
  const syncActive = Boolean(
    activeTaskTypes.has("sync")
    || busyFlag(busyState, "syncRun")
    || busyFlag(busyState, "liveSyncRunning")
  );
  const fetchActive = Boolean(
    activeTaskTypes.has("fetch")
    || busyFlag(busyState, "fetcherRun")
    || busyFlag(busyState, "fetcherWatch")
    || busyFlag(busyState, "liveFetchRunning")
    || fetcherLiveProgressState
    || pipelineStatusIndicatesFetch(pipelineStatusPayload)
  );
  const discoveryActive = Boolean(
    activeTaskTypes.has("discovery")
    || busyFlag(busyState, "discoveryRun")
    || busyFlag(busyState, "discoveryWatch")
    || busyFlag(busyState, "liveDiscoveryRunning")
    || discoveryLiveProgressState
    || pipelineStatusIndicatesDiscovery(pipelineStatusPayload)
  );
  const pipelineActive = Boolean(
    activeTaskTypes.has("pipeline")
    || busyFlag(busyState, "livePipelineRunning")
    || livePipelineOrFetchRunning
    || pipelineStatusIndicatesActive(pipelineStatusPayload)
  );
  const registryMutationActive = Boolean(
    busyFlag(busyState, "discoveryWrite")
    || busyFlag(busyState, "manualAdd")
    || busyFlag(busyState, "manualCheck")
  );
  const pipelineOrFetchActive = Boolean(pipelineActive || fetchActive);
  const isActive = Boolean(syncActive || pipelineActive || fetchActive || discoveryActive);
  const reason = syncActive
    ? "sync_running"
    : fetchActive
      ? "fetch_running"
      : discoveryActive
        ? "discovery_running"
        : pipelineActive
          ? "pipeline_running"
          : "idle";
  const taskType = syncActive
    ? "sync"
    : fetchActive
      ? "fetch"
      : discoveryActive
        ? "discovery"
        : pipelineActive
          ? "pipeline"
          : "";
  return {
    active: isActive,
    isActive,
    pipelineActive,
    fetchActive,
    discoveryActive,
    syncActive,
    pipelineOrFetchActive,
    registryMutationActive,
    canLoadCompactSourceTables: Boolean(isActive && !syncActive),
    sourceTablesCanLoadCompact: Boolean(isActive && !syncActive),
    sourceMutationsAllowed: Boolean(!isActive && !registryMutationActive),
    reason,
    taskType
  };
}

export function activeSummaryIndicatesAdminWork(activeSummary) {
  if (activeSummary?.isActive || activeSummary?.pipelinePayload?.active) return true;
  return deriveAdminActiveWorkContext({
    taskStatePayload: activeSummary?.taskStatePayload,
    pipelineStatusPayload: activeSummary?.pipelinePayload
  }).isActive;
}
