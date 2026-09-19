import { getObjectValue } from "../../domain/ops-shape-utils.js";
import {
  ACTIVE_ADMIN_TASK_TYPES,
  ACTIVE_PIPELINE_OR_FETCH_TASK_TYPES,
  hasActiveAdminTaskRows
} from "../active-work-policy.js";
import {
  getOpsAbortKey,
  buildOptimisticAbortRow
} from "../../domain/ops-abort-model.js";
import {
  getTaskRowType,
  getTaskRowRunId
} from "../../domain/ops-pipeline-status-model.js";

const OPS_DEGRADED_ACTIVE_TTL_MS = 30000;
const ACTIVE_IDLE_RECOVERY_COOLDOWN_MS = 1500;

/**
 * Active-run, abort-request and degraded-active bookkeeping for the ops health
 * controller. Every helper reads and writes `state`; nothing here closes over
 * controller-local mutable state, so the leaves stay independently testable.
 */
export function createOpsRunState({
  state,
  refs,
  markStep,
  setBusyFlag,
  renderOpsHealthSnapshot,
  currentRenderToken,
  onActivePipelineIdle,
  deriveAdminRunsModel,
  taskStateController
}) {
  function getCachedTaskStatePayload() {
    return getObjectValue(state.latestOpsTaskStatePayload);
  }

  function getCachedHistoryPayload() {
    return state.latestOpsHistoryPayload
      && typeof state.latestOpsHistoryPayload === "object"
      && !Array.isArray(state.latestOpsHistoryPayload)
      ? state.latestOpsHistoryPayload
      : { runs: [] };
  }

  function getCachedSourcePolicyPayload() {
    return state.latestSourcePolicyRecommendationsPayload
      && typeof state.latestSourcePolicyRecommendationsPayload === "object"
      && !Array.isArray(state.latestSourcePolicyRecommendationsPayload)
      ? state.latestSourcePolicyRecommendationsPayload
      : { summaryStatus: "pending" };
  }

  function getCachedRegistryConflictsPayload() {
    return state.latestRegistryConflictsPayload
      && typeof state.latestRegistryConflictsPayload === "object"
      && !Array.isArray(state.latestRegistryConflictsPayload)
      ? state.latestRegistryConflictsPayload
      : { summary: {}, summaryStatus: "pending", conflicts: [] };
  }

  function getCachedDiscoveryAuditArtifactsPayload() {
    return state.latestDiscoveryAuditArtifactsPayload
      && typeof state.latestDiscoveryAuditArtifactsPayload === "object"
      && !Array.isArray(state.latestDiscoveryAuditArtifactsPayload)
      ? state.latestDiscoveryAuditArtifactsPayload
      : { ok: true, artifacts: [] };
  }

  function getCachedTaskFailureAttemptsPayload() {
    return state.latestTaskFailureAttemptsPayload
      && typeof state.latestTaskFailureAttemptsPayload === "object"
      && !Array.isArray(state.latestTaskFailureAttemptsPayload)
      ? state.latestTaskFailureAttemptsPayload
      : { ok: true, fetch: {}, discovery: {}, warnings: [] };
  }

  function getCachedPerformanceProfilePayload() {
    return state.latestOpsPerformanceProfilePayload
      && typeof state.latestOpsPerformanceProfilePayload === "object"
      && !Array.isArray(state.latestOpsPerformanceProfilePayload)
      ? state.latestOpsPerformanceProfilePayload
      : { ok: true, routeTimings: { routes: [] }, operationTimings: { operations: [] } };
  }

  function hasActiveRows(taskStatePayload = getCachedTaskStatePayload()) {
    const rows = Array.isArray(taskStatePayload?.tasks) ? taskStatePayload.tasks : [];
    return rows.some(row => row && row.active !== false && !row.finishedAt);
  }

  function getOpsAbortRequests() {
    if (!state.adminOpsAbortRequests || typeof state.adminOpsAbortRequests !== "object" || Array.isArray(state.adminOpsAbortRequests)) {
      state.adminOpsAbortRequests = {};
    }
    return state.adminOpsAbortRequests;
  }

  function hasPendingOpsAbort(taskType, runId) {
    const key = getOpsAbortKey(taskType, runId);
    return Boolean(key.trim() && getOpsAbortRequests()[key]);
  }

  function setPendingOpsAbort(taskType, runId, value = {}) {
    const key = getOpsAbortKey(taskType, runId);
    if (!key.trim()) return null;
    const pending = {
      taskType: String(taskType || "").trim().toLowerCase(),
      runId: String(runId || "").trim(),
      requestedAt: new Date().toISOString(),
      ...value
    };
    getOpsAbortRequests()[key] = pending;
    return pending;
  }

  function clearPendingOpsAbort(taskType, runId) {
    const key = getOpsAbortKey(taskType, runId);
    if (!key.trim() || !state.adminOpsAbortRequests) return;
    delete state.adminOpsAbortRequests[key];
  }

  function clearAllPendingOpsAborts() {
    state.adminOpsAbortRequests = {};
  }

  function hasPendingOpsAbortRequests() {
    return Object.keys(getOpsAbortRequests()).length > 0;
  }

  function applyOptimisticAbortRow(taskType, runId, pendingAbort = null) {
    const cleanTaskType = String(taskType || "").trim().toLowerCase();
    const cleanRunId = String(runId || "").trim();
    if (!cleanTaskType || !cleanRunId) return null;
    const abortMeta = pendingAbort || setPendingOpsAbort(cleanTaskType, cleanRunId);
    const existingPayload = getCachedTaskStatePayload();
    const existingRows = Array.isArray(existingPayload?.tasks) ? existingPayload.tasks : [];
    let matched = false;
    const tasks = existingRows.map(row => {
      if (getTaskRowType(row) !== cleanTaskType || getTaskRowRunId(row) !== cleanRunId) {
        return row;
      }
      matched = true;
      return buildOptimisticAbortRow(row, cleanTaskType, cleanRunId, abortMeta);
    });
    if (!matched) {
      tasks.unshift(buildOptimisticAbortRow({}, cleanTaskType, cleanRunId, abortMeta));
    }
    state.latestOpsTaskStatePayload = {
      ...getObjectValue(existingPayload),
      tasks,
      count: tasks.length,
      summary: true
    };
    if (cleanTaskType === "pipeline") setBusyFlag("livePipelineRunning", true);
    if (cleanTaskType === "fetch") setBusyFlag("liveFetchRunning", true);
    if (cleanTaskType === "discovery") setBusyFlag("liveDiscoveryRunning", true);
    if (["pipeline", "fetch", "discovery"].includes(cleanTaskType)) {
      state.opsActivePipelineOrFetchLastActive = true;
      markOpsDegradedActive("abort_requested");
    }
    renderOpsHealthSnapshot(currentRenderToken(), state.latestOpsHealthCache || {}, {
      taskStatePayload: state.latestOpsTaskStatePayload,
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      syncTaskState: true,
      renderDeferredPanels: false,
      renderActivityPanel: true,
      schedulePolling: false
    });
    return state.latestOpsTaskStatePayload;
  }

  function hasOptimisticRows() {
    return Boolean(state.discoveryOptimisticRun || state.fetchOptimisticRun);
  }

  function hasRecentOpsDegradedActive() {
    return Date.now() < Number(state.opsDegradedActiveUntilMs || 0);
  }

  function markActiveIdleRecoveryCooldown() {
    state.opsActiveIdleRecoveryCooldownUntilMs = Date.now() + ACTIVE_IDLE_RECOVERY_COOLDOWN_MS;
  }

  function hasActiveIdleRecoveryCooldown() {
    return Date.now() < Number(state.opsActiveIdleRecoveryCooldownUntilMs || 0);
  }

  function hasPossiblePipelineOrFetchEvidence({ includeRecent = true } = {}) {
    if (hasActiveIdleRecoveryCooldown()) return false;
    const busyState = state.adminBusyState || {};
    return Boolean(
      hasActivePipelineOrFetchRows(getCachedTaskStatePayload())
      || hasOptimisticRows()
      || hasPendingOpsAbortRequests()
      || state.opsActivePipelineOrFetchLastActive
      || state.fetcherLiveProgressState
      || busyState.fetcherWatch
      || busyState.livePipelineRunning
      || busyState.liveFetchRunning
      || (includeRecent && hasRecentOpsDegradedActive() && state.opsActivePipelineOrFetchLastActive)
    );
  }

  function hasPossibleActiveRunEvidence({ includeRecent = true } = {}) {
    if (hasActiveIdleRecoveryCooldown()) return false;
    const busyState = state.adminBusyState || {};
    return Boolean(
      hasActiveAdminWorkRows(getCachedTaskStatePayload())
      || hasOptimisticRows()
      || hasPendingOpsAbortRequests()
      || state.opsActiveAdminWorkLastActive
      || state.opsActivePipelineOrFetchLastActive
      || state.fetcherLiveProgressState
      || state.discoveryLiveProgressState
      || busyState.fetcherWatch
      || busyState.discoveryWatch
      || busyState.livePipelineRunning
      || busyState.liveFetchRunning
      || busyState.liveDiscoveryRunning
      || busyState.liveSyncRunning
      || (includeRecent && hasRecentOpsDegradedActive())
    );
  }

  function markOpsDegradedActive(reason = "control_plane_unavailable") {
    state.opsDegradedActiveUntilMs = Date.now() + OPS_DEGRADED_ACTIVE_TTL_MS;
    state.opsDegradedActiveReason = String(reason || "control_plane_unavailable");
    state.opsActiveAdminWorkLastActive = true;
    if (hasPossiblePipelineOrFetchEvidence({ includeRecent: false })) {
      state.opsActivePipelineOrFetchLastActive = true;
    }
  }

  function clearOpsDegradedActive() {
    state.opsDegradedActiveUntilMs = 0;
    state.opsDegradedActiveReason = "";
  }

  function hasActivePipelineOrFetchRows(taskStatePayload = getCachedTaskStatePayload()) {
    return hasActiveAdminTaskRows(taskStatePayload, ACTIVE_PIPELINE_OR_FETCH_TASK_TYPES);
  }

  function hasActiveAdminWorkRows(taskStatePayload = getCachedTaskStatePayload()) {
    return hasActiveAdminTaskRows(taskStatePayload, ACTIVE_ADMIN_TASK_TYPES);
  }

  function notifyActiveAdminWorkIdleIfNeeded(wasActive, isActive, { wasPipelineOrFetchActive = false, pipelineOrFetchActive = false } = {}) {
    state.opsActiveAdminWorkLastActive = Boolean(isActive);
    state.opsActivePipelineOrFetchLastActive = Boolean(pipelineOrFetchActive);
    if (!wasActive || isActive || typeof onActivePipelineIdle !== "function") return;
    Promise.resolve(onActivePipelineIdle({
      reason: wasPipelineOrFetchActive ? "active_pipeline_idle" : "active_admin_work_idle",
      at: new Date().toISOString()
    })).catch(() => {});
  }

  function deriveLiveRunContext(taskStatePayload, registryConflictsPayload) {
    const historyPayload = getCachedHistoryPayload();
    const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
    const runModel = deriveAdminRunsModel(
      {
        taskState: taskStatePayload || {},
        historyRuns
      },
      Date.now()
    );
    const liveTaskRows = taskStateController.getActiveTaskRows(taskStatePayload);
    const liveTypes = new Set(
      liveTaskRows
        .map(row => taskStateController.getTaskType(row))
        .filter(Boolean)
    );
    const registryConflictRunning = String(registryConflictsPayload?.adjudication?.status || "") === "running";
    return {
      historyRuns,
      runModel,
      liveTaskRows,
      liveTypes,
      registryConflictRunning
    };
  }

  function resolveLiveRef(refName, selector, diagnosticName) {
    const current = refs?.[refName] || null;
    const currentConnected = Boolean(
      current
      && (
        typeof current.isConnected !== "boolean"
        || current.isConnected
      )
    );
    if (currentConnected) return current;
    const next = globalThis.document?.querySelector?.(selector) || current;
    if (next && next !== current) {
      refs[refName] = next;
      markStep(`admin_${diagnosticName}_render_target_rebound`);
      return next;
    }
    if (!next) {
      markStep(`admin_${diagnosticName}_render_target_missing`);
    }
    return next;
  }

  function getScheduleElement() {
    return resolveLiveRef("adminOpsScheduleEl", '[data-ui="admin-ops-schedule"]', "pipeline_schedule");
  }

  function getHistoryElement() {
    return resolveLiveRef("adminOpsHistoryEl", '[data-ui="admin-ops-history"]', "ops_history");
  }

  return {
    getCachedTaskStatePayload,
    getCachedHistoryPayload,
    getCachedSourcePolicyPayload,
    getCachedRegistryConflictsPayload,
    getCachedDiscoveryAuditArtifactsPayload,
    getCachedTaskFailureAttemptsPayload,
    getCachedPerformanceProfilePayload,
    hasActiveRows,
    hasPendingOpsAbort,
    setPendingOpsAbort,
    clearPendingOpsAbort,
    clearAllPendingOpsAborts,
    applyOptimisticAbortRow,
    hasOptimisticRows,
    markActiveIdleRecoveryCooldown,
    hasPossiblePipelineOrFetchEvidence,
    hasPossibleActiveRunEvidence,
    markOpsDegradedActive,
    clearOpsDegradedActive,
    hasActivePipelineOrFetchRows,
    hasActiveAdminWorkRows,
    notifyActiveAdminWorkIdleIfNeeded,
    deriveLiveRunContext,
    getScheduleElement,
    getHistoryElement
  };
}
