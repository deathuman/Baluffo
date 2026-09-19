import { mergeOpsHealth } from "../../domain/ops-merge-model.js";
import { markFetchKpisDeferredDuringActiveRun } from "../../domain/ops-fetch-kpi-model.js";
import { shouldKeepExistingActiveTaskState } from "../../domain/ops-pipeline-status-model.js";

/**
 * Seeds ops controller state from the startup bootstrap payload so the Ops
 * panel paints real data on first render instead of waiting for the first poll.
 */
export function createOpsBootstrapSeeder({
  state,
  nextRenderToken,
  currentRenderToken,
  getOpsPollIntervalMs,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  hasActiveRows,
  hasActiveAdminWorkRows,
  hasActivePipelineOrFetchRows,
  canHydrateCompactDuringActiveRun,
  seedFromBootstrapPayload,
  renderOpsHealthSnapshot,
  loadActiveOpsSupplementalData,
  schedulePipelineStatusPolling
}) {
  function applyBootstrapPayload(payload = {}) {
    const renderToken = nextRenderToken();
    const tasks = payload?.tasks && typeof payload.tasks === "object" ? payload.tasks : {};
    let currentRows = Array.isArray(tasks.current) ? tasks.current : [];
    const cachedTaskStatePayload = getCachedTaskStatePayload();
    let taskStateSource = "";
    if (!currentRows.length && cachedTaskStatePayload?.source === "pipeline-status" && hasActiveRows(cachedTaskStatePayload)) {
      currentRows = Array.isArray(cachedTaskStatePayload.tasks) ? cachedTaskStatePayload.tasks : [];
      taskStateSource = "pipeline-status";
    }
    const candidateTaskStatePayload = {
      tasks: currentRows,
      count: currentRows.length,
      summary: true
    };
    if (
      currentRows.length
      && cachedTaskStatePayload?.source === "pipeline-status"
      && hasActiveRows(cachedTaskStatePayload)
      && !shouldKeepExistingActiveTaskState(candidateTaskStatePayload, cachedTaskStatePayload, hasActiveRows)
    ) {
      currentRows = Array.isArray(cachedTaskStatePayload.tasks) ? cachedTaskStatePayload.tasks : [];
      taskStateSource = "pipeline-status";
    }
    const recentRows = Array.isArray(tasks.recent) ? tasks.recent : [];
    const taskStatePayload = {
      tasks: currentRows,
      count: currentRows.length,
      summary: true
    };
    if (taskStateSource) {
      taskStatePayload.source = taskStateSource;
    }
    const historyPayload = {
      runs: recentRows,
      count: recentRows.length,
      summaryView: true
    };
    const registrySummary = payload?.registrySummary && typeof payload.registrySummary === "object"
      ? payload.registrySummary
      : {};
    const kpis = {};
    if (Object.keys(registrySummary).length) {
      kpis.registrySync = { ...registrySummary };
      if (Object.prototype.hasOwnProperty.call(registrySummary, "pendingCount")) {
        kpis.pendingApprovalsCount = registrySummary.pendingCount;
      }
    }
    seedFromBootstrapPayload(payload);
    const health = {
      ok: true,
      status: "healthy",
      summaryView: true,
      alerts: [],
      kpis,
      appVersion: String(payload?.app?.version || "")
    };
    state.latestOpsHealthCache = mergeOpsHealth(state.latestOpsHealthCache || {}, health, { summary: true });
    state.latestOpsTaskStatePayload = taskStatePayload;
    state.taskStateUnavailable = false;
    if (hasActivePipelineOrFetchRows(taskStatePayload) && !canHydrateCompactDuringActiveRun()) {
      markFetchKpisDeferredDuringActiveRun(state);
    }
    renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || health, {
      taskStatePayload,
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      syncTaskState: true,
      dispatchRefresh: true,
      scheduleDetails: false,
      renderDeferredPanels: false,
      renderActivityPanel: true,
      schedulePolling: false
    });
    if (hasActiveAdminWorkRows(taskStatePayload)) {
      state.opsActiveAdminWorkLastActive = true;
      if (hasActivePipelineOrFetchRows(taskStatePayload)) {
        state.opsActivePipelineOrFetchLastActive = true;
      }
      loadActiveOpsSupplementalData(renderToken, {
        fromPoll: false
      }).finally(() => {
        if (renderToken === currentRenderToken() && (
          state.opsActiveAdminWorkLastActive || hasActiveAdminWorkRows()
        )) {
          schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
        }
      }).catch(() => {});
    }
    return { taskStatePayload, historyPayload };
  }

  return { applyBootstrapPayload };
}
