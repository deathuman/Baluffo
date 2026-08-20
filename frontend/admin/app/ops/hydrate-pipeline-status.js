export function createPipelineStatusHydration({
  state,
  measuredGetBridge,
  buildPipelineTaskStatePayload,
  shouldKeepExistingActiveTaskState,
  hasActiveRows,
  hasActivePipelineOrFetchRows,
  canHydrateCompactDuringActiveRun,
  markFetchKpisDeferredDuringActiveRun,
  hasPossibleActiveRunEvidence,
  markOpsDegradedActive,
  stopPipelineStatusPolling,
  schedulePipelineStatusPolling,
  queueIdleRecoveryHealthLoad,
  getOpsPollIntervalMs,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  renderOpsHealthSnapshot,
  currentRenderToken,
  isStaleRenderToken,
  JOBS_PIPELINE_STATUS_PATH
}) {
  let pipelineStatusLoad = null;

  async function loadPipelineStatusFallbackData(renderToken = currentRenderToken(), options = {}) {
    const shouldScheduleNext = options?.scheduleNext !== false;
    try {
      if (!pipelineStatusLoad) {
        pipelineStatusLoad = measuredGetBridge(
          JOBS_PIPELINE_STATUS_PATH,
          "admin_jobs_pipeline_status_fallback_fetch",
          { enabled: false }
        ).finally(() => {
          pipelineStatusLoad = null;
        });
      }
      const payload = await pipelineStatusLoad;
      const activeRenderToken = isStaleRenderToken(renderToken) ? currentRenderToken() : renderToken;
      const taskStatePayload = buildPipelineTaskStatePayload(payload);
      if (!taskStatePayload) {
        if (getCachedTaskStatePayload()?.source === "pipeline-status") {
          state.latestOpsTaskStatePayload = { tasks: [], count: 0, summary: true, source: "pipeline-status" };
          renderOpsHealthSnapshot(activeRenderToken, state.latestOpsHealthCache || {}, {
            taskStatePayload: getCachedTaskStatePayload(),
            registryConflictsPayload: getCachedRegistryConflictsPayload(),
            syncTaskState: true,
            renderDeferredPanels: false,
            renderActivityPanel: true,
            schedulePolling: false
          });
        }
        stopPipelineStatusPolling();
        if (options?.fromPoll && shouldScheduleNext) {
          if (hasPossibleActiveRunEvidence({ includeRecent: false })) {
            markOpsDegradedActive("pipeline_status_idle_task_state_unknown");
            schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
          } else {
            queueIdleRecoveryHealthLoad({ summary: true });
          }
        }
        return payload || null;
      }
      const cachedTaskStatePayload = getCachedTaskStatePayload();
      if (shouldKeepExistingActiveTaskState(cachedTaskStatePayload, taskStatePayload, hasActiveRows)) {
        if (shouldScheduleNext) {
          schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
        }
        return payload || null;
      }
      state.latestOpsTaskStatePayload = taskStatePayload;
      state.taskStateUnavailable = false;
      state.waitingForTaskState = false;
      if (hasActivePipelineOrFetchRows(taskStatePayload) && !canHydrateCompactDuringActiveRun()) {
        markFetchKpisDeferredDuringActiveRun();
      }
      renderOpsHealthSnapshot(activeRenderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload,
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        syncTaskState: true,
        renderDeferredPanels: false,
        renderActivityPanel: true,
        schedulePolling: false
      });
      if (shouldScheduleNext) {
        schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
      }
      return payload || null;
    } catch {
      if (hasPossibleActiveRunEvidence({ includeRecent: false })) {
        markOpsDegradedActive("pipeline_status_unavailable");
      }
      if (shouldScheduleNext && hasPossibleActiveRunEvidence()) {
        schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
      }
      return hasPossibleActiveRunEvidence() ? { active: true, degradedActive: true } : null;
    }
  }

  return { loadPipelineStatusFallbackData };
}
