export function createActiveOpsHydration({
  state,
  loadTaskStateSummaryData,
  loadPipelineStatusFallbackData,
  getCachedTaskStatePayload,
  hasActivePipelineOrFetchRows,
  hasActiveAdminWorkRows,
  hasPossibleActiveRunEvidence,
  hasPossiblePipelineOrFetchEvidence,
  clearOpsDegradedActive,
  markOpsDegradedActive,
  clearAllPendingOpsAborts,
  markActiveIdleRecoveryCooldown,
  notifyActiveAdminWorkIdleIfNeeded,
  schedulePipelineStatusPolling,
  stopPipelineStatusPolling,
  queueIdleRecoveryHealthLoad,
  getOpsPollIntervalMs,
  currentRenderToken,
  isStaleRenderToken
}) {
  async function loadActiveOpsSupplementalData(renderToken = currentRenderToken(), options = {}) {
    const taskStatePromise = loadTaskStateSummaryData(renderToken, {
      ...options,
      fromPoll: Boolean(options?.fromPoll),
      summary: true,
      schedulePolling: false
    }).then(value => ({ loaded: Boolean(value), payload: value || null })).catch(() => ({ loaded: false, payload: null }));
    const [taskStateResult] = await Promise.allSettled([taskStatePromise]);
    if (taskStateResult.status === "fulfilled" && taskStateResult.value?.payload) {
      return options?.returnMeta
        ? {
            taskStatePayload: taskStateResult.value.payload,
            taskStateLoaded: Boolean(taskStateResult.value.loaded)
          }
        : taskStateResult.value.payload;
    }
    const fallbackPayload = getCachedTaskStatePayload();
    return options?.returnMeta
      ? { taskStatePayload: fallbackPayload, taskStateLoaded: false }
      : fallbackPayload;
  }

  async function loadActiveOpsSummaryData(renderToken = currentRenderToken(), options = {}) {
    const cachedTaskStatePayload = getCachedTaskStatePayload();
    const wasPipelineOrFetchActive = Boolean(
      state.opsActivePipelineOrFetchLastActive
      || hasActivePipelineOrFetchRows(cachedTaskStatePayload)
    );
    const wasActive = Boolean(
      state.opsActiveAdminWorkLastActive
      || wasPipelineOrFetchActive
      || hasActiveAdminWorkRows(cachedTaskStatePayload)
      || state.adminBusyState?.liveSyncRunning
    );
    const pipelinePayload = await loadPipelineStatusFallbackData(renderToken, {
      ...options,
      fromPoll: Boolean(options?.fromPoll),
      scheduleNext: false
    });
    if (isStaleRenderToken(renderToken)) return pipelinePayload || null;
    const supplemental = await loadActiveOpsSupplementalData(renderToken, {
      ...options,
      fromPoll: Boolean(options?.fromPoll),
      returnMeta: true
    });
    if (isStaleRenderToken(renderToken)) return pipelinePayload || null;
    const taskStatePayload = supplemental?.taskStatePayload || getCachedTaskStatePayload();
    const taskStateLoaded = Boolean(supplemental?.taskStateLoaded);
    const hasActiveTaskRows = hasActiveAdminWorkRows(taskStatePayload);
    const hasActivePipelineOrFetchTaskRows = hasActivePipelineOrFetchRows(taskStatePayload);
    const pipelineKnownIdle = Boolean(pipelinePayload && pipelinePayload.active === false && !pipelinePayload.degradedActive);
    const positiveIdle = Boolean(
      pipelineKnownIdle
      && taskStateLoaded
      && !hasActiveTaskRows
    );
    const degradedActive = Boolean(
      !positiveIdle
      && (
        pipelinePayload?.degradedActive
        || (!taskStateLoaded && hasPossibleActiveRunEvidence())
        || (!pipelinePayload && hasPossibleActiveRunEvidence())
      )
    );
    const pipelineOrFetchActive = Boolean(
      pipelinePayload?.active
      || hasActivePipelineOrFetchTaskRows
      || (degradedActive && hasPossiblePipelineOrFetchEvidence())
    );
    const isActive = Boolean(pipelinePayload?.active || hasActiveTaskRows || degradedActive);
    if (positiveIdle) {
      clearOpsDegradedActive();
      clearAllPendingOpsAborts();
      state.opsActiveAdminWorkLastActive = false;
      state.opsActivePipelineOrFetchLastActive = false;
      markActiveIdleRecoveryCooldown();
    } else if (degradedActive) {
      markOpsDegradedActive("active_summary_unresolved");
    }
    notifyActiveAdminWorkIdleIfNeeded(wasActive, isActive, {
      wasPipelineOrFetchActive,
      pipelineOrFetchActive
    });
    if (isActive) {
      schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
    } else {
      clearOpsDegradedActive();
      state.opsActiveAdminWorkLastActive = false;
      state.opsActivePipelineOrFetchLastActive = false;
      stopPipelineStatusPolling();
      if (!options?.summaryOnly) {
        queueIdleRecoveryHealthLoad({ summary: true });
      }
    }
    if (options?.returnMeta) {
      return {
        pipelinePayload: pipelinePayload || null,
        taskStatePayload,
        taskStateLoaded,
        hasActiveTaskRows,
        hasActivePipelineOrFetchTaskRows,
        degradedActive,
        isActive,
        positiveIdle
      };
    }
    return pipelinePayload || null;
  }

  return { loadActiveOpsSupplementalData, loadActiveOpsSummaryData };
}
