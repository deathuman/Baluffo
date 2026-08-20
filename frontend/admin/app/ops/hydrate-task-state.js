export function createTaskStateHydration({
  state,
  measuredGetBridge,
  taskStateController,
  hasActiveRows,
  hasOptimisticRows,
  hasPossibleActiveRunEvidence,
  markOpsDegradedActive,
  renderOpsHealthSnapshot,
  getCachedTaskStatePayload,
  getCachedRegistryConflictsPayload,
  isStaleRenderToken,
  OPS_TASK_STATE_SUMMARY_PATH
}) {
  let taskStateSummaryLoad = null;
  let taskStateSummaryLoadToken = 0;

  async function loadTaskStateSummaryData(renderToken, options = {}) {
    const previousTaskStatePayload = getCachedTaskStatePayload();
    try {
      const canReuseTaskStateLoad = Boolean(
        taskStateSummaryLoad
        && (options?.fromPoll || taskStateSummaryLoadToken === renderToken)
      );
      if (!canReuseTaskStateLoad) {
        taskStateSummaryLoadToken = renderToken;
        taskStateSummaryLoad = measuredGetBridge(
          OPS_TASK_STATE_SUMMARY_PATH,
          "admin_ops_task_summary_fetch",
          { enabled: !options?.fromPoll }
        ).finally(() => {
          taskStateSummaryLoad = null;
          taskStateSummaryLoadToken = 0;
        });
      }
      const payload = await taskStateSummaryLoad;
      if (isStaleRenderToken(renderToken)) return null;
      const taskStatePayload = taskStateController.resolveTaskStatePayload({
        status: "fulfilled",
        value: payload
      });
      state.latestOpsTaskStatePayload = taskStatePayload || {};
      state.taskStateUnavailable = Boolean(taskStatePayload?.taskStateUnavailable);
      const renderActivityPanel = Boolean(options?.summary)
        || !options?.fromPoll
        || hasActiveRows(taskStatePayload)
        || hasActiveRows(previousTaskStatePayload)
        || hasOptimisticRows();
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload,
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        syncTaskState: true,
        renderDeferredPanels: false,
        renderActivityPanel,
        schedulePolling: options?.schedulePolling !== false
      });
      return taskStatePayload;
    } catch (err) {
      if (isStaleRenderToken(renderToken)) return null;
      if (hasPossibleActiveRunEvidence({ includeRecent: false })) {
        markOpsDegradedActive("task_state_summary_unavailable");
      }
      const taskStatePayload = taskStateController.resolveTaskStatePayload({
        status: "rejected",
        reason: err
      });
      const fallbackTaskStatePayload = hasActiveRows(previousTaskStatePayload)
        ? previousTaskStatePayload
        : taskStatePayload || {};
      state.latestOpsTaskStatePayload = fallbackTaskStatePayload || {};
      state.taskStateUnavailable = !hasActiveRows(previousTaskStatePayload);
      const renderActivityPanel = Boolean(options?.summary)
        || !options?.fromPoll
        || hasActiveRows(fallbackTaskStatePayload)
        || hasActiveRows(previousTaskStatePayload)
        || hasOptimisticRows();
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
        taskStatePayload: fallbackTaskStatePayload,
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        syncTaskState: true,
        renderDeferredPanels: false,
        renderActivityPanel,
        schedulePolling: options?.schedulePolling !== false
      });
      return null;
    }
  }

  return { loadTaskStateSummaryData };
}
