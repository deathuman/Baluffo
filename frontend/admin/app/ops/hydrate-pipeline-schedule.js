export function createPipelineScheduleHydration({
  state,
  markStep,
  measuredGetBridge,
  getErrorMessage,
  showToast,
  normalizePipelineSchedulePayload,
  rememberPipelineSchedule,
  renderPipelineScheduleModel,
  hasKnownPipelineSchedule,
  hasPipelineScheduleNextRun,
  schedulePipelineScheduleRetry,
  scheduleIdleOpsHeavyHydration,
  JOBS_PIPELINE_SCHEDULE_PATH,
  OPS_AUTHORITY_FETCH_TIMEOUT_MS,
  currentRenderToken
}) {
  let pipelineScheduleLoad = null;

  async function loadPipelineScheduleData(options = {}) {
    if (!options?.force && hasKnownPipelineSchedule(state.pipelineScheduleModel)) {
      renderPipelineScheduleModel();
      return state.pipelineScheduleModel || {};
    }
    if (pipelineScheduleLoad) return pipelineScheduleLoad;
    markStep("admin_pipeline_schedule_model_fetch_start");
    pipelineScheduleLoad = measuredGetBridge(
      JOBS_PIPELINE_SCHEDULE_PATH,
      "admin_pipeline_schedule_fetch",
      {
        enabled: !options?.silent,
        requestOptions: { timeoutMs: OPS_AUTHORITY_FETCH_TIMEOUT_MS }
      }
    )
      .then(payload => {
        const schedule = normalizePipelineSchedulePayload(payload);
        const accepted = rememberPipelineSchedule(schedule);
        markStep("admin_pipeline_schedule_model_fetch_done", {
          ok: Boolean(accepted),
          enabled: Boolean(schedule?.pipeline?.enabled),
          intervalHours: Number(schedule?.pipeline?.intervalHours || 0),
          nextRunAt: String(schedule?.pipeline?.nextRunAt || "")
        });
        renderPipelineScheduleModel();
        if (schedule?.pipeline?.scheduleStatusRefreshing && !hasPipelineScheduleNextRun(state.pipelineScheduleModel)) {
          state.pipelineScheduleFailureCount = Math.max(1, Number(state.pipelineScheduleFailureCount || 0) + 1);
          schedulePipelineScheduleRetry();
        }
        if (!options?.deferIdleHydration) {
          scheduleIdleOpsHeavyHydration(currentRenderToken(), { silent: true });
        }
        return hasKnownPipelineSchedule(state.pipelineScheduleModel)
          ? state.pipelineScheduleModel
          : schedule;
      })
      .catch(err => {
        state.pipelineScheduleLastError = getErrorMessage(err);
        state.pipelineScheduleFailureCount = Math.max(1, Number(state.pipelineScheduleFailureCount || 0) + 1);
        markStep("admin_pipeline_schedule_model_fetch_done", {
          ok: false,
          error: String(state.pipelineScheduleLastError || "unknown error")
        });
        renderPipelineScheduleModel();
        schedulePipelineScheduleRetry();
        if (!options?.silent) {
          showToast(`Could not load pipeline schedule: ${getErrorMessage(err)}`, "error");
        }
        return null;
      })
      .finally(() => {
        pipelineScheduleLoad = null;
      });
    return pipelineScheduleLoad;
  }

  function ensurePipelineScheduleLoaded(options = {}) {
    if (!options?.force && hasKnownPipelineSchedule(state.pipelineScheduleModel)) return Promise.resolve(state.latestOpsHealthCache?.schedule || {});
    return loadPipelineScheduleData({ silent: true, ...options });
  }

  return { loadPipelineScheduleData, ensurePipelineScheduleLoaded };
}
