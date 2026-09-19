import { isPlainObject } from "../../domain/ops-shape-utils.js";
import {
  normalizePipelineSchedulePayload,
  hasKnownPipelineSchedule,
  getPipelineScheduleRenderModel,
  isTrustedBootstrapSchedule
} from "../../domain/ops-schedule-model.js";

const OPS_AUTHORITY_RETRY_BASE_MS = 3000;
const OPS_AUTHORITY_RETRY_MAX_MS = 30000;

/**
 * Pipeline schedule model ownership: caching the accepted schedule, rendering
 * it, retrying a failed load, and wiring the save controls.
 */
export function createOpsPipelineSchedule({
  state,
  refs,
  postBridge,
  showToast,
  getErrorMessage,
  markStep,
  loadOpsHealthData,
  loadPipelineScheduleData,
  maybeUnrefTimer,
  renderAdminOpsScheduleImpl,
  getScheduleElement
}) {
  function renderPipelineScheduleModel() {
    renderAdminOpsScheduleImpl(
      getScheduleElement(),
      getPipelineScheduleRenderModel(state.pipelineScheduleModel, {
        hasError: Boolean(state.pipelineScheduleLastError)
      }),
      state.latestOpsHealthCache
    );
  }

  function rememberPipelineSchedule(schedule) {
    if (!hasKnownPipelineSchedule(schedule)) return false;
    const existingPipeline = isPlainObject(state.pipelineScheduleModel?.pipeline)
      ? state.pipelineScheduleModel.pipeline
      : {};
    const incomingPipeline = isPlainObject(schedule?.pipeline) ? schedule.pipeline : {};
    const incomingNextRunAt = String(incomingPipeline.nextRunAt || "").trim();
    const existingNextRunAt = String(existingPipeline.nextRunAt || "").trim();
    const preserveExistingNextRun = Boolean(
      !incomingNextRunAt
      && existingNextRunAt
      && incomingPipeline.scheduleStatusRefreshing === true
    );
    const mergedPipeline = preserveExistingNextRun
      ? {
          ...incomingPipeline,
          nextRunAt: existingPipeline.nextRunAt,
          lastPipelineFinishedAt: incomingPipeline.lastPipelineFinishedAt || existingPipeline.lastPipelineFinishedAt || "",
          scheduleStatusRefreshing: false
        }
      : incomingPipeline;
    const acceptedSchedule = { ...schedule, pipeline: mergedPipeline };
    state.pipelineScheduleModel = acceptedSchedule;
    state.pipelineScheduleLastError = "";
    state.pipelineScheduleFailureCount = acceptedSchedule?.pipeline?.scheduleStatusRefreshing
      ? Math.max(1, Number(state.pipelineScheduleFailureCount || 0))
      : 0;
    markStep("admin_pipeline_schedule_model_loaded", {
      enabled: Boolean(acceptedSchedule?.pipeline?.enabled),
      intervalHours: Number(acceptedSchedule?.pipeline?.intervalHours || 0),
      nextRunAt: String(acceptedSchedule?.pipeline?.nextRunAt || "")
    });
    return true;
  }

  function schedulePipelineScheduleRetry() {
    if (state.pipelineScheduleRetryTimer) return;
    const failures = Math.max(1, Number(state.pipelineScheduleFailureCount || 1));
    const delayMs = Math.min(OPS_AUTHORITY_RETRY_MAX_MS, OPS_AUTHORITY_RETRY_BASE_MS * (2 ** Math.min(3, failures - 1)));
    state.pipelineScheduleRetryTimer = maybeUnrefTimer(setTimeout(() => {
      state.pipelineScheduleRetryTimer = null;
      loadPipelineScheduleData({ force: true, silent: true }).catch(() => {});
    }, delayMs));
  }

  async function handlePipelineScheduleSave(button) {
    const root = refs.adminOpsScheduleEl;
    if (!root) return;
    const enabledEl = root.querySelector?.('[data-ui="admin-pipeline-schedule-enabled"]');
    const intervalEl = root.querySelector?.('[data-ui="admin-pipeline-schedule-interval"]');
    const intervalHours = Number(intervalEl?.value || 0);
    if (!Number.isInteger(intervalHours) || intervalHours < 1 || intervalHours > 168) {
      showToast("Pipeline schedule interval must be between 1 and 168 hours.", "error");
      return;
    }
    if (button) button.disabled = true;
    try {
      const result = await postBridge("/tasks/jobs-pipeline-schedule", {
        enabled: Boolean(enabledEl?.checked),
        intervalHours
      });
      if (result?.ok === false) {
        throw new Error(String(result?.error || "schedule save failed"));
      }
      rememberPipelineSchedule(normalizePipelineSchedulePayload(result));
      renderPipelineScheduleModel();
      showToast("Pipeline schedule saved.", "success");
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Could not save pipeline schedule: ${getErrorMessage(err)}`, "error");
    } finally {
      if (button) button.disabled = false;
    }
  }

  function setupPipelineScheduleControls() {
    const root = refs.adminOpsScheduleEl;
    if (!root || state.adminPipelineScheduleControlsInitialized) return;
    if (typeof root.addEventListener !== "function") return;
    state.adminPipelineScheduleControlsInitialized = true;
    root.addEventListener("click", event => {
      const button = event.target?.closest?.('[data-action="save-pipeline-schedule"]');
      if (!button) return;
      event.preventDefault?.();
      handlePipelineScheduleSave(button).catch(() => {});
    });
  }

  function seedFromBootstrapPayload(payload) {
    // Seed the schedule model straight from the bootstrap payload so the Ops
    // panel paints real settings on first render instead of waiting for the
    // first poll (previously ~10s of "loading schedule..." after every open).
    // Degraded bootstrap schedules are not trusted over an existing model.
    if (isTrustedBootstrapSchedule(payload)) {
      rememberPipelineSchedule(normalizePipelineSchedulePayload({ schedule: payload.schedule }));
    }
  }

  return {
    renderPipelineScheduleModel,
    rememberPipelineSchedule,
    schedulePipelineScheduleRetry,
    setupPipelineScheduleControls,
    seedFromBootstrapPayload
  };
}
