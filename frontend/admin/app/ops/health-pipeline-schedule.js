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
  let pipelineScheduleDraft = null;
  let pipelineScheduleDraftVersion = 0;
  let pipelineScheduleSave = null;
  let pipelineScheduleControlsRoot = null;
  let pipelineScheduleWriteGuard = null;

  function getScheduleControls(root = getScheduleElement()) {
    return {
      enabled: root?.querySelector?.('[data-ui="admin-pipeline-schedule-enabled"]') || null,
      interval: root?.querySelector?.('[data-ui="admin-pipeline-schedule-interval"]') || null,
      save: root?.querySelector?.('[data-action="save-pipeline-schedule"]') || null
    };
  }

  function readScheduleDraft(controls) {
    return {
      enabled: Boolean(controls.enabled?.checked),
      intervalValue: String(controls.interval?.value ?? "")
    };
  }

  function applyScheduleDraftAndPendingState(root = getScheduleElement()) {
    if (!root) return;
    const controls = getScheduleControls(root);
    if (pipelineScheduleDraft) {
      if (controls.enabled) controls.enabled.checked = pipelineScheduleDraft.enabled;
      if (controls.interval) controls.interval.value = pipelineScheduleDraft.intervalValue;
    }
    if (pipelineScheduleSave) {
      root.setAttribute?.("aria-busy", "true");
      if (controls.save) {
        controls.save.disabled = true;
        controls.save.textContent = "Saving…";
        controls.save.setAttribute?.("aria-busy", "true");
        if (!controls.save.setAttribute) controls.save.ariaBusy = "true";
      }
      return;
    }
    root.removeAttribute?.("aria-busy");
    if (controls.save) {
      controls.save.removeAttribute?.("aria-busy");
      if (!controls.save.removeAttribute) delete controls.save.ariaBusy;
      controls.save.textContent = "Save";
      controls.save.disabled = !hasKnownPipelineSchedule(state.pipelineScheduleModel)
        || state.pipelineScheduleModel?.pipeline?.scheduleLoading === true;
    }
  }

  function renderPipelineScheduleModel({ force = false } = {}) {
    const root = getScheduleElement();
    if (force && root?.dataset) delete root.dataset.opsScheduleSig;
    renderAdminOpsScheduleImpl(
      root,
      getPipelineScheduleRenderModel(state.pipelineScheduleModel, {
        hasError: Boolean(state.pipelineScheduleLastError)
      }),
      state.latestOpsHealthCache
    );
    // The schedule renderer may replace its controls when the model signature
    // changes, so pending/draft presentation is controller-owned and reapplied.
    applyScheduleDraftAndPendingState(root);
    setupPipelineScheduleControls();
  }

  function rememberPipelineSchedule(schedule, { source = "load" } = {}) {
    if (!hasKnownPipelineSchedule(schedule)) return false;
    if (pipelineScheduleWriteGuard && source !== "write") {
      const incomingPipeline = isPlainObject(schedule?.pipeline) ? schedule.pipeline : {};
      const matchesSavedSettings = Boolean(
        Object.prototype.hasOwnProperty.call(incomingPipeline, "enabled")
        && Object.prototype.hasOwnProperty.call(incomingPipeline, "intervalHours")
        && Boolean(incomingPipeline.enabled) === pipelineScheduleWriteGuard.payload.enabled
        && Number(incomingPipeline.intervalHours) === pipelineScheduleWriteGuard.payload.intervalHours
      );
      if (!matchesSavedSettings) {
        // Keep rejecting reads until one confirms the committed settings; any
        // number of older in-flight GETs may still settle after the POST.
        return false;
      }
      if (pipelineScheduleWriteGuard.settled && !pipelineScheduleSave) {
        // A post-write read confirms the server baseline. Later GETs can once
        // again report genuine external settings changes.
        pipelineScheduleWriteGuard = null;
      }
    }
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

  async function handlePipelineScheduleSave() {
    const root = getScheduleElement();
    if (!root || pipelineScheduleSave) return;
    const controls = getScheduleControls(root);
    if (!pipelineScheduleDraft) {
      pipelineScheduleDraft = readScheduleDraft(controls);
      pipelineScheduleDraftVersion += 1;
    }
    const draft = pipelineScheduleDraft;
    const intervalHours = Number(draft.intervalValue || 0);
    if (!Number.isInteger(intervalHours) || intervalHours < 1 || intervalHours > 168) {
      showToast("Pipeline schedule interval must be between 1 and 168 hours.", "error");
      return;
    }

    // Snapshot the payload before the asynchronous boundary. Subsequent edits
    // are kept as a newer draft and are never overwritten by this response.
    const submittedDraft = { ...draft };
    const submittedPayload = {
      enabled: submittedDraft.enabled,
      intervalHours
    };
    const save = { version: pipelineScheduleDraftVersion, payload: submittedPayload };
    pipelineScheduleWriteGuard = { version: save.version, payload: submittedPayload, settled: false };
    pipelineScheduleSave = save;
    applyScheduleDraftAndPendingState(root);

    let persistenceSaved = false;
    try {
      const result = await postBridge("/tasks/jobs-pipeline-schedule", submittedPayload);
      if (result?.ok === false) {
        throw new Error(String(result?.error || "schedule save failed"));
      }
      persistenceSaved = true;
      if (pipelineScheduleWriteGuard?.version === save.version) {
        pipelineScheduleWriteGuard.settled = true;
      }
      const returnedSchedule = normalizePipelineSchedulePayload(result);
      if (!rememberPipelineSchedule(returnedSchedule, { source: "write" })) {
        const existingPipeline = isPlainObject(state.pipelineScheduleModel?.pipeline)
          ? state.pipelineScheduleModel.pipeline
          : {};
        rememberPipelineSchedule({
          pipeline: {
            ...existingPipeline,
            enabled: submittedPayload.enabled,
            intervalHours: submittedPayload.intervalHours
          }
        });
      }
      if (pipelineScheduleDraftVersion === save.version) {
        pipelineScheduleDraft = null;
      }
      renderPipelineScheduleModel();
      showToast("Pipeline schedule saved.", "success");
    } catch (err) {
      if (!persistenceSaved) {
        if (pipelineScheduleWriteGuard?.version === save.version) pipelineScheduleWriteGuard = null;
        showToast(`Could not save pipeline schedule: ${getErrorMessage(err)}`, "error");
      }
      return;
    } finally {
      if (pipelineScheduleSave === save) {
        pipelineScheduleSave = null;
        renderPipelineScheduleModel();
      }
    }

    // A health refresh is subsequent read-side work. Its failure must not
    // recast an acknowledged schedule write as a failed save.
    try {
      await loadOpsHealthData();
    } catch (err) {
      showToast(`Pipeline schedule saved, but Ops health could not refresh: ${getErrorMessage(err)}`, "warning");
    }
  }

  function setupPipelineScheduleControls() {
    const root = getScheduleElement() || refs.adminOpsScheduleEl;
    if (!root || root === pipelineScheduleControlsRoot) return;
    if (typeof root.addEventListener !== "function") return;
    pipelineScheduleControlsRoot = root;
    const captureDraft = event => {
      const target = event.target;
      if (!target?.matches?.('[data-ui="admin-pipeline-schedule-enabled"], [data-ui="admin-pipeline-schedule-interval"]')) return;
      pipelineScheduleDraft = readScheduleDraft(getScheduleControls(getScheduleElement()));
      pipelineScheduleDraftVersion += 1;
      applyScheduleDraftAndPendingState(getScheduleElement());
    };
    root.addEventListener("input", captureDraft);
    root.addEventListener("change", captureDraft);
    root.addEventListener("click", event => {
      const button = event.target?.closest?.('[data-action="save-pipeline-schedule"]');
      if (!button) return;
      event.preventDefault?.();
      handlePipelineScheduleSave().catch(() => {});
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
