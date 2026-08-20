import { isPlainObject } from "./ops-shape-utils.js";
import { isDegradedControlFallbackPayload } from "./ops-merge-model.js";

export function hasActionablePipelineSchedule(payload = {}) {
  const pipeline = isPlainObject(payload?.schedule?.pipeline)
    ? payload.schedule.pipeline
    : {};
  return Boolean(
    pipeline.nextAfterCurrentCompletes === true
    || String(pipeline.nextRunAt || "").trim()
    || pipeline.pending === true
    || pipeline.due === true
  );
}

export function normalizePipelineSchedulePayload(payload = {}) {
  if (
    isPlainObject(payload?.schedule)
    && isPlainObject(payload.schedule.pipeline)
    && !isDegradedControlFallbackPayload(payload)
  ) {
    return payload.schedule;
  }
  if (isPlainObject(payload?.pipeline)) {
    return { pipeline: { ...payload.pipeline } };
  }
  const saved = isPlainObject(payload?.savedConfig) ? payload.savedConfig : {};
  const status = isPlainObject(payload?.status) ? payload.status : {};
  if (!Object.keys(saved).length && !Object.keys(status).length) return {};
  const {
    scheduleDelayed: statusScheduleDelayed,
    scheduleAuthority: statusScheduleAuthority,
    ...statusFields
  } = status;
  const enabled = Object.prototype.hasOwnProperty.call(saved, "enabled")
    ? Boolean(saved.enabled)
    : Boolean(status.enabled);
  const intervalHours = Number(
    Object.prototype.hasOwnProperty.call(saved, "intervalHours")
      ? saved.intervalHours
      : status.intervalHours
  );
  const statusDelayed = Boolean(
    statusScheduleDelayed === true
    || String(statusScheduleAuthority || "").toLowerCase() === "degraded"
  );
  return {
    pipeline: {
      ...statusFields,
      enabled,
      ...(Number.isFinite(intervalHours) && intervalHours > 0 ? { intervalHours } : {}),
      ...(statusDelayed ? { scheduleStatusRefreshing: true } : {})
    }
  };
}

export function hasKnownPipelineSchedule(schedule) {
  const pipeline = isPlainObject(schedule?.pipeline) ? schedule.pipeline : {};
  const hasSavedConfig = Boolean(
    Object.prototype.hasOwnProperty.call(pipeline, "enabled")
    || Object.prototype.hasOwnProperty.call(pipeline, "intervalHours")
  );
  return Boolean(
    isPlainObject(schedule)
    && isPlainObject(pipeline)
    && Object.keys(pipeline).length > 0
    && pipeline.scheduleLoading !== true
    && (
      hasSavedConfig
      || (
        pipeline.scheduleAuthority !== "degraded"
        && pipeline.scheduleDelayed !== true
      )
    )
  );
}

export function hasPipelineScheduleNextRun(schedule) {
  return Boolean(String(schedule?.pipeline?.nextRunAt || "").trim());
}

export function getPipelineScheduleRenderModel(schedule, { hasError = false } = {}) {
  if (hasKnownPipelineSchedule(schedule)) {
    return schedule;
  }
  return {
    pipeline: {
      scheduleLoading: true,
      scheduleRetrying: Boolean(hasError)
    }
  };
}
