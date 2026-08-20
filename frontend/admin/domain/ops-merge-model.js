import { hasUsefulValue, isPlainObject } from "./ops-shape-utils.js";

const FETCH_KPI_ZERO_CAN_BE_PLACEHOLDER = new Set([
  "sevenDayFetchSuccessRate",
  "avgFetchDurationMs7d",
  "failedSourceRatioLatest"
]);

function mergeKpis(existing = {}, incoming = {}, { preserveExisting = false } = {}) {
  const result = isPlainObject(existing) ? { ...existing } : {};
  if (!isPlainObject(incoming)) return result;
  Object.entries(incoming).forEach(([key, value]) => {
    if (key === "registrySync" && isPlainObject(value)) {
      result.registrySync = mergePlainObjects(
        isPlainObject(result.registrySync) ? result.registrySync : {},
        value,
        { preserveUnknowns: preserveExisting }
      );
      return;
    }
    if (
      preserveExisting
      && Object.prototype.hasOwnProperty.call(result, key)
      && hasUsefulValue(result[key])
      && (
        !hasUsefulValue(value)
        || (FETCH_KPI_ZERO_CAN_BE_PLACEHOLDER.has(key) && Number(value) === 0)
      )
    ) {
      return;
    }
    if (hasUsefulValue(value) || !Object.prototype.hasOwnProperty.call(result, key)) {
      result[key] = value;
    }
  });
  return result;
}

function mergePlainObjects(existing = {}, incoming = {}, { preserveUnknowns = false } = {}) {
  const result = isPlainObject(existing) ? { ...existing } : {};
  if (!isPlainObject(incoming)) return result;
  Object.entries(incoming).forEach(([key, value]) => {
    if (
      preserveUnknowns
      && Object.prototype.hasOwnProperty.call(result, key)
      && hasUsefulValue(result[key])
      && !hasUsefulValue(value)
    ) {
      return;
    }
    result[key] = value;
  });
  return result;
}

export function mergeOpsHealth(existing = {}, incoming = {}, { summary = false } = {}) {
  const base = isPlainObject(existing) ? existing : {};
  const patch = isPlainObject(incoming) ? incoming : {};
  const preserveExisting = Boolean(summary);
  const patchAlertsAuthoritative = !summary || patch.alertsEvaluated === true;
  const merged = {
    ...base,
    ...patch,
    kpis: mergeKpis(base.kpis, patch.kpis, { preserveExisting })
  };
  if (!patchAlertsAuthoritative) {
    if (Object.prototype.hasOwnProperty.call(base, "status")) merged.status = base.status;
    if (Object.prototype.hasOwnProperty.call(base, "alerts")) merged.alerts = base.alerts;
    if (Object.prototype.hasOwnProperty.call(base, "suppressedAlertsCount")) {
      merged.suppressedAlertsCount = base.suppressedAlertsCount;
    }
    if (Object.prototype.hasOwnProperty.call(base, "alertsEvaluated")) {
      merged.alertsEvaluated = base.alertsEvaluated;
    }
    if (Object.prototype.hasOwnProperty.call(base, "alertBasis")) merged.alertBasis = base.alertBasis;
  }
  if (isPlainObject(base.schedule) || isPlainObject(patch.schedule)) {
    merged.schedule = mergePlainObjects(base.schedule, patch.schedule, {
      preserveUnknowns: preserveExisting
    });
  }
  return merged;
}

export function isDegradedControlFallbackPayload(payload = {}) {
  const source = String(payload?.source || "").toLowerCase();
  return Boolean(
    payload?.degraded === true
    && (
      source === "container-gateway-fallback"
      || source === "frontend-bootstrap-fallback"
      || source === "gateway-degraded"
    )
  );
}
