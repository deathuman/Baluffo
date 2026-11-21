import { hasUsefulValue, isPlainObject } from "./ops-shape-utils.js";
import { mergeOpsHealth } from "./ops-merge-model.js";

export const ACTIVE_PIPELINE_KPI_DELAYED_LABEL = "Updating while job is running.";
export const FETCH_KPI_LOADING_LABEL = "Loading latest fetch KPI...";
const FETCH_KPI_NOT_MEASURED_LABEL = "Not measured (no fetch in the last 7 days)";
const FETCH_KPI_UNAVAILABLE_LABEL = "Not available";
const FETCH_KPI_NO_SUCCESS_LABEL = "No successful fetch yet";

export function hasFetchKpiValues(kpis = {}) {
  return [
    "lastSuccessfulFetchAt",
    "lastSuccessfulFetchAge",
    "sevenDayFetchSuccessRate",
    "avgFetchDurationMs7d",
    "medianFetchDurationMs7d",
    "p90FetchDurationMs7d",
    "fetchDurationSampleCount7d",
    "failedSourceRatioLatest"
  ].some(key => hasUsefulValue(kpis?.[key]));
}

// The renderer treats an absent rate as its pending state, so an unmeasured window is
// passed to it without the rate. This lives here rather than in the shell renderer: the
// Admin shell is at its hard bundle ceiling, and this module rides the lazy Ops chunk.
export function maskUnmeasuredFetchSuccessRate(kpis = {}) {
  if (!isPlainObject(kpis) || kpis.fetchRunCount7d !== 0) return kpis || {};
  const masked = { ...kpis };
  delete masked.sevenDayFetchSuccessRate;
  return masked;
}

export function buildFetchKpiPendingLabels(health = {}, _activePipelineOrFetch = false) {
  const fetchKpisLoaded = Boolean(health?.fetchKpisLoaded);
  const fetchKpisDelayed = Boolean(health?.fetchKpisDelayedDuringActiveRun);
  // Zero runs is a measurement of an empty window, not a missing value: the card
  // says so instead of printing the rate's 0.0 as a total failure.
  const fetchUnmeasured = health?.kpis?.fetchRunCount7d === 0;
  const fetchNeverRun = Array.isArray(health?.alerts)
    && health.alerts.some(alert => String(alert?.id || "") === "fetch_never_run");
  if (fetchKpisDelayed) {
    if (hasFetchKpiValues(health?.kpis || {})) {
      return { default: ACTIVE_PIPELINE_KPI_DELAYED_LABEL };
    }
    return {
      default: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      lastSuccessfulFetchAge: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      lastSuccessfulFetchAt: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      sevenDayFetchSuccessRate: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      avgFetchDurationMs7d: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      medianFetchDurationMs7d: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      p90FetchDurationMs7d: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      fetchDurationSampleCount7d: ACTIVE_PIPELINE_KPI_DELAYED_LABEL,
      failedSourceRatioLatest: ACTIVE_PIPELINE_KPI_DELAYED_LABEL
    };
  }
  if (fetchKpisLoaded) {
    const generic = FETCH_KPI_UNAVAILABLE_LABEL;
    return {
      default: generic,
      lastSuccessfulFetchAge: fetchNeverRun ? FETCH_KPI_NO_SUCCESS_LABEL : generic,
      lastSuccessfulFetchAt: fetchNeverRun ? FETCH_KPI_NO_SUCCESS_LABEL : generic,
      sevenDayFetchSuccessRate: fetchUnmeasured ? FETCH_KPI_NOT_MEASURED_LABEL : generic,
      avgFetchDurationMs7d: generic,
      medianFetchDurationMs7d: generic,
      p90FetchDurationMs7d: generic,
      fetchDurationSampleCount7d: generic,
      failedSourceRatioLatest: generic
    };
  }
  const pending = fetchKpisDelayed
    ? ACTIVE_PIPELINE_KPI_DELAYED_LABEL
    : FETCH_KPI_LOADING_LABEL;
  return { default: pending };
}

export function maskDeferredFetchKpisForRender(kpis = {}, health = {}) {
  if (!health?.fetchKpisDelayedDuringActiveRun || !isPlainObject(kpis)) {
    return kpis || {};
  }
  if (hasFetchKpiValues(kpis)) {
    return kpis || {};
  }
  const masked = { ...kpis };
  [
    "lastSuccessfulFetchAge",
    "lastSuccessfulFetchAt",
    "sevenDayFetchSuccessRate",
    "fetchRunCount7d",
    "avgFetchDurationMs7d",
    "medianFetchDurationMs7d",
    "p90FetchDurationMs7d",
    "fetchDurationSampleCount7d",
    "failedSourceRatioLatest",
    "sourceOutcomeSummary",
    "pendingSourcesCount",
    "pendingApprovalsCount"
  ].forEach(key => {
    delete masked[key];
  });
  return masked;
}

export function markFetchKpisDeferredDuringActiveRun(state) {
  state.latestOpsHealthCache = mergeOpsHealth(
    state.latestOpsHealthCache || {},
    {
      fetchKpisDelayedDuringActiveRun: true,
      fetchKpisLoaded: false,
      summaryView: true
    },
    { summary: true }
  );
}
