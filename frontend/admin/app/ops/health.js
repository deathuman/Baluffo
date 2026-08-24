import { deriveFetcherFailureSummary } from "../../domain.js";
import {
  renderAdminOpsAlerts,
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsHistory,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsTrends,
  renderDiscoveryCandidateReviewHtml
} from "../../render.js?v=25";
import { renderAdminSourcePolicyReview } from "../../render/source-policy-review.js?v=6";
import { renderAdminRegistryConflicts } from "../../render/registry-conflicts.js?v=6";
import {
  ACTIVE_ADMIN_TASK_TYPES,
  ACTIVE_PIPELINE_OR_FETCH_TASK_TYPES,
  hasActiveAdminTaskRows
} from "../active-work-policy.js";
import { getObjectValue, isPlainObject } from "../../domain/ops-shape-utils.js";
import {
  mergeOpsHealth,
  isDegradedControlFallbackPayload
} from "../../domain/ops-merge-model.js";
import {
  hasActionablePipelineSchedule,
  normalizePipelineSchedulePayload,
  hasKnownPipelineSchedule,
  hasPipelineScheduleNextRun,
  getPipelineScheduleRenderModel,
  isTrustedBootstrapSchedule
} from "../../domain/ops-schedule-model.js";
import {
  FETCH_KPI_LOADING_LABEL,
  buildFetchKpiPendingLabels,
  maskDeferredFetchKpisForRender,
  hasFetchKpiValues,
  markFetchKpisDeferredDuringActiveRun
} from "../../domain/ops-fetch-kpi-model.js";
import {
  getOpsAbortKey,
  buildOptimisticAbortRow,
  isAbortAcceptedResult
} from "../../domain/ops-abort-model.js";
import {
  getTaskRowType,
  getTaskRowRunId,
  shouldKeepExistingActiveTaskState,
  buildPipelineTaskStatePayload
} from "../../domain/ops-pipeline-status-model.js";
import {
  OPS_TAB_KEYS,
  toDiscoveryBadgeState,
  isLoadedDiscoveryReport,
  isLoadedDedupPayload,
  hasRegistrySyncDetails,
  renderOpsTabBadges
} from "./health-badges.js";
import { createHistoryHydration } from "./hydrate-history.js";
import { createOverviewHydration } from "./hydrate-overview.js";
import { createPipelineScheduleHydration } from "./hydrate-pipeline-schedule.js";
import { createTaskStateHydration } from "./hydrate-task-state.js";
import { createRegistryConflictsHydration } from "./hydrate-registry-conflicts.js";
import { createFetchKpisHydration } from "./hydrate-fetch-kpis.js";
import { createDashboardHealthHydration } from "./hydrate-dashboard-health.js";
import { createPipelineStatusHydration } from "./hydrate-pipeline-status.js";
import { createActiveOpsHydration } from "./hydrate-active-ops.js";
import { createRegistrySyncHydration } from "./hydrate-registry-sync.js";
import { createTabCountsHydration } from "./hydrate-tab-counts.js";
import { createOpsActions } from "./health-actions.js";

const OPS_TASK_STATE_SUMMARY_PATH = "/ops/task-state?view=summary";
const OPS_DASHBOARD_HEALTH_SUMMARY_PATH = "/ops/dashboard-health?view=summary";
const JOBS_PIPELINE_STATUS_PATH = "/tasks/run-jobs-pipeline-status";
const JOBS_PIPELINE_SCHEDULE_PATH = "/tasks/jobs-pipeline-schedule";
const OPS_FETCH_KPIS_SUMMARY_PATH = "/ops/fetch-kpis?view=summary";
const OPS_TAB_COUNTS_SUMMARY_PATH = "/admin/ops-tab-counts?view=summary";
const OPS_HISTORY_STARTUP_PATH = "/ops/history?limit=2";
const OPS_HISTORY_DETAIL_PATH = "/ops/history?limit=80";
const OPS_AUTHORITY_FETCH_TIMEOUT_MS = 5000;
const OPS_AUTHORITY_RETRY_BASE_MS = 3000;
const OPS_AUTHORITY_RETRY_MAX_MS = 30000;
const OPS_IDLE_HEAVY_HYDRATION_DELAY_MS = 0;
const OPS_FETCHER_METRICS_DETAIL_PATH = "/ops/fetcher-metrics?windowRuns=80";
const OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH = "/ops/discovery-audit-artifacts";
const OPS_TASK_FAILURE_ATTEMPTS_PATH = "/ops/task-failure-attempts";
const OPS_PERFORMANCE_PROFILE_PATH = "/ops/performance-profile";
const SOURCE_POLICY_DETAIL_PATH = "/source-policy/recommendations";
const REGISTRY_CONFLICTS_SUMMARY_PATH = "/registry/conflicts?view=summary";
const REGISTRY_CONFLICTS_DETAIL_PATH = "/registry/conflicts";
const OPS_DEGRADED_ACTIVE_TTL_MS = 30000;
const OPS_HEAVY_ROUTE_BACKOFF_BASE_MS = 5000;
const OPS_HEAVY_ROUTE_BACKOFF_MAX_MS = 30000;
const OPS_HEAVY_ROUTE_DASHBOARD = "dashboard-health";
const OPS_HEAVY_ROUTE_REGISTRY_CONFLICTS = "registry-conflicts";
const OPS_HEAVY_ROUTE_TAB_COUNTS = "ops-tab-counts";
const ACTIVE_IDLE_RECOVERY_COOLDOWN_MS = 1500;

function historyRunKey(row) {
  return `${String(row?.taskType || row?.type || "").trim().toLowerCase()}|${String(row?.runId || row?.id || "").trim()}`;
}

export function mergeOpsHistoryPayload(existing, incoming, limit = 80) {
  const base = getObjectValue(existing);
  const patch = getObjectValue(incoming);
  const seen = new Set();
  const merged = [];
  [...(Array.isArray(patch?.runs) ? patch.runs : []), ...(Array.isArray(base?.runs) ? base.runs : [])].forEach(row => {
    if (!row || typeof row !== "object") return;
    const key = historyRunKey(row);
    if (seen.has(key)) return;
    seen.add(key);
    merged.push(row);
  });
  return {
    ...base,
    ...patch,
    runs: merged.slice(0, Math.max(1, Number(limit) || merged.length)),
    count: Math.max(0, Number(limit) || merged.length),
    summaryView: true
  };
}

function maybeUnrefTimer(timer) {
  if (timer && typeof timer.unref === "function") {
    try {
      timer.unref();
    } catch {
      // Best-effort unref for Node-style timers.
    }
  }
  return timer;
}

export function createOpsHealthController({
  state,
  refs,
  getBridge,
  postBridge,
  deriveAdminRunsModel,
  getOpsPollIntervalMs,
  renderAdminOpsAlerts: renderAdminOpsAlertsImpl = renderAdminOpsAlerts,
  renderAdminOpsKpis: renderAdminOpsKpisImpl = renderAdminOpsKpis,
  renderAdminOpsSchedule: renderAdminOpsScheduleImpl = renderAdminOpsSchedule,
  renderAdminOpsDedupLists: renderAdminOpsDedupListsImpl = renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics: renderAdminOpsFetcherMetricsImpl = renderAdminOpsFetcherMetrics,
  renderAdminSourcePolicyReview: renderAdminSourcePolicyReviewImpl = renderAdminSourcePolicyReview,
  renderAdminRegistryConflicts: renderAdminRegistryConflictsImpl = renderAdminRegistryConflicts,
  renderAdminOpsTrends: renderAdminOpsTrendsImpl = renderAdminOpsTrends,
  renderAdminOpsHistory: renderAdminOpsHistoryImpl = renderAdminOpsHistory,
  setBusyFlag,
  showToast,
  getErrorMessage,
  adminDispatch,
  adminActions,
  escapeHtml,
  idlePollIntervalMs,
  taskStateController,
  getBridgeStatus: _getBridgeStatus,
  awaitBridgeReady = async () => true,
  loadLatestDiscoveryReport,
  onActivePipelineIdle,
  markAdminStep,
  measureAdminStep,
  activeHydrationPolicy = "protected",
  getFrontendPerfCounters = () => {
    try {
      return globalThis.__baluffoSnapshotFrontendPerfCounters?.() || {};
    } catch {
      return {};
    }
  },
  renderScheduler
}) {
  let initialBridgeReadyResolved = false;
  let opsRenderToken = 0;
  let idleRecoveryHealthLoad = null;
  let opsIdleHeavyHydrationTimer = null;
  let opsIdleHeavyHydrationInFlight = false;

  function currentRenderToken() {
    return opsRenderToken;
  }

  function isStaleRenderToken(renderToken) {
    return renderToken !== opsRenderToken;
  }

  function canHydrateCompactDuringActiveRun() {
    return String(activeHydrationPolicy || "protected").trim().toLowerCase() === "desktop";
  }

  function queueIdleRecoveryHealthLoad(options = { summary: true }) {
    if (!idleRecoveryHealthLoad) {
      idleRecoveryHealthLoad = Promise.resolve()
        .then(() => loadOpsHealthData(options))
        .catch(() => {})
        .finally(() => {
          idleRecoveryHealthLoad = null;
        });
    }
    return idleRecoveryHealthLoad;
  }

  function getOpsRouteBackoffs() {
    if (!state.opsRouteBackoffs || typeof state.opsRouteBackoffs !== "object" || Array.isArray(state.opsRouteBackoffs)) {
      state.opsRouteBackoffs = {};
    }
    return state.opsRouteBackoffs;
  }

  function isOpsRouteBackedOff(routeKey) {
    const entry = getOpsRouteBackoffs()[String(routeKey || "")];
    return Boolean(entry && Date.now() < Number(entry.untilMs || 0));
  }

  function markOpsRouteFailure(routeKey) {
    const key = String(routeKey || "");
    if (!key) return;
    const routeBackoffs = getOpsRouteBackoffs();
    const previous = routeBackoffs[key] || {};
    const failureCount = Math.max(1, Number(previous.failureCount || 0) + 1);
    const delayMs = Math.min(
      OPS_HEAVY_ROUTE_BACKOFF_MAX_MS,
      OPS_HEAVY_ROUTE_BACKOFF_BASE_MS * (2 ** (failureCount - 1))
    );
    routeBackoffs[key] = {
      failureCount,
      untilMs: Date.now() + delayMs
    };
  }

  function clearOpsRouteFailure(routeKey) {
    const key = String(routeKey || "");
    if (!key || !state.opsRouteBackoffs) return;
    delete state.opsRouteBackoffs[key];
  }

  function markStep(name, payload = {}) {
    if (typeof markAdminStep === "function") markAdminStep(name, payload);
  }

  function measureStep(name, startMark, endMark, payload = {}) {
    if (typeof measureAdminStep === "function") measureAdminStep(name, startMark, endMark, payload);
  }

  async function measuredGetBridge(path, metricName, { enabled = true, requestOptions = {} } = {}) {
    const startMark = `${metricName}_start`;
    const endMark = `${metricName}_done`;
    if (enabled) markStep(startMark);
    try {
      const payload = await getBridge(path, requestOptions);
      if (enabled) {
        markStep(endMark, { ok: true });
        measureStep(metricName, startMark, endMark, { ok: true });
      }
      return payload;
    } catch (err) {
      if (enabled) {
        markStep(endMark, { ok: false, error: String(err?.message || err || "unknown error") });
        measureStep(metricName, startMark, endMark, { ok: false });
      }
      throw err;
    }
  }

  function resolveLiveRef(refName, selector, diagnosticName) {
    const current = refs?.[refName] || null;
    const currentConnected = Boolean(
      current
      && (
        typeof current.isConnected !== "boolean"
        || current.isConnected
      )
    );
    if (currentConnected) return current;
    const next = globalThis.document?.querySelector?.(selector) || current;
    if (next && next !== current) {
      refs[refName] = next;
      markStep(`admin_${diagnosticName}_render_target_rebound`);
      return next;
    }
    if (!next) {
      markStep(`admin_${diagnosticName}_render_target_missing`);
    }
    return next;
  }

  function getScheduleElement() {
    return resolveLiveRef("adminOpsScheduleEl", '[data-ui="admin-ops-schedule"]', "pipeline_schedule");
  }

  function getHistoryElement() {
    return resolveLiveRef("adminOpsHistoryEl", '[data-ui="admin-ops-history"]', "ops_history");
  }

  // ── Per-endpoint hydration leaves ────────────────────────────────────

  const historyHydration = createHistoryHydration({
    state,
    getErrorMessage,
    markStep,
    measuredGetBridge,
    mergeOpsHistoryPayload,
    OPS_HISTORY_STARTUP_PATH,
    OPS_AUTHORITY_FETCH_TIMEOUT_MS,
    scheduleOpsHistoryRetry,
    renderDeferredHistoryDetails
  });

  const pipelineScheduleHydration = createPipelineScheduleHydration({
    state,
    markStep,
    measuredGetBridge,
    getErrorMessage,
    showToast,
    normalizePipelineSchedulePayload,
    rememberPipelineSchedule,
    renderPipelineScheduleModel,
    hasKnownPipelineSchedule: schedule => hasKnownPipelineSchedule(schedule),
    hasPipelineScheduleNextRun: schedule => hasPipelineScheduleNextRun(schedule),
    schedulePipelineScheduleRetry,
    scheduleIdleOpsHeavyHydration,
    JOBS_PIPELINE_SCHEDULE_PATH,
    OPS_AUTHORITY_FETCH_TIMEOUT_MS,
    currentRenderToken
  });

  const taskStateHydration = createTaskStateHydration({
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
    currentRenderToken,
    isStaleRenderToken,
    OPS_TASK_STATE_SUMMARY_PATH
  });

  const fetchKpisHydration = createFetchKpisHydration({
    state,
    canHydrateCompactDuringActiveRun,
    hasPossibleActiveRunEvidence,
    markFetchKpisDeferredDuringActiveRun: () => markFetchKpisDeferredDuringActiveRun(state),
    renderOpsHealthSnapshot,
    getCachedTaskStatePayload,
    getCachedRegistryConflictsPayload,
    mergeOpsHealth,
    measuredGetBridge,
    currentRenderToken,
    isStaleRenderToken,
    OPS_FETCH_KPIS_SUMMARY_PATH
  });

  const dashboardHealthHydration = createDashboardHealthHydration({
    state,
    hasPossibleActiveRunEvidence,
    isOpsRouteBackedOff,
    markOpsRouteFailure,
    clearOpsRouteFailure,
    measuredGetBridge,
    mergeOpsHealth,
    renderOpsHealthSnapshot,
    getCachedTaskStatePayload,
    getCachedRegistryConflictsPayload,
    currentRenderToken,
    isStaleRenderToken,
    OPS_DASHBOARD_HEALTH_SUMMARY_PATH,
    OPS_HEAVY_ROUTE_DASHBOARD
  });

  const pipelineStatusHydration = createPipelineStatusHydration({
    state,
    measuredGetBridge,
    buildPipelineTaskStatePayload,
    shouldKeepExistingActiveTaskState,
    hasActiveRows,
    hasActivePipelineOrFetchRows,
    canHydrateCompactDuringActiveRun,
    markFetchKpisDeferredDuringActiveRun: () => markFetchKpisDeferredDuringActiveRun(state),
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
  });

  const activeOpsHydration = createActiveOpsHydration({
    state,
    loadTaskStateSummaryData: (renderToken, options) => loadTaskStateSummaryData(renderToken, options),
    loadPipelineStatusFallbackData: (renderToken, options) => loadPipelineStatusFallbackData(renderToken, options),
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
  });

  const registrySyncHydration = createRegistrySyncHydration({
    state,
    renderOpsHealthSnapshot,
    getCachedTaskStatePayload,
    getCachedRegistryConflictsPayload,
    hasRegistrySyncDetails,
    measuredGetBridge,
    mergeOpsHealth,
    currentRenderToken,
    isStaleRenderToken,
    OPS_DASHBOARD_HEALTH_SUMMARY_PATH
  });

  const tabCountsHydration = createTabCountsHydration({
    state,
    isOpsRouteBackedOff,
    markOpsRouteFailure,
    clearOpsRouteFailure,
    measuredGetBridge,
    rerenderOpsTabBadges,
    currentRenderToken,
    isStaleRenderToken,
    OPS_TAB_COUNTS_SUMMARY_PATH,
    OPS_HEAVY_ROUTE_TAB_COUNTS
  });

  const {
    loadOpsHistoryData
  } = historyHydration;
  const {
    loadPipelineScheduleData
  } = pipelineScheduleHydration;
  const {
    loadTaskStateSummaryData
  } = taskStateHydration;
  const {
    loadFetchKpisSummaryData
  } = fetchKpisHydration;
  const {
    loadDashboardHealthSummaryData
  } = dashboardHealthHydration;
  const {
    loadPipelineStatusFallbackData
  } = pipelineStatusHydration;
  const {
    loadActiveOpsSupplementalData,
    loadActiveOpsSummaryData
  } = activeOpsHydration;
  const {
    loadRegistrySyncDiagnosticsData
  } = registrySyncHydration;
  const {
    loadOpsTabCountsSummaryData
  } = tabCountsHydration;

  const {
    handleDedupReviewAction,
    handleCopySectionDiagnostics,
    handleCopyRunDiagnostics,
    handleRefreshAuditArtifacts,
    handleRefreshTaskFailureAttempts,
    handleRefreshPerformanceProfile,
    handleAbortRun,
    renderRegistryConflictsQueue,
    renderDiscoveryReviewPanel,
    renderSourcePolicyReviewQueue
  } = createOpsActions({
    state,
    refs,
    postBridge,
    showToast,
    getErrorMessage,
    escapeHtml,
    getObjectValue,
    loadOpsHealthData,
    loadSourcePolicyDetail: options => loadSourcePolicyDetail(options),
    loadOpsOverviewDetailData: renderToken => loadOpsOverviewDetailData(renderToken),
    loadActiveOpsSummaryData,
    applyOptimisticAbortRow,
    setPendingOpsAbort,
    clearPendingOpsAbort,
    hasPendingOpsAbort,
    isAbortAcceptedResult,
    hasActivePipelineOrFetchRows,
    renderAdminRegistryConflictsImpl,
    renderDiscoveryCandidateReviewHtml,    toDiscoveryBadgeState,
    renderAdminSourcePolicyReviewImpl,
    currentRenderToken
  });

  const registryConflictsHydration = createRegistryConflictsHydration({
    state,
    isOpsRouteBackedOff,
    markOpsRouteFailure,
    clearOpsRouteFailure,
    measuredGetBridge,
    getCachedTaskStatePayload,
    getCachedRegistryConflictsPayload,
    renderOpsHealthSnapshot,
    renderRegistryConflictsQueue,
    currentRenderToken,
    isStaleRenderToken,
    REGISTRY_CONFLICTS_SUMMARY_PATH,
    OPS_HEAVY_ROUTE_REGISTRY_CONFLICTS
  });
  const {
    loadRegistryConflictsSummaryData
  } = registryConflictsHydration;

  const overviewHydration = createOverviewHydration({
    state,
    refs,
    getBridge,
    getErrorMessage,
    showToast,
    escapeHtml,
    getObjectValue,
    isLoadedDiscoveryReport,
    isLoadedDedupPayload,
    maybeUnrefTimer,
    loadLatestDiscoveryReport,
    getCachedSourcePolicyPayload,
    getCachedRegistryConflictsPayload,
    renderDeferredOverviewDetails,
    renderSourcePolicyReviewQueue,
    renderRegistryConflictsQueue,
    renderDiscoveryReviewPanel,
    renderAdminOpsDedupListsImpl,
    buildFetcherMetricsPayload,
    handleDedupReviewAction,
    rerenderOpsTabBadges,
    currentRenderToken,
    isStaleRenderToken,
    OPS_HISTORY_DETAIL_PATH,
    OPS_FETCHER_METRICS_DETAIL_PATH,
    OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH,
    OPS_TASK_FAILURE_ATTEMPTS_PATH,
    OPS_PERFORMANCE_PROFILE_PATH,
    SOURCE_POLICY_DETAIL_PATH,
    REGISTRY_CONFLICTS_DETAIL_PATH
  });

  const {
    loadOpsOverviewDetailData,
    handleLoadDebugDiagnostics,
    scheduleOpsOverviewDetailData,
    loadSourcePolicyDetail,
    loadActiveOpsTabDetail
  } = overviewHydration;

  // ── Pipeline schedule model (coordinator-owned) ──────────────────────

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

  // ── Cached payload getters ───────────────────────────────────────────

  function getCachedTaskStatePayload() {
    return getObjectValue(state.latestOpsTaskStatePayload);
  }

  function getCachedHistoryPayload() {
    return state.latestOpsHistoryPayload
      && typeof state.latestOpsHistoryPayload === "object"
      && !Array.isArray(state.latestOpsHistoryPayload)
      ? state.latestOpsHistoryPayload
      : { runs: [] };
  }

  function scheduleOpsHistoryRetry() {
    if (state.opsHistoryRetryTimer) return;
    const failures = Math.max(1, Number(state.opsHistoryFailureCount || 1));
    const delayMs = Math.min(OPS_AUTHORITY_RETRY_MAX_MS, OPS_AUTHORITY_RETRY_BASE_MS * (2 ** Math.min(3, failures - 1)));
    state.opsHistoryRetryTimer = maybeUnrefTimer(setTimeout(() => {
      state.opsHistoryRetryTimer = null;
      loadOpsHistoryData({ force: true, silent: true }).catch(() => {});
    }, delayMs));
  }

  function getCachedSourcePolicyPayload() {
    return state.latestSourcePolicyRecommendationsPayload
      && typeof state.latestSourcePolicyRecommendationsPayload === "object"
      && !Array.isArray(state.latestSourcePolicyRecommendationsPayload)
      ? state.latestSourcePolicyRecommendationsPayload
      : { summaryStatus: "pending" };
  }

  function getCachedRegistryConflictsPayload() {
    return state.latestRegistryConflictsPayload
      && typeof state.latestRegistryConflictsPayload === "object"
      && !Array.isArray(state.latestRegistryConflictsPayload)
      ? state.latestRegistryConflictsPayload
      : { summary: {}, summaryStatus: "pending", conflicts: [] };
  }

  function getCachedDiscoveryAuditArtifactsPayload() {
    return state.latestDiscoveryAuditArtifactsPayload
      && typeof state.latestDiscoveryAuditArtifactsPayload === "object"
      && !Array.isArray(state.latestDiscoveryAuditArtifactsPayload)
      ? state.latestDiscoveryAuditArtifactsPayload
      : { ok: true, artifacts: [] };
  }

  function getCachedTaskFailureAttemptsPayload() {
    return state.latestTaskFailureAttemptsPayload
      && typeof state.latestTaskFailureAttemptsPayload === "object"
      && !Array.isArray(state.latestTaskFailureAttemptsPayload)
      ? state.latestTaskFailureAttemptsPayload
      : { ok: true, fetch: {}, discovery: {}, warnings: [] };
  }

  function getCachedPerformanceProfilePayload() {
    return state.latestOpsPerformanceProfilePayload
      && typeof state.latestOpsPerformanceProfilePayload === "object"
      && !Array.isArray(state.latestOpsPerformanceProfilePayload)
      ? state.latestOpsPerformanceProfilePayload
      : { ok: true, routeTimings: { routes: [] }, operationTimings: { operations: [] } };
  }

  // ── Active-run / abort state ─────────────────────────────────────────

  function hasActiveRows(taskStatePayload = getCachedTaskStatePayload()) {
    const rows = Array.isArray(taskStatePayload?.tasks) ? taskStatePayload.tasks : [];
    return rows.some(row => row && row.active !== false && !row.finishedAt);
  }

  function getOpsAbortRequests() {
    if (!state.adminOpsAbortRequests || typeof state.adminOpsAbortRequests !== "object" || Array.isArray(state.adminOpsAbortRequests)) {
      state.adminOpsAbortRequests = {};
    }
    return state.adminOpsAbortRequests;
  }

  function hasPendingOpsAbort(taskType, runId) {
    const key = getOpsAbortKey(taskType, runId);
    return Boolean(key.trim() && getOpsAbortRequests()[key]);
  }

  function setPendingOpsAbort(taskType, runId, value = {}) {
    const key = getOpsAbortKey(taskType, runId);
    if (!key.trim()) return null;
    const pending = {
      taskType: String(taskType || "").trim().toLowerCase(),
      runId: String(runId || "").trim(),
      requestedAt: new Date().toISOString(),
      ...value
    };
    getOpsAbortRequests()[key] = pending;
    return pending;
  }

  function clearPendingOpsAbort(taskType, runId) {
    const key = getOpsAbortKey(taskType, runId);
    if (!key.trim() || !state.adminOpsAbortRequests) return;
    delete state.adminOpsAbortRequests[key];
  }

  function clearAllPendingOpsAborts() {
    state.adminOpsAbortRequests = {};
  }

  function hasPendingOpsAbortRequests() {
    return Object.keys(getOpsAbortRequests()).length > 0;
  }

  function applyOptimisticAbortRow(taskType, runId, pendingAbort = null) {
    const cleanTaskType = String(taskType || "").trim().toLowerCase();
    const cleanRunId = String(runId || "").trim();
    if (!cleanTaskType || !cleanRunId) return null;
    const abortMeta = pendingAbort || setPendingOpsAbort(cleanTaskType, cleanRunId);
    const existingPayload = getCachedTaskStatePayload();
    const existingRows = Array.isArray(existingPayload?.tasks) ? existingPayload.tasks : [];
    let matched = false;
    const tasks = existingRows.map(row => {
      if (getTaskRowType(row) !== cleanTaskType || getTaskRowRunId(row) !== cleanRunId) {
        return row;
      }
      matched = true;
      return buildOptimisticAbortRow(row, cleanTaskType, cleanRunId, abortMeta);
    });
    if (!matched) {
      tasks.unshift(buildOptimisticAbortRow({}, cleanTaskType, cleanRunId, abortMeta));
    }
    state.latestOpsTaskStatePayload = {
      ...getObjectValue(existingPayload),
      tasks,
      count: tasks.length,
      summary: true
    };
    if (cleanTaskType === "pipeline") setBusyFlag("livePipelineRunning", true);
    if (cleanTaskType === "fetch") setBusyFlag("liveFetchRunning", true);
    if (cleanTaskType === "discovery") setBusyFlag("liveDiscoveryRunning", true);
    if (["pipeline", "fetch", "discovery"].includes(cleanTaskType)) {
      state.opsActivePipelineOrFetchLastActive = true;
      markOpsDegradedActive("abort_requested");
    }
    renderOpsHealthSnapshot(opsRenderToken, state.latestOpsHealthCache || {}, {
      taskStatePayload: state.latestOpsTaskStatePayload,
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      syncTaskState: true,
      renderDeferredPanels: false,
      renderActivityPanel: true,
      schedulePolling: false
    });
    return state.latestOpsTaskStatePayload;
  }

  function hasOptimisticRows() {
    return Boolean(state.discoveryOptimisticRun || state.fetchOptimisticRun);
  }

  function hasRecentOpsDegradedActive() {
    return Date.now() < Number(state.opsDegradedActiveUntilMs || 0);
  }

  function markActiveIdleRecoveryCooldown() {
    state.opsActiveIdleRecoveryCooldownUntilMs = Date.now() + ACTIVE_IDLE_RECOVERY_COOLDOWN_MS;
  }

  function hasActiveIdleRecoveryCooldown() {
    return Date.now() < Number(state.opsActiveIdleRecoveryCooldownUntilMs || 0);
  }

  function hasPossiblePipelineOrFetchEvidence({ includeRecent = true } = {}) {
    if (hasActiveIdleRecoveryCooldown()) return false;
    const busyState = state.adminBusyState || {};
    return Boolean(
      hasActivePipelineOrFetchRows(getCachedTaskStatePayload())
      || hasOptimisticRows()
      || hasPendingOpsAbortRequests()
      || state.opsActivePipelineOrFetchLastActive
      || state.fetcherLiveProgressState
      || busyState.fetcherWatch
      || busyState.livePipelineRunning
      || busyState.liveFetchRunning
      || (includeRecent && hasRecentOpsDegradedActive() && state.opsActivePipelineOrFetchLastActive)
    );
  }

  function hasPossibleActiveRunEvidence({ includeRecent = true } = {}) {
    if (hasActiveIdleRecoveryCooldown()) return false;
    const busyState = state.adminBusyState || {};
    return Boolean(
      hasActiveAdminWorkRows(getCachedTaskStatePayload())
      || hasOptimisticRows()
      || hasPendingOpsAbortRequests()
      || state.opsActiveAdminWorkLastActive
      || state.opsActivePipelineOrFetchLastActive
      || state.fetcherLiveProgressState
      || state.discoveryLiveProgressState
      || busyState.fetcherWatch
      || busyState.discoveryWatch
      || busyState.livePipelineRunning
      || busyState.liveFetchRunning
      || busyState.liveDiscoveryRunning
      || busyState.liveSyncRunning
      || (includeRecent && hasRecentOpsDegradedActive())
    );
  }

  function markOpsDegradedActive(reason = "control_plane_unavailable") {
    state.opsDegradedActiveUntilMs = Date.now() + OPS_DEGRADED_ACTIVE_TTL_MS;
    state.opsDegradedActiveReason = String(reason || "control_plane_unavailable");
    state.opsActiveAdminWorkLastActive = true;
    if (hasPossiblePipelineOrFetchEvidence({ includeRecent: false })) {
      state.opsActivePipelineOrFetchLastActive = true;
    }
  }

  function clearOpsDegradedActive() {
    state.opsDegradedActiveUntilMs = 0;
    state.opsDegradedActiveReason = "";
  }

  function hasActivePipelineOrFetchRows(taskStatePayload = getCachedTaskStatePayload()) {
    return hasActiveAdminTaskRows(taskStatePayload, ACTIVE_PIPELINE_OR_FETCH_TASK_TYPES);
  }

  function hasActiveAdminWorkRows(taskStatePayload = getCachedTaskStatePayload()) {
    return hasActiveAdminTaskRows(taskStatePayload, ACTIVE_ADMIN_TASK_TYPES);
  }

  function notifyActiveAdminWorkIdleIfNeeded(wasActive, isActive, { wasPipelineOrFetchActive = false, pipelineOrFetchActive = false } = {}) {
    state.opsActiveAdminWorkLastActive = Boolean(isActive);
    state.opsActivePipelineOrFetchLastActive = Boolean(pipelineOrFetchActive);
    if (!wasActive || isActive || typeof onActivePipelineIdle !== "function") return;
    Promise.resolve(onActivePipelineIdle({
      reason: wasPipelineOrFetchActive ? "active_pipeline_idle" : "active_admin_work_idle",
      at: new Date().toISOString()
    })).catch(() => {});
  }

  function deriveLiveRunContext(taskStatePayload, registryConflictsPayload) {
    const historyPayload = getCachedHistoryPayload();
    const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
    const runModel = deriveAdminRunsModel(
      {
        taskState: taskStatePayload || {},
        historyRuns
      },
      Date.now()
    );
    const liveTaskRows = taskStateController.getActiveTaskRows(taskStatePayload);
    const liveTypes = new Set(
      liveTaskRows
        .map(row => taskStateController.getTaskType(row))
        .filter(Boolean)
    );
    const registryConflictRunning = String(registryConflictsPayload?.adjudication?.status || "") === "running";
    return {
      historyRuns,
      runModel,
      liveTaskRows,
      liveTypes,
      registryConflictRunning
    };
  }

  // ── Tabs and schedule controls ───────────────────────────────────────

  function getOpsTabPanels() {
    return {
      overview: refs.adminOpsTabOverviewEl,
      discovery: refs.adminOpsTabDiscoveryEl,
      "source-policy": refs.adminOpsTabSourcePolicyEl,
      "registry-conflicts": refs.adminOpsTabRegistryConflictsEl,
      dedup: refs.adminOpsTabDedupEl
    };
  }

  function selectOpsTab(tabKey = "overview") {
    const activeKey = OPS_TAB_KEYS.has(tabKey) ? tabKey : "overview";
    state.adminOpsActiveTab = activeKey;
    const buttons = Array.isArray(refs.adminOpsTabBtnEls) ? refs.adminOpsTabBtnEls : [];
    buttons.forEach(button => {
      const buttonKey = String(button?.dataset?.opsTab || button?.getAttribute?.("data-ops-tab") || "");
      const active = buttonKey === activeKey;
      button?.setAttribute?.("aria-selected", active ? "true" : "false");
      button?.classList?.toggle?.("active", active);
      if (button) button.tabIndex = active ? 0 : -1;
    });
    Object.entries(getOpsTabPanels()).forEach(([key, panel]) => {
      if (!panel) return;
      const active = key === activeKey;
      panel.hidden = !active;
      panel.classList?.toggle?.("hidden", !active);
      if (active) {
        panel.removeAttribute?.("hidden");
      } else {
        panel.setAttribute?.("hidden", "");
      }
    });
    return loadActiveOpsTabDetail(activeKey).catch(() => {});
  }

  function setupOpsTabs() {
    const buttons = Array.isArray(refs.adminOpsTabBtnEls) ? refs.adminOpsTabBtnEls : [];
    if (!buttons.length || state.adminOpsTabsInitialized) {
      selectOpsTab(state.adminOpsActiveTab || "overview");
      return;
    }
    state.adminOpsTabsInitialized = true;
    buttons.forEach(button => {
      button?.addEventListener?.("click", () => {
        selectOpsTab(String(button?.dataset?.opsTab || button?.getAttribute?.("data-ops-tab") || "overview"));
      });
    });
    selectOpsTab(state.adminOpsActiveTab || "overview");
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

  setupOpsTabs();
  setupPipelineScheduleControls();

  // ── Placeholders and polling ─────────────────────────────────────────

  function setOpsPlaceholders(message = "Operations health unavailable.") {
    if (refs.adminSyncStatusEl) {
      refs.adminSyncStatusEl.textContent = message;
    }
    if (refs.adminSyncConfigHintEl) {
      refs.adminSyncConfigHintEl.textContent = "GitHub App credentials are packaged with the app.";
    }
    if (refs.adminOpsAlertsEl) {
      refs.adminOpsAlertsEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
    }
    if (refs.adminOpsKpisEl) refs.adminOpsKpisEl.innerHTML = "";
    renderPipelineScheduleModel();
    if (refs.adminSourcePolicyReviewEl) {
      refs.adminSourcePolicyReviewEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
    }
    if (refs.adminOpsFetcherMetricsEl) refs.adminOpsFetcherMetricsEl.innerHTML = "";
    if (refs.adminOpsDedupListsEl) refs.adminOpsDedupListsEl.innerHTML = "";
    if (refs.adminOpsTrendsEl) refs.adminOpsTrendsEl.textContent = message;
    renderOpsTabBadges(refs, {
      health: { alerts: [] },
      discoveryReport: {},
      sourcePolicyRecommendations: {},
      fetcherMetricsPayload: {},
      tabCountsPayload: state.latestOpsTabCountsPayload || null
    });
  }

  function stopOpsHealthPolling() {
    if (state.opsHealthPollTimer) clearTimeout(state.opsHealthPollTimer);
    state.opsHealthPollTimer = null;
    stopPipelineStatusPolling();
  }

  function stopPipelineStatusPolling() {
    if (!state.pipelineStatusPollTimer) return;
    clearTimeout(state.pipelineStatusPollTimer);
    state.pipelineStatusPollTimer = null;
  }

  function schedulePipelineStatusPolling(delayMs) {
    // ponytail: exclusive lanes — starting the active lane must cancel any
    // pending idle poll or the two loops stack duplicate summary requests.
    stopOpsHealthPolling();
    const waitMs = Math.max(600, Number(delayMs) || 2000);
    state.pipelineStatusPollTimer = maybeUnrefTimer(setTimeout(() => {
      loadActiveOpsSummaryData(opsRenderToken, { fromPoll: true }).catch(() => {});
    }, waitMs));
  }

  function scheduleOpsHealthPolling(delayMs) {
    stopOpsHealthPolling();
    const waitMs = Math.max(600, Number(delayMs) || 10000);
    if (hasPossibleActiveRunEvidence({ includeRecent: false })) {
      // Route through the active lane while run evidence exists: it keeps
      // cadence adaptive and skips the heavy dashboard-health summary.
      state.pipelineStatusPollTimer = maybeUnrefTimer(setTimeout(() => {
        loadActiveOpsSummaryData(opsRenderToken, { fromPoll: true }).catch(() => {});
      }, waitMs));
      return;
    }
    state.opsHealthPollTimer = maybeUnrefTimer(setTimeout(() => {
      loadOpsHealthData({ fromPoll: true, summary: true }).catch(() => {});
    }, waitMs));
  }

  function shouldDeferIdleOpsHeavyHydration(options = {}) {
    return Boolean(
      (!options?.allowStartupBridgeLane && state.adminStartupBridgeHydrationInFlight)
      || state.adminBusyState?.discoveryLoad
      || hasPossibleActiveRunEvidence()
    );
  }

  async function loadIdleOpsHeavyHydration(renderToken = opsRenderToken, options = {}) {
    if (options?.fromPoll || opsIdleHeavyHydrationInFlight || shouldDeferIdleOpsHeavyHydration(options)) return null;
    const hydrationRenderToken = options?.renderWithCurrentToken ? opsRenderToken : renderToken;
    const shouldLoadRegistryConflicts = !state.latestRegistryConflictsPayload;
    const shouldLoadFetchKpis = !hasFetchKpiValues(state.latestOpsHealthCache?.kpis || {});
    const shouldLoadOpsTabCounts = !state.latestOpsTabCountsPayload || state.opsTabCountsDelayedDuringActiveRun;
    if (!shouldLoadRegistryConflicts && !shouldLoadFetchKpis && !shouldLoadOpsTabCounts) return null;
    opsIdleHeavyHydrationInFlight = true;
    try {
      if (shouldLoadRegistryConflicts) {
        await loadRegistryConflictsSummaryData(hydrationRenderToken, { silent: true }).catch(() => {});
      }
      if (shouldLoadFetchKpis) {
        await loadFetchKpisSummaryData(hydrationRenderToken, {
          silent: true,
          renderWithCurrentToken: true
        }).catch(() => {});
      }
      if (shouldLoadOpsTabCounts) {
        await loadOpsTabCountsSummaryData(hydrationRenderToken, {
          silent: true,
          renderWithCurrentToken: true,
          force: Boolean(state.opsTabCountsDelayedDuringActiveRun)
        }).catch(() => {});
      }
    } finally {
      opsIdleHeavyHydrationInFlight = false;
    }
    return {
      registryConflicts: state.latestRegistryConflictsPayload || null,
      opsHealth: state.latestOpsHealthCache || null,
      tabCounts: state.latestOpsTabCountsPayload || null
    };
  }

  function scheduleIdleOpsHeavyHydration(renderToken, options = {}) {
    if (options?.fromPoll || opsIdleHeavyHydrationTimer || opsIdleHeavyHydrationInFlight) return;
    const hydrate = () => {
      opsIdleHeavyHydrationTimer = null;
      loadIdleOpsHeavyHydration(renderToken, options).catch(() => {});
    };
    if (OPS_IDLE_HEAVY_HYDRATION_DELAY_MS <= 0) {
      Promise.resolve().then(hydrate);
      return;
    }
    opsIdleHeavyHydrationTimer = maybeUnrefTimer(setTimeout(hydrate, OPS_IDLE_HEAVY_HYDRATION_DELAY_MS));
  }

  // ── Render helpers and hub ───────────────────────────────────────────

  function setOpsReadinessShell() {
    if (refs.adminOpsAlertsEl) refs.adminOpsAlertsEl.innerHTML = "";
    if (refs.adminOpsKpisEl) refs.adminOpsKpisEl.innerHTML = "";
    renderPipelineScheduleModel();
    if (refs.adminSourcePolicyReviewEl) refs.adminSourcePolicyReviewEl.innerHTML = "";
    if (refs.adminOpsFetcherMetricsEl) refs.adminOpsFetcherMetricsEl.innerHTML = "";
    if (refs.adminOpsDedupListsEl) refs.adminOpsDedupListsEl.innerHTML = "";
    if (refs.adminOpsTrendsEl) {
      refs.adminOpsTrendsEl.textContent = "No run trend data yet.";
    }
    renderOpsTabBadges(refs, {
      health: { alerts: [] },
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations: getCachedSourcePolicyPayload(),
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      fetcherMetricsPayload: buildFetcherMetricsPayload(),
      tabCountsPayload: state.latestOpsTabCountsPayload || null
    });
  }

  function getRenderScheduler() {
    return typeof renderScheduler === "function"
      ? renderScheduler
      : callback => {
        callback();
        return () => {};
      };
  }

  function buildFetcherMetricsPayload(fetcherMetrics = state.latestOpsFetcherMetricsPayload || {}, health = state.latestOpsHealthCache || {}) {
    const frontendPerfCounters = getFrontendPerfCounters();
    return {
      ...(fetcherMetrics && typeof fetcherMetrics === "object" ? fetcherMetrics : {}),
      discoveryAuditArtifacts: getCachedDiscoveryAuditArtifactsPayload(),
      taskFailureAttempts: getCachedTaskFailureAttemptsPayload(),
      performanceProfile: getCachedPerformanceProfilePayload(),
      frontendPerfCounters: (
        frontendPerfCounters
        && typeof frontendPerfCounters === "object"
        && !Array.isArray(frontendPerfCounters)
      )
        ? frontendPerfCounters
        : {},
      latestRun: {
        ...(
          fetcherMetrics?.latestRun && typeof fetcherMetrics.latestRun === "object"
            ? fetcherMetrics.latestRun
            : {}
        ),
        conservativeStaticCleanupProposals:
          health?.kpis?.conservativeStaticCleanupProposals
          && typeof health.kpis.conservativeStaticCleanupProposals === "object"
            ? health.kpis.conservativeStaticCleanupProposals
            : (
              fetcherMetrics?.latestRun?.conservativeStaticCleanupProposals
              && typeof fetcherMetrics.latestRun.conservativeStaticCleanupProposals === "object"
                ? fetcherMetrics.latestRun.conservativeStaticCleanupProposals
                : {}
            )
      }
    };
  }

  function rerenderOpsTabBadges() {
    const activePipelineOrFetch = Boolean(
      state.opsActivePipelineOrFetchLastActive
      || state.adminBusyState?.livePipelineRunning
      || state.adminBusyState?.liveFetchRunning
      || hasActivePipelineOrFetchRows(getCachedTaskStatePayload())
    );
    renderOpsTabBadges(refs, {
      health: state.latestOpsHealthCache || {},
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations: getCachedSourcePolicyPayload(),
      registryConflictsPayload: getCachedRegistryConflictsPayload(),
      fetcherMetricsPayload: buildFetcherMetricsPayload(),
      tabCountsPayload: state.latestOpsTabCountsPayload || null,
      activePipelineOrFetch,
      tabCountsUnavailable: Boolean(state.opsTabCountsUnavailable && !activePipelineOrFetch)
    });
  }

  function renderDeferredOverviewDetails(renderToken = opsRenderToken) {
    if (renderToken !== opsRenderToken) return;
    const fetcherMetricsPayload = buildFetcherMetricsPayload();
    rerenderOpsTabBadges();
    getRenderScheduler()(() => {
      if (renderToken !== opsRenderToken) return;
      const historyPayload = getCachedHistoryPayload();
      const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
      const taskStatePayload = state.latestOpsTaskStatePayload || { tasks: [] };
      const runModel = deriveAdminRunsModel(
        {
          taskState: taskStatePayload || {},
          historyRuns
        },
        Date.now()
      );
      renderAdminOpsFetcherMetricsImpl(
        refs.adminOpsFetcherMetricsEl,
        fetcherMetricsPayload,
        deriveFetcherFailureSummary(state.latestFetcherReportCache || {}),
        {
          onDedupReviewAction: handleDedupReviewAction,
          onCopySectionDiagnostics: handleCopySectionDiagnostics,
          onRefreshAuditArtifacts: handleRefreshAuditArtifacts,
          onRefreshTaskFailureAttempts: handleRefreshTaskFailureAttempts,
          onRefreshPerformanceProfile: handleRefreshPerformanceProfile,
          onLoadDebugDiagnostics: handleLoadDebugDiagnostics,
          includeDebugDiagnostics: Boolean(state.opsDebugDiagnosticsLoaded),
          debugDiagnosticsLoading: Boolean(state.opsDebugDiagnosticsLoading),
          runModel
        }
      );
      renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, fetcherMetricsPayload, {
        onDedupReviewAction: handleDedupReviewAction
      });
      renderAdminOpsHistoryImpl(getHistoryElement(), runModel, {
        onCopyRunDiagnostics: handleCopyRunDiagnostics,
        onAbortRun: handleAbortRun,
        waitingForTaskState: Boolean(state.waitingForTaskState),
        taskStateUnavailable: Boolean(state.taskStateUnavailable),
        taskStateError: String(state.lastTaskStateError || "").trim(),
        historyPending: Boolean(state.opsHistoryLoadPending),
        historyLoaded: Boolean(state.opsHistoryLoaded),
        historyError: state.opsHistoryLastError,
        historyFullLoaded: Boolean(state.opsHistoryFullLoaded)
      });
      if (state.opsHistoryLoaded) {
        renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
      }
    });
  }

  function renderDeferredHistoryDetails(renderToken = null) {
    if (renderToken !== null && renderToken !== opsRenderToken) return;
    getRenderScheduler()(() => {
      if (renderToken !== null && renderToken !== opsRenderToken) return;
      const historyPayload = getCachedHistoryPayload();
      const historyRuns = Array.isArray(historyPayload?.runs) ? historyPayload.runs : [];
      const taskStatePayload = state.latestOpsTaskStatePayload || { tasks: [] };
      const runModel = deriveAdminRunsModel(
        {
          taskState: taskStatePayload || {},
          historyRuns
        },
        Date.now()
      );
      renderAdminOpsHistoryImpl(getHistoryElement(), runModel, {
        onCopyRunDiagnostics: handleCopyRunDiagnostics,
        onAbortRun: handleAbortRun,
        waitingForTaskState: Boolean(state.waitingForTaskState),
        taskStateUnavailable: Boolean(state.taskStateUnavailable),
        taskStateError: String(state.lastTaskStateError || "").trim(),
        historyPending: Boolean(state.opsHistoryLoadPending),
        historyLoaded: Boolean(state.opsHistoryLoaded),
        historyError: state.opsHistoryLastError,
        historyFullLoaded: Boolean(state.opsHistoryFullLoaded)
      });
      if (state.opsHistoryLoaded) {
        renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
      }
    });
  }

  function renderOpsHealthSnapshot(renderToken, health, {
    taskStatePayload = getCachedTaskStatePayload(),
    registryConflictsPayload = getCachedRegistryConflictsPayload(),
    syncTaskState = false,
    dispatchRefresh = false,
    scheduleDetails = false,
    renderDeferredPanels = true,
    renderActivityPanel = false,
    schedulePolling = true
  } = {}) {
    if (renderToken !== opsRenderToken) return;
    const sourcePolicyRecommendations = getCachedSourcePolicyPayload();
    const {
      historyRuns,
      runModel,
      liveTaskRows,
      liveTypes,
      registryConflictRunning
    } = deriveLiveRunContext(taskStatePayload, registryConflictsPayload);
    if (syncTaskState) {
      taskStateController.syncLiveBusyFlags(liveTypes);
      taskStateController.maybeAttachLiveTaskRows(liveTaskRows);
    }
    const fetcherMetricsPayload = buildFetcherMetricsPayload(
      state.latestOpsFetcherMetricsPayload || {},
      health || {}
    );
    const controlPlanePipelineActive = Boolean(
      taskStatePayload?.source === "pipeline-status" && hasActiveRows(taskStatePayload)
    );
    const activePipelineOrFetch = Boolean(
      controlPlanePipelineActive || liveTypes.has("pipeline") || liveTypes.has("fetch")
    );
    if (activePipelineOrFetch) {
      state.opsTabCountsDelayedDuringActiveRun = true;
    }
    const fetchKpiPendingLabels = buildFetchKpiPendingLabels(health, activePipelineOrFetch);
    const fetchKpiPendingLabel = String(fetchKpiPendingLabels.default || FETCH_KPI_LOADING_LABEL);
    const renderKpis = maskDeferredFetchKpisForRender(
      health?.kpis || {},
      health || {}
    );

    renderAdminOpsAlertsImpl(refs.adminOpsAlertsEl, health?.alerts || [], {
      onAck: async alertId => {
        if (!alertId) return;
        try {
          await postBridge("/ops/alerts/ack", { id: alertId });
          await loadOpsHealthData();
        } catch (err) {
          showToast(`Could not dismiss alert: ${getErrorMessage(err)}`, "error");
        }
      }
    });
    renderAdminOpsKpisImpl(
      refs.adminOpsKpisEl,
      renderKpis,
      String(health?.status || "healthy"),
      { fetchKpiPendingLabel, fetchKpiPendingLabels }
    );
    renderPipelineScheduleModel();
    renderOpsTabBadges(refs, {
      health,
      discoveryReport: state.latestDiscoveryReportCache || {},
      sourcePolicyRecommendations,
      registryConflictsPayload,
      fetcherMetricsPayload,
      tabCountsPayload: state.latestOpsTabCountsPayload || null,
      activePipelineOrFetch,
      tabCountsUnavailable: Boolean(state.opsTabCountsUnavailable && !activePipelineOrFetch)
    });
    renderAdminOpsTrendsImpl(refs.adminOpsTrendsEl, historyRuns);
    const historyRenderOptions = {
      onCopyRunDiagnostics: handleCopyRunDiagnostics,
      onAbortRun: handleAbortRun,
      waitingForTaskState: Boolean(state.waitingForTaskState),
      taskStateUnavailable: Boolean(state.taskStateUnavailable),
      taskStateError: String(state.lastTaskStateError || "").trim(),
      historyPending: Boolean(state.opsHistoryLoadPending),
      historyLoaded: Boolean(state.opsHistoryLoaded),
      historyError: state.opsHistoryLastError,
      historyFullLoaded: Boolean(state.opsHistoryFullLoaded)
    };
    if (renderActivityPanel) {
      renderAdminOpsHistoryImpl(getHistoryElement(), runModel, historyRenderOptions);
    }
    if (renderDeferredPanels) {
      getRenderScheduler()(() => {
        if (renderToken !== opsRenderToken) return;
        const deferredContext = deriveLiveRunContext(
          getCachedTaskStatePayload(),
          getCachedRegistryConflictsPayload()
        );
        renderSourcePolicyReviewQueue(getCachedSourcePolicyPayload());
        renderRegistryConflictsQueue(getCachedRegistryConflictsPayload());
        renderAdminOpsFetcherMetricsImpl(
          refs.adminOpsFetcherMetricsEl,
          fetcherMetricsPayload,
          deriveFetcherFailureSummary(state.latestFetcherReportCache || {}),
          {
            onDedupReviewAction: handleDedupReviewAction,
            onCopySectionDiagnostics: handleCopySectionDiagnostics,
            onRefreshAuditArtifacts: handleRefreshAuditArtifacts,
            onRefreshTaskFailureAttempts: handleRefreshTaskFailureAttempts,
            onRefreshPerformanceProfile: handleRefreshPerformanceProfile,
            onLoadDebugDiagnostics: handleLoadDebugDiagnostics,
            includeDebugDiagnostics: Boolean(state.opsDebugDiagnosticsLoaded),
            debugDiagnosticsLoading: Boolean(state.opsDebugDiagnosticsLoading),
            runModel: deferredContext.runModel
          }
        );
        renderAdminOpsDedupListsImpl(refs.adminOpsDedupListsEl, fetcherMetricsPayload, {
          onDedupReviewAction: handleDedupReviewAction
        });
        renderAdminOpsHistoryImpl(getHistoryElement(), deferredContext.runModel, {
          ...historyRenderOptions
        });
      });
    }
    if (dispatchRefresh) {
      adminDispatch.dispatch({ type: adminActions.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
    }
    if (schedulePolling) {
      scheduleOpsHealthPolling(getOpsPollIntervalMs(liveTypes.size > 0 || registryConflictRunning));
    }
    if (scheduleDetails) {
      scheduleOpsOverviewDetailData(renderToken);
    }
  }

  // ── Bootstrap payload and top-level conductor ────────────────────────

  function applyBootstrapPayload(payload = {}) {
    const renderToken = ++opsRenderToken;
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
    // Seed the schedule model straight from the bootstrap payload so the Ops
    // panel paints real settings on first render instead of waiting for the
    // first poll (previously ~10s of "loading schedule..." after every open).
    // Degraded bootstrap schedules are not trusted over an existing model.
    if (isTrustedBootstrapSchedule(payload)) {
      rememberPipelineSchedule(normalizePipelineSchedulePayload({ schedule: payload.schedule }));
    }
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
        if (renderToken === opsRenderToken && (
          state.opsActiveAdminWorkLastActive || hasActiveAdminWorkRows()
        )) {
          schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
        }
      }).catch(() => {});
    }
    return { taskStatePayload, historyPayload };
  }

  async function loadOpsHealthData(options = {}) {
    if (state.adminBusyState.opsLoad) {
      if (options?.fromPoll) scheduleOpsHealthPolling(idlePollIntervalMs);
      return;
    }
    const renderToken = ++opsRenderToken;
    if (!initialBridgeReadyResolved) {
      initialBridgeReadyResolved = true;
      if (!(await awaitBridgeReady())) {
        scheduleOpsHealthPolling(idlePollIntervalMs);
        return;
      }
    }
    setBusyFlag("opsLoad", true);
    const showLoadingState = !options?.fromPoll && !state.latestOpsHealthCache;
    if (showLoadingState) setOpsReadinessShell();
    const measureFirstRender = !options?.fromPoll;
    if (measureFirstRender) markStep("admin_ops_health_first_render_start");
    const pipelinePayload = await loadPipelineStatusFallbackData(renderToken, {
      fromPoll: Boolean(options?.fromPoll)
    });
    if (pipelinePayload?.active) {
      state.opsActiveAdminWorkLastActive = true;
      state.opsActivePipelineOrFetchLastActive = true;
      loadPipelineScheduleData({ force: true, silent: true }).catch(() => {});
      if (canHydrateCompactDuringActiveRun() || !hasFetchKpiValues(state.latestOpsHealthCache?.kpis || {})) {
        loadFetchKpisSummaryData(renderToken, {
          force: true,
          silent: true,
          fromPoll: Boolean(options?.fromPoll)
        }).catch(() => {});
      }
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", { ok: true, source: "pipeline-status" });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: true, source: "pipeline-status" }
        );
      }
      setBusyFlag("opsLoad", false);
      loadActiveOpsSupplementalData(renderToken, {
        fromPoll: Boolean(options?.fromPoll)
      }).finally(() => {
        if (renderToken === opsRenderToken && (
          state.opsActiveAdminWorkLastActive || hasActiveAdminWorkRows()
        )) {
          schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
        }
      }).catch(() => {});
      return;
    }
    if (pipelinePayload?.degradedActive || hasPossibleActiveRunEvidence()) {
      markOpsDegradedActive(pipelinePayload?.degradedActive ? "pipeline_status_unavailable" : "possible_active_unresolved");
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", { ok: true, source: "degraded-active" });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: true, source: "degraded-active" }
        );
      }
      setBusyFlag("opsLoad", false);
      loadActiveOpsSummaryData(renderToken, {
        fromPoll: Boolean(options?.fromPoll)
      }).catch(() => {
        if (renderToken === opsRenderToken && hasPossibleActiveRunEvidence()) {
          schedulePipelineStatusPolling(getOpsPollIntervalMs(true));
        }
      });
      return;
    }
    try {
      let health;
      const useSummaryView = Boolean(options?.summary);
      const dashboardHealthPath = useSummaryView
        ? OPS_DASHBOARD_HEALTH_SUMMARY_PATH
        : "/ops/dashboard-health";
      try {
        health = useSummaryView
          ? await loadDashboardHealthSummaryData(renderToken, options)
          : await measuredGetBridge(
              dashboardHealthPath,
              "admin_dashboard_health_fetch",
              { enabled: !options?.fromPoll }
            );
      } catch (err) {
        markOpsRouteFailure(OPS_HEAVY_ROUTE_DASHBOARD);
        health = state.latestOpsHealthCache || {
          ok: true,
          status: "degraded",
          summaryView: true,
          degraded: true,
          alerts: [],
          alertsEvaluated: false,
          alertBasis: "bridge-degraded",
          suppressedAlertsCount: 0,
          kpis: {},
          schedule: {},
          scheduleDelayed: true,
          message: `Admin data delayed; retrying: ${getErrorMessage(err)}`
        };
      }
      if (renderToken !== opsRenderToken) return;
      if (health && typeof health === "object" && !Array.isArray(health)) {
        const healthForCache = isDegradedControlFallbackPayload(health)
          ? { ...health, schedule: hasActionablePipelineSchedule(health) ? health.schedule : {}, kpis: {} }
          : health;
        state.latestOpsHealthCache = mergeOpsHealth(
          state.latestOpsHealthCache || {},
          healthForCache || {},
          { summary: useSummaryView }
        );
      }
      renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || health || {}, {
        taskStatePayload: getCachedTaskStatePayload(),
        registryConflictsPayload: getCachedRegistryConflictsPayload(),
        syncTaskState: Boolean(state.latestOpsTaskStatePayload),
        dispatchRefresh: true,
        scheduleDetails: false,
        renderDeferredPanels: false
      });
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", { ok: true });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: true }
        );
      }
      loadTaskStateSummaryData(renderToken, options).catch(() => {});
      await Promise.allSettled([
        loadPipelineScheduleData({ force: true, silent: true }),
        loadOpsHistoryData({ force: true, silent: true })
      ]);
      scheduleIdleOpsHeavyHydration(renderToken, options);
    } catch (err) {
      if (measureFirstRender) {
        markStep("admin_ops_health_first_render_done", {
          ok: false,
          error: String(err?.message || err || "unknown error")
        });
        measureStep(
          "admin_ops_health_first_render",
          "admin_ops_health_first_render_start",
          "admin_ops_health_first_render_done",
          { ok: false }
        );
      }
      if (hasActiveRows()) {
        renderOpsHealthSnapshot(renderToken, state.latestOpsHealthCache || {}, {
          taskStatePayload: getCachedTaskStatePayload(),
          registryConflictsPayload: getCachedRegistryConflictsPayload(),
          syncTaskState: true,
          renderDeferredPanels: false,
          renderActivityPanel: true,
          schedulePolling: true
        });
      } else {
        taskStateController.resetLifecycleTaskState();
        setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
        taskStateController.syncLiveBusyFlags(new Set());
        scheduleOpsHealthPolling(idlePollIntervalMs);
      }
    } finally {
      setBusyFlag("opsLoad", false);
    }
  }

  return {
    setOpsPlaceholders,
    setOpsReadinessShell,
    stopOpsHealthPolling,
    scheduleOpsHealthPolling,
    applyBootstrapPayload,
    loadPipelineScheduleData,
    loadPipelineStatusFallbackData,
    loadActiveOpsSummaryData: options => loadActiveOpsSummaryData(opsRenderToken, options || {}),
    loadOpsHealthData,
    loadIdleOpsHeavyHydration: options => loadIdleOpsHeavyHydration(opsRenderToken, options || {}),
    loadOpsHistoryData,
    loadOpsOverviewDetailData,
    loadRegistrySyncDiagnosticsData,
    selectOpsTab
  };
}
