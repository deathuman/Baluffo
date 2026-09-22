import {
  renderAdminOpsAlerts,
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsHistory,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsTrends,
  renderDiscoveryCandidateReviewHtml
} from "../../render.js";
import { renderAdminSourcePolicyReview } from "../../render/source-policy-review.js";
import { renderAdminRegistryConflicts } from "../../render/registry-conflicts.js";
import { getObjectValue } from "../../domain/ops-shape-utils.js";
import { mergeOpsHealth } from "../../domain/ops-merge-model.js";
import {
  normalizePipelineSchedulePayload,
  hasKnownPipelineSchedule,
  hasPipelineScheduleNextRun
} from "../../domain/ops-schedule-model.js";
import { markFetchKpisDeferredDuringActiveRun, hasFetchKpiValues } from "../../domain/ops-fetch-kpi-model.js";
import { isAbortAcceptedResult } from "../../domain/ops-abort-model.js";
import {
  shouldKeepExistingActiveTaskState,
  buildPipelineTaskStatePayload
} from "../../domain/ops-pipeline-status-model.js";
import {
  toDiscoveryBadgeState,
  isLoadedDiscoveryReport,
  isLoadedDedupPayload,
  hasRegistrySyncDetails
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
import { createOpsRunState } from "./health-run-state.js";
import { createOpsPipelineSchedule } from "./health-pipeline-schedule.js";
import { createOpsRenderHub } from "./health-render.js";
import { createOpsTabs } from "./health-tabs.js";
import { createOpsBootstrapSeeder } from "./health-bootstrap.js";
import { createOpsLoaders } from "./health-loaders.js";

const OPS_TASK_STATE_SUMMARY_PATH = "/ops/task-state?view=summary";
const OPS_DASHBOARD_HEALTH_SUMMARY_PATH = "/ops/dashboard-health?view=summary";
const JOBS_PIPELINE_STATUS_PATH = "/tasks/run-jobs-pipeline-status";
const JOBS_PIPELINE_SCHEDULE_PATH = "/tasks/jobs-pipeline-schedule";
const OPS_FETCH_KPIS_SUMMARY_PATH = "/ops/fetch-kpis?view=summary";
const OPS_TAB_COUNTS_SUMMARY_PATH = "/admin/ops-tab-counts?view=summary";
const OPS_HISTORY_STARTUP_PATH = "/ops/history?limit=2";
const OPS_HISTORY_DETAIL_PATH = "/ops/history?limit=80";
const OPS_AUTHORITY_FETCH_TIMEOUT_MS = 5000;
const OPS_IDLE_HEAVY_HYDRATION_DELAY_MS = 0;
const OPS_AUTHORITY_RETRY_BASE_MS = 3000;
const OPS_AUTHORITY_RETRY_MAX_MS = 30000;
const OPS_FETCHER_METRICS_DETAIL_PATH = "/ops/fetcher-metrics?windowRuns=80";
const OPS_DISCOVERY_AUDIT_ARTIFACTS_PATH = "/ops/discovery-audit-artifacts";
const OPS_TASK_FAILURE_ATTEMPTS_PATH = "/ops/task-failure-attempts";
const OPS_PERFORMANCE_PROFILE_PATH = "/ops/performance-profile";
const SOURCE_POLICY_DETAIL_PATH = "/source-policy/recommendations";
const REGISTRY_CONFLICTS_SUMMARY_PATH = "/registry/conflicts?view=summary";
const REGISTRY_CONFLICTS_DETAIL_PATH = "/registry/conflicts";
const OPS_HEAVY_ROUTE_DASHBOARD = "dashboard-health";
const OPS_HEAVY_ROUTE_REGISTRY_CONFLICTS = "registry-conflicts";
const OPS_HEAVY_ROUTE_TAB_COUNTS = "ops-tab-counts";
const OPS_HEAVY_ROUTE_BACKOFF_BASE_MS = 5000;
const OPS_HEAVY_ROUTE_BACKOFF_MAX_MS = 30000;

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

/**
 * Thin coordinator for the admin Ops health surface. It owns render-token
 * sequencing, route backoff and measurement helpers; every other concern lives
 * in a sibling `health-*.js` leaf.
 */
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
  let opsRenderToken = 0;

  function currentRenderToken() {
    return opsRenderToken;
  }

  function nextRenderToken() {
    return ++opsRenderToken;
  }

  function isStaleRenderToken(renderToken) {
    return renderToken !== opsRenderToken;
  }

  function canHydrateCompactDuringActiveRun() {
    return String(activeHydrationPolicy || "protected").trim().toLowerCase() === "desktop";
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

  // The render hub and the loader leaf reference each other (the hub schedules
  // polling; the loaders repaint). These forwarders break the cycle: both sides
  // are constructed below and only invoked after construction completes.
  const renderOpsHealthSnapshot = (...args) => renderHub.renderOpsHealthSnapshot(...args);
  const renderDeferredHistoryDetails = (...args) => renderHub.renderDeferredHistoryDetails(...args);
  const renderDeferredOverviewDetails = (...args) => renderHub.renderDeferredOverviewDetails(...args);
  const buildFetcherMetricsPayload = (...args) => renderHub.buildFetcherMetricsPayload(...args);
  const rerenderOpsTabBadges = (...args) => renderHub.rerenderOpsTabBadges(...args);
  const setOpsPlaceholders = (...args) => renderHub.setOpsPlaceholders(...args);
  const setOpsReadinessShell = (...args) => renderHub.setOpsReadinessShell(...args);
  const scheduleOpsHealthPolling = (...args) => loaders.scheduleOpsHealthPolling(...args);
  const scheduleOpsOverviewDetailData = (...args) => overviewHydration.scheduleOpsOverviewDetailData(...args);
  const loadOpsHealthData = (...args) => loaders.runOpsHealthData(...args);
  const renderPipelineScheduleModel = () => pipelineSchedule.renderPipelineScheduleModel();
  const rememberPipelineSchedule = schedule => pipelineSchedule.rememberPipelineSchedule(schedule);

  // ── Idle heavy hydration (coordinator-owned source shape) ────────────
  // tests/frontend/unit/admin-startup-diagnostics.test.mjs matches these two
  // function bodies as source text, so they keep their original form here.

  let opsIdleHeavyHydrationTimer = null;
  let opsIdleHeavyHydrationInFlight = false;

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

  const runState = createOpsRunState({
    state,
    refs,
    markStep,
    setBusyFlag,
    renderOpsHealthSnapshot,
    currentRenderToken,
    onActivePipelineIdle,
    deriveAdminRunsModel,
    taskStateController
  });

  const {
    getCachedTaskStatePayload,
    getCachedHistoryPayload,
    getCachedSourcePolicyPayload,
    getCachedRegistryConflictsPayload,
    getCachedDiscoveryAuditArtifactsPayload,
    getCachedTaskFailureAttemptsPayload,
    getCachedPerformanceProfilePayload,
    hasActiveRows,
    hasPendingOpsAbort,
    setPendingOpsAbort,
    clearPendingOpsAbort,
    clearAllPendingOpsAborts,
    applyOptimisticAbortRow,
    hasOptimisticRows,
    markActiveIdleRecoveryCooldown,
    hasPossiblePipelineOrFetchEvidence,
    hasPossibleActiveRunEvidence,
    markOpsDegradedActive,
    clearOpsDegradedActive,
    hasActivePipelineOrFetchRows,
    hasActiveAdminWorkRows,
    notifyActiveAdminWorkIdleIfNeeded,
    deriveLiveRunContext,
    getScheduleElement,
    getHistoryElement
  } = runState;

  function scheduleOpsHistoryRetry() {
    if (state.opsHistoryRetryTimer) return;
    const failures = Math.max(1, Number(state.opsHistoryFailureCount || 1));
    const delayMs = Math.min(OPS_AUTHORITY_RETRY_MAX_MS, OPS_AUTHORITY_RETRY_BASE_MS * (2 ** Math.min(3, failures - 1)));
    state.opsHistoryRetryTimer = maybeUnrefTimer(setTimeout(() => {
      state.opsHistoryRetryTimer = null;
      loadOpsHistoryData({ force: true, silent: true }).catch(() => {});
    }, delayMs));
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
    schedulePipelineScheduleRetry: () => pipelineSchedule.schedulePipelineScheduleRetry(),
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
    stopPipelineStatusPolling: (...args) => loaders.stopPipelineStatusPolling(...args),
    schedulePipelineStatusPolling: (...args) => loaders.schedulePipelineStatusPolling(...args),
    queueIdleRecoveryHealthLoad: (...args) => loaders.queueIdleRecoveryHealthLoad(...args),
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
    schedulePipelineStatusPolling: (...args) => loaders.schedulePipelineStatusPolling(...args),
    stopPipelineStatusPolling: (...args) => loaders.stopPipelineStatusPolling(...args),
    queueIdleRecoveryHealthLoad: (...args) => loaders.queueIdleRecoveryHealthLoad(...args),
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
    loadRegistryConflictsMore: () => loadRegistryConflictsMore(),
    loadOpsOverviewDetailData: renderToken => loadOpsOverviewDetailData(renderToken),
    loadActiveOpsSummaryData,
    applyOptimisticAbortRow,
    setPendingOpsAbort,
    clearPendingOpsAbort,
    hasPendingOpsAbort,
    isAbortAcceptedResult,
    hasActivePipelineOrFetchRows,
    renderAdminRegistryConflictsImpl,
    renderDiscoveryCandidateReviewHtml,
    toDiscoveryBadgeState,
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

  const pipelineSchedule = createOpsPipelineSchedule({
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
  });

  const renderHub = createOpsRenderHub({
    state,
    refs,
    postBridge,
    showToast,
    getErrorMessage,
    escapeHtml,
    adminDispatch,
    adminActions,
    getOpsPollIntervalMs,
    renderScheduler,
    getFrontendPerfCounters,
    currentRenderToken,
    deriveAdminRunsModel,
    renderAdminOpsAlertsImpl,
    renderAdminOpsKpisImpl,
    renderAdminOpsDedupListsImpl,
    renderAdminOpsFetcherMetricsImpl,
    renderAdminOpsTrendsImpl,
    renderAdminOpsHistoryImpl,
    getCachedTaskStatePayload,
    getCachedHistoryPayload,
    getCachedSourcePolicyPayload,
    getCachedRegistryConflictsPayload,
    getCachedDiscoveryAuditArtifactsPayload,
    getCachedTaskFailureAttemptsPayload,
    getCachedPerformanceProfilePayload,
    hasActiveRows,
    hasActivePipelineOrFetchRows,
    deriveLiveRunContext,
    getHistoryElement,
    renderPipelineScheduleModel,
    renderSourcePolicyReviewQueue,
    renderRegistryConflictsQueue,
    scheduleOpsHealthPolling,
    scheduleOpsOverviewDetailData,
    loadOpsHealthData,
    taskStateController,
    handleDedupReviewAction,
    handleCopySectionDiagnostics,
    handleCopyRunDiagnostics,
    handleRefreshAuditArtifacts,
    handleRefreshTaskFailureAttempts,
    handleRefreshPerformanceProfile,
    handleAbortRun,
    handleLoadDebugDiagnostics: (...args) => overviewHydration.handleLoadDebugDiagnostics(...args)
  });

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
    loadSourcePolicyDetail,
    loadRegistryConflictsMore,
    loadActiveOpsTabDetail
  } = overviewHydration;

  const loaders = createOpsLoaders({
    state,
    idlePollIntervalMs,
    awaitBridgeReady,
    setBusyFlag,
    markStep,
    measureStep,
    measuredGetBridge,
    getErrorMessage,
    maybeUnrefTimer,
    getOpsPollIntervalMs,
    opsRenderToken: currentRenderToken,
    nextRenderToken,
    getCachedTaskStatePayload,
    getCachedRegistryConflictsPayload,
    hasActiveRows,
    hasActiveAdminWorkRows,
    hasPossibleActiveRunEvidence,
    markOpsDegradedActive,
    markOpsRouteFailure,
    canHydrateCompactDuringActiveRun,
    loadPipelineStatusFallbackData,
    loadPipelineScheduleData,
    loadFetchKpisSummaryData,
    loadDashboardHealthSummaryData,
    loadOpsHealthData,
    loadTaskStateSummaryData,
    loadOpsHistoryData,
    loadActiveOpsSummaryData,
    loadActiveOpsSupplementalData,
    loadRegistryConflictsSummaryData,
    loadOpsTabCountsSummaryData,
    scheduleIdleOpsHeavyHydration,
    renderOpsHealthSnapshot,
    setOpsPlaceholders,
    setOpsReadinessShell,
    taskStateController
  });

  const tabs = createOpsTabs({
    state,
    refs,
    loadActiveOpsTabDetail
  });

  const bootstrap = createOpsBootstrapSeeder({
    state,
    nextRenderToken,
    currentRenderToken,
    getOpsPollIntervalMs,
    getCachedTaskStatePayload,
    getCachedRegistryConflictsPayload,
    hasActiveRows,
    hasActiveAdminWorkRows,
    hasActivePipelineOrFetchRows,
    canHydrateCompactDuringActiveRun,
    seedFromBootstrapPayload: payload => pipelineSchedule.seedFromBootstrapPayload(payload),
    renderOpsHealthSnapshot,
    loadActiveOpsSupplementalData,
    schedulePipelineStatusPolling: (...args) => loaders.schedulePipelineStatusPolling(...args)
  });

  const {
    applyBootstrapPayload
  } = bootstrap;

  tabs.setupOpsTabs();
  pipelineSchedule.setupPipelineScheduleControls();

  return {
    setOpsPlaceholders,
    setOpsReadinessShell,
    stopOpsHealthPolling: (...args) => loaders.stopOpsHealthPolling(...args),
    scheduleOpsHealthPolling,
    applyBootstrapPayload,
    loadPipelineScheduleData,
    loadPipelineStatusFallbackData,
    loadActiveOpsSummaryData: options => loadActiveOpsSummaryData(currentRenderToken(), options || {}),
    loadOpsHealthData,
    loadIdleOpsHeavyHydration: options => loadIdleOpsHeavyHydration(currentRenderToken(), options || {}),
    loadOpsHistoryData,
    loadOpsOverviewDetailData,
    loadRegistrySyncDiagnosticsData,
    selectOpsTab: tabs.selectOpsTab
  };
}
