import { escapeHtml } from "../../../shared/ui/index.js?v=6";
import { createAdminDispatcher, ADMIN_ACTIONS } from "../../actions.js";
import {
  applySourceFilter as applySourceFilterFromDomain,
  deriveAdminRunsModel as deriveAdminRunsModelFromDomain,
  deriveSourceApprovalStatus as deriveSourceApprovalStatusFromDomain,
  deriveSourceStatus as deriveSourceStatusFromDomain,
  getOpsPollIntervalMs as getOpsPollIntervalMsFromDomain,
  getSourceDiscoveryJobsCount as getSourceDiscoveryJobsCountFromDomain,
  getSourceJobsFoundCount as getSourceJobsFoundCountFromDomain,
  mergeSourceDiscoveryCandidates as mergeSourceDiscoveryCandidatesFromDomain,
  mergeSourceStatusFromReport as mergeSourceStatusFromDomain
} from "../../domain.js";
import {
  renderTotalsHtml,
  renderUsersEmptyHtml,
  renderUsersTableHtml
} from "../../render.js?v=25";
import { adminService } from "../../services.js";
import { createAdminAuthController } from "../auth.js?v=8";
import { createAdminDiscoveryController } from "../discovery.js?v=2";
import {
  createAdminFetcherController
} from "../fetcher.js?v=15";
import { createRestoreActiveRunWatches } from "../live-task.js";
import { createAdminOpsController, formatBytes } from "../ops.js?v=35";
import { createAdminRegistryController } from "../registry.js?v=22";
import { createAdminSyncController } from "../sync.js?v=14";
import { createAdminOverviewController } from "./overview.js?v=15";
import { createActionCenterController } from "../action-center.js?v=3";
import { createAdminInspectorController } from "../inspector.js";
import { activeSummaryIndicatesAdminWork } from "../active-work-policy.js";
import {
  bootstrapScheduleNeedsRefresh as bootstrapScheduleNeedsRefreshFromDomain
} from "../../domain.js";

export function composeAdminControllers({
  state,
  refs,
  getBridge,
  postBridge,
  fetchJobsFetchReportJson,
  writeJobsAutoRefreshSignal,
  showToast,
  getErrorMessage,
  logAdminError,
  setBusyFlag,
  isSyncBusy,
  setSourceStatus,
  emitAdminStartupMetric,
  markAdminFirstInteractive,
  markAdminStep,
  measureAdminStep,
  syncAdminBusyUi,
  syncDiscoveryLogDisclosure,
  resetBusyFlags,
  setSourceFilter,
  readShowZeroJobs,
  normalizeSourceFilter,
  toLocalTime,
  appendLogRow,
  createLogEvent,
  activeProgressPollIntervalMs,
  bridgeStatusPollIntervalMs,
  opsPollIdleIntervalMs,
  opsPollLiveIntervalMs,
  jobsAutoRefreshSignalKey,
  jobsFetcherCommand,
  jobsFetcherTaskLabel,
  requestConfirmationDialog,
  awaitBridgeReady = async () => true,
  activeHydrationPolicy = "protected"
}) {
  const adminDispatch = createAdminDispatcher();
  let authController;
  let bootstrapSourceTablesLoadScheduled = false;
  let adminStartupBridgeLane = Promise.resolve();
  let adminStartupBridgeLanePending = 0;
  let adminStartupBootstrapSettled = false;
  let activeIdleRecoveryLane = Promise.resolve();
  let activeIdleRecoveryInFlight = false;
  let syncController;
  let opsController;
  let fetcherController;
  let discoveryController;
  let registryController;
  state.adminStartupBridgeHydrationInFlight = true;

  const overviewController = createAdminOverviewController({
    refs,
    adminService,
    requestConfirmationDialog,
    showToast,
    getErrorMessage,
    setSourceStatus,
    adminDispatch,
    adminActions: ADMIN_ACTIONS,
    formatBytes,
    renderTotalsHtml,
    renderUsersTableHtml,
    renderUsersEmptyHtml
  });

  opsController = createAdminOpsController({
    state,
    refs,
    getBridge,
    postBridge,
    deriveAdminRunsModel: (payload, nowMs = Date.now()) => deriveAdminRunsModelFromDomain(payload, nowMs),
    getOpsPollIntervalMs: hasLiveRuns => getOpsPollIntervalMsFromDomain(hasLiveRuns, opsPollIdleIntervalMs, opsPollLiveIntervalMs),
    loadSyncStatus: options => syncController.loadSyncStatus(options),
    setBusyFlag,
    showToast,
    getErrorMessage,
    adminDispatch,
    adminActions: ADMIN_ACTIONS,
    escapeHtml,
    onBridgeStatusChange: () => {},
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args),
    onActivePipelineIdle: (...args) => runActivePipelineIdleRecovery(...args),
    attachToActiveFetchRun: (...args) => fetcherController?.attachToActiveFetchRun?.(...args),
    loadLatestFetcherSummary: options => fetcherController?.loadLatestFetcherSummary?.(options),
    loadLatestFetcherReport: options => fetcherController?.loadLatestFetcherReport?.(options),
    attachToActiveDiscoveryRun: (...args) => discoveryController?.attachToActiveDiscoveryRun?.(...args),
    loadLatestDiscoveryReport: options => discoveryController?.loadLatestDiscoveryReport?.(options),
    activeHydrationPolicy,
    bridgeStatusPollIntervalMs,
    idlePollIntervalMs: opsPollIdleIntervalMs,
    markAdminStep,
    measureAdminStep
  });

  syncController = createAdminSyncController({
    state,
    refs,
    getBridge,
    postBridge,
    isSyncBusy,
    setBusyFlag,
    getErrorMessage,
    showToast,
    toLocalTime,
    loadOpsHealthData: (...args) => opsController.loadOpsHealthData(...args),
    scheduleOpsHealthPolling: (...args) => opsController.scheduleOpsHealthPolling(...args),
    escapeHtml
  });

  fetcherController = createAdminFetcherController({
    state,
    refs,
    getBridge,
    postBridge,
    fetchJobsFetchReportJson,
    writeJobsAutoRefreshSignal,
    showToast,
    getErrorMessage,
    logAdminError,
    setBusyFlag,
    getSourceStatusSetter: () => setSourceStatus,
    loadOpsHealthData: (...args) => opsController.loadOpsHealthData(...args),
    activeProgressPollIntervalMs,
    jobsAutoRefreshSignalKey,
    jobsFetcherCommand,
    jobsFetcherTaskLabel,
    syncSourceTablesAfterTaskCompletion: (...args) => registryController?.syncSourceTablesAfterTaskCompletion?.(...args),
    createLogEvent,
    appendLogRow
  });

  discoveryController = createAdminDiscoveryController({
    state,
    refs,
    getBridge,
    postBridge,
    setBusyFlag,
    getErrorMessage,
    logAdminError,
    showToast,
    createLogEvent,
    appendLogRow,
    loadOpsHealthData: (...args) => opsController.loadOpsHealthData(...args),
    scheduleOpsHealthPolling: (...args) => opsController.scheduleOpsHealthPolling(...args),
    activeProgressPollIntervalMs,
    syncSourceTablesAfterTaskCompletion: (...args) => registryController?.syncSourceTablesAfterTaskCompletion?.(...args),
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args)
  });

  const restoreActiveRunWatches = createRestoreActiveRunWatches({
    loadFetcherLivePayload: (...args) => fetcherController.loadFetcherLivePayload(...args),
    loadLatestFetcherSummary: options => fetcherController.loadLatestFetcherSummary(options),
    loadLatestFetcherReport: options => fetcherController.loadLatestFetcherReport(options),
    fetcherController,
    loadDiscoveryLivePayload: (...args) => discoveryController.loadDiscoveryLivePayload(...args),
    loadLatestDiscoveryReport: options => discoveryController.loadLatestDiscoveryReport(options),
    discoveryController
  });

  registryController = createAdminRegistryController({
    state,
    refs,
    getBridge,
    postBridge,
    fetchJobsFetchReportJson,
    mergeSourceDiscoveryCandidates: (...args) => mergeSourceDiscoveryCandidatesFromDomain(...args),
    mergeSourceStatusFromReport: (...args) => mergeSourceStatusFromDomain(...args),
    applySourceFilter: rows => applySourceFilterFromDomain(rows, state.activeSourceFilter),
    getSourceJobsFoundCount: (...args) => getSourceJobsFoundCountFromDomain(...args),
    getSourceDiscoveryJobsCount: (...args) => getSourceDiscoveryJobsCountFromDomain(...args),
    deriveSourceStatus: (...args) => deriveSourceStatusFromDomain(...args),
    deriveSourceApprovalStatus: (...args) => deriveSourceApprovalStatusFromDomain(...args),
    readShowZeroJobs,
    normalizeSourceFilter,
    adminDispatch,
    adminActions: ADMIN_ACTIONS,
    appendDiscoveryLog: (...args) => discoveryController.appendDiscoveryLog(...args),
    formatManualCheckFailureMessage: (...args) => discoveryController.formatManualCheckFailureMessage(...args),
    loadOpsHealthData: (...args) => opsController.loadOpsHealthData(...args),
    setBusyFlag,
    showToast,
    getErrorMessage
  });

  const actionCenterController = createActionCenterController({
    refs,
    getBridge,
    postBridge,
    showToast,
    logAdminError,
    shouldDeferCoreSignals: () => Boolean(
      state.opsActiveAdminWorkLastActive
      || state.opsActivePipelineOrFetchLastActive
      || state.adminBusyState?.livePipelineRunning
      || state.adminBusyState?.liveFetchRunning
      || state.adminBusyState?.liveDiscoveryRunning
    ),
    shouldDeferStorageHealth: () => Boolean(
      state.adminStartupBridgeHydrationInFlight
      || state.adminBusyState?.discoveryLoad
      || state.opsActiveAdminWorkLastActive
      || state.opsActivePipelineOrFetchLastActive
      || state.adminBusyState?.livePipelineRunning
      || state.adminBusyState?.liveFetchRunning
      || state.adminBusyState?.liveDiscoveryRunning
    ),
    onSyncStatus: payload => {
      state.latestSyncStatusCache = payload || null;
      syncController.renderSyncStatus(payload || {});
    }
  });

  const inspectorController = createAdminInspectorController({
    refs,
    getBridge,
    postBridge,
    showToast,
    logAdminError
  });

  async function loadPostInteractiveDiagnostics() {
    return null;
  }

  function refreshAdminStartupBridgeHydrationState() {
    state.adminStartupBridgeHydrationInFlight = Boolean(
      !adminStartupBootstrapSettled
      || adminStartupBridgeLanePending > 0
    );
  }

  function markAdminStartupBootstrapSettled() {
    adminStartupBootstrapSettled = true;
    refreshAdminStartupBridgeHydrationState();
  }

  function enqueueAdminStartupBridgeTask(task) {
    adminStartupBridgeLanePending += 1;
    refreshAdminStartupBridgeHydrationState();
    const taskPromise = adminStartupBridgeLane.catch(() => {}).then(async () => {
      try {
        return await task();
      } finally {
        adminStartupBridgeLanePending = Math.max(0, adminStartupBridgeLanePending - 1);
        refreshAdminStartupBridgeHydrationState();
      }
    });
    adminStartupBridgeLane = taskPromise.catch(() => {});
    return taskPromise;
  }

  function isAuthoritativeSyncPayload(payload) {
    return Boolean(
      payload
      && typeof payload === "object"
      && !Array.isArray(payload)
      && payload.degraded !== true
      && payload.delayed !== true
      && payload.config
      && typeof payload.config === "object"
      && !Array.isArray(payload.config)
    );
  }

  function renderBootstrapSyncPayload(syncPayload) {
    if (isAuthoritativeSyncPayload(syncPayload)) {
      state.latestSyncStatusCache = syncPayload || null;
      syncController.renderSyncStatus(syncPayload || {}, { forceForm: true });
      return;
    }
    if (state.latestSyncStatusCache) {
      syncController.renderSyncStatus(state.latestSyncStatusCache || {});
      return;
    }
    syncController.renderSyncStatus({
      ok: true,
      summaryView: true,
      degraded: true,
      delayed: true
    });
  }

  function loadActiveIdleOpsSummary() {
    return typeof opsController.loadActiveOpsSummaryData === "function"
      ? opsController.loadActiveOpsSummaryData({
        fromPoll: false,
        returnMeta: true,
        summaryOnly: true,
        silent: true
      })
      : Promise.resolve(null);
  }

  function runActivePipelineIdleRecovery(meta = {}) {
    if (activeIdleRecoveryInFlight) {
      return activeIdleRecoveryLane;
    }
    activeIdleRecoveryInFlight = true;
    activeIdleRecoveryLane = activeIdleRecoveryLane.catch(() => {}).then(async () => {
      try {
        await loadActiveIdleOpsSummary().catch(err => {
          logAdminError("Admin active idle final task-state refresh delayed.", err);
        });
        await opsController.loadPipelineScheduleData({
          silent: true,
          force: true,
          deferIdleHydration: true
        }).catch(err => {
          logAdminError("Admin active idle schedule refresh delayed.", err);
        });
        await opsController.loadOpsHistoryData({
          silent: true,
          force: true
        }).catch(err => {
          logAdminError("Admin active idle operations activity refresh delayed.", err);
        });
        await syncController.loadSyncStatus({
          silent: true,
          forceForm: false,
          includeLive: false,
          summary: true
        }).catch(err => {
          logAdminError("Admin active idle sync refresh delayed.", err);
        });
        await opsController.loadIdleOpsHeavyHydration({
          silent: true,
          renderWithCurrentToken: true
        }).catch(err => {
          logAdminError("Admin active idle summary refresh delayed.", err);
        });
        return await registryController?.refreshSourceTablesAfterActiveRunIdle?.(meta);
      } finally {
        activeIdleRecoveryInFlight = false;
      }
    });
    return activeIdleRecoveryLane;
  }

  async function loadCriticalBootstrapFallbacks() {
    const activeSummary = typeof opsController.loadActiveOpsSummaryData === "function"
      ? await opsController.loadActiveOpsSummaryData({
        fromPoll: false,
        returnMeta: true,
        summaryOnly: true,
        silent: true
      }).catch(() => null)
      : null;
    const activeAdminWork = activeSummaryIndicatesAdminWork(activeSummary);
    if (activeAdminWork) {
      registryController.markSourceTablesDelayedForActiveWork?.("active_admin_work", { onlyIfPlaceholder: false });
    }
    const tasks = [
      overviewController.refreshOverview({
        detail: "summary",
        scheduleFullRefresh: false,
        timeoutMs: 5000
      }),
      syncController.loadSyncStatus({
        silent: true,
        forceForm: true,
        includeLive: false,
        summary: true
      }),
      opsController.loadPipelineScheduleData({
        silent: true,
        force: true
      }),
      opsController.loadOpsHistoryData({
        silent: true,
        force: true
      })
    ];
    const results = await Promise.allSettled(tasks);
    results.forEach((result, index) => {
      if (result.status === "rejected") {
        const labels = ["Admin overview fallback", "Sync status fallback", "Pipeline schedule fallback", "Operations activity fallback"];
        logAdminError(labels[index] || "Admin bootstrap fallback", result.reason);
      }
    });
    return results;
  }

  function scheduleBootstrapSourceTablesLoad() {
    if (bootstrapSourceTablesLoadScheduled) return;
    bootstrapSourceTablesLoadScheduled = true;
    registryController.markSourceTablesLoadingForBootstrap?.();
    enqueueAdminStartupBridgeTask(() => (
      registryController.loadDiscoveryData({
        sourceTablesOnly: true,
        logChanges: false,
        skipIfFreshMs: 10000,
        suppressRegistryRetry: true
      })
    )).catch(err => {
        logAdminError("Failed to load Admin source tables after bootstrap", err);
      });
  }

  function scheduleBootstrapOpsFallbackHydration({
    bootstrapScheduleNeedsRefresh = false,
    bootstrapSyncNeedsRefresh = false
  } = {}) {
    if (bootstrapSyncNeedsRefresh) {
      enqueueAdminStartupBridgeTask(() => syncController.loadSyncStatus({
        silent: true,
        forceForm: false,
        includeLive: false,
        summary: true
      })).catch(err => {
        logAdminError("Admin sync status fallback refresh delayed.", err);
      });
    }
    if (bootstrapScheduleNeedsRefresh) {
      enqueueAdminStartupBridgeTask(() => opsController.loadPipelineScheduleData({
        silent: true,
        force: true,
        deferIdleHydration: true
      })).catch(err => {
        logAdminError("Admin pipeline schedule fallback refresh delayed.", err);
      });
    }
    enqueueAdminStartupBridgeTask(() => opsController.loadOpsHistoryData({
      silent: true,
      force: true
    })).catch(err => {
      logAdminError("Admin operations activity fallback refresh delayed.", err);
    });
    enqueueAdminStartupBridgeTask(() => opsController.loadIdleOpsHeavyHydration({
      silent: true,
      renderWithCurrentToken: true,
      allowStartupBridgeLane: true
    })).catch(err => {
      logAdminError("Admin idle summary hydration delayed.", err);
    });
  }

  async function loadAdminBootstrap() {
    let payload;
    try {
      payload = await getBridge("/admin/bootstrap", { timeoutMs: 10000 });
    } catch (err) {
      state.adminBridgeHeavyRouteDegradedUntilMs = Date.now() + 30000;
      logAdminError("Admin bootstrap delayed; using compact fallback.", err);
      const appReady = await getBridge("/app/ready", { timeoutMs: 3500 }).catch(() => ({}));
      const pipeline = await getBridge("/tasks/run-jobs-pipeline-status", { timeoutMs: 3500 })
        .catch(() => ({ active: false, stage: "idle" }));
      payload = {
        ok: true,
        degraded: true,
        summaryView: true,
        source: "frontend-bootstrap-fallback",
        app: appReady || {},
        overview: {},
        ops: {
          ok: true,
          status: "degraded",
          summaryView: true,
          degraded: true,
          alerts: [],
          kpis: {},
          schedule: {},
          message: "Admin data delayed; retrying."
        },
        tasks: {
          current: [],
          recent: [],
          summary: true,
          pipeline
        },
        sync: {},
        registrySummary: {
          ok: true,
          summary: {},
          summaryStatus: "unavailable",
          degraded: true
        },
        schedule: {},
        pipeline,
        message: "Admin data delayed; retrying."
      };
    }
    const bootstrapDegraded = payload?.degraded === true || payload?.overview?.degraded === true;
    // Seed the schedule model first, then decide whether an early schedule GET
    // is still needed — the panel reads the model, not the raw payload.
    opsController.applyBootstrapPayload(payload || {});
    const bootstrapScheduleNeedsRefresh = bootstrapScheduleNeedsRefreshFromDomain(
      payload || {},
      state
    );
    const bootstrapSyncNeedsRefresh = !isAuthoritativeSyncPayload(payload?.sync || null);
    overviewController.renderOverview(payload?.overview || {}, { degraded: bootstrapDegraded });
    if (bootstrapDegraded) {
      overviewController.refreshOverview({
        detail: "summary",
        scheduleFullRefresh: true,
        timeoutMs: 5000,
        background: true
      }).catch(err => {
        logAdminError("Admin overview fallback refresh delayed.", err);
      });
    }
    renderBootstrapSyncPayload(payload?.sync || null);
    scheduleBootstrapSourceTablesLoad();
    scheduleBootstrapOpsFallbackHydration({ bootstrapScheduleNeedsRefresh, bootstrapSyncNeedsRefresh });
    markAdminStartupBootstrapSettled();
    return payload || null;
  }

  authController = createAdminAuthController({
    refs,
    emitAdminStartupMetric,
    markAdminFirstInteractive,
    markAdminStep,
    measureAdminStep,
    syncAdminBusyUi,
    syncDiscoveryLogDisclosure,
    resetBusyFlags,
    setSourceFilter,
    setSourceStatus,
    setFetcherLogPlaceholder: (...args) => fetcherController.setFetcherLogPlaceholder(...args),
    clearOptimisticFetchRun: (...args) => fetcherController.clearOptimisticFetchRun(...args),
    restoreActiveRunWatches,
    setDiscoveryLogPlaceholder: (...args) => discoveryController.setDiscoveryLogPlaceholder(...args),
    clearOptimisticDiscoveryRun: (...args) => discoveryController.clearOptimisticDiscoveryRun(...args),
    setManualSourceFeedback: (...args) => registryController.setManualSourceFeedback(...args),
    setOpsPlaceholders: (...args) => opsController.setOpsPlaceholders(...args),
    setOpsReadinessShell: (...args) => opsController.setOpsReadinessShell(...args),
    setBridgeStatusBadge: (...args) => opsController.setBridgeStatusBadge(...args),
    renderUsersEmpty: (...args) => overviewController.renderUsersEmpty(...args),
    startBridgeStatusWatch: (...args) => opsController.startBridgeStatusWatch(...args),
    stopBridgeStatusWatch: (...args) => opsController.stopBridgeStatusWatch(...args),
    scheduleOpsHealthPolling: (...args) => opsController.scheduleOpsHealthPolling(...args),
    stopOpsHealthPolling: (...args) => opsController.stopOpsHealthPolling(...args),
    refreshOverview: (...args) => overviewController.refreshOverview(...args),
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args),
    loadDiscoveryConfig: (...args) => discoveryController.loadDiscoveryConfig(...args),
    loadPipelineStatusFallbackData: (...args) => opsController.loadPipelineStatusFallbackData(...args),
    loadPipelineScheduleData: (...args) => opsController.loadPipelineScheduleData(...args),
    loadOpsHistoryData: (...args) => opsController.loadOpsHistoryData(...args),
    loadOpsHealthData: (...args) => opsController.loadOpsHealthData(...args),
    loadSyncStatus: (...args) => syncController.loadSyncStatus(...args),
    loadAdminBootstrap,
    loadCriticalBootstrapFallbacks,
    loadPostInteractiveDiagnostics,
    awaitLocalDataReady: awaitBridgeReady,
    logAdminError,
    showToast
  });

  return {
    authController,
    syncController,
    opsController,
    fetcherController,
    discoveryController,
    registryController,
    overviewController,
    actionCenterController,
    inspectorController,
    restoreActiveRunWatches
  };
}
