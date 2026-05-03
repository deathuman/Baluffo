import { escapeHtml } from "../../../shared/ui/index.js";
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
} from "../../render.js?v=3";
import { adminService } from "../../services.js";
import { createAdminAuthController } from "../auth.js";
import { createAdminDiscoveryController } from "../discovery.js";
import {
  createAdminFetcherController
} from "../fetcher.js";
import { createRestoreActiveRunWatches } from "../live-task.js";
import { createAdminOpsController, formatBytes } from "../ops.js";
import { createAdminRegistryController } from "../registry.js";
import { createAdminSyncController } from "../sync.js";
import { createAdminOverviewController } from "./overview.js";

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
  requestConfirmationDialog
}) {
  const adminDispatch = createAdminDispatcher();
  let authController;
  let syncController;
  let opsController;
  let fetcherController;
  let discoveryController;
  let registryController;

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
    onBridgeStatusChange: status => {
      if (status === "online") {
        registryController?.loadDiscoveryData().catch(() => {});
      }
    },
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args),
    attachToActiveFetchRun: (...args) => fetcherController?.attachToActiveFetchRun?.(...args),
    loadLatestFetcherReport: options => fetcherController?.loadLatestFetcherReport?.(options),
    attachToActiveDiscoveryRun: (...args) => discoveryController?.attachToActiveDiscoveryRun?.(...args),
    loadLatestDiscoveryReport: options => discoveryController?.loadLatestDiscoveryReport?.(options),
    bridgeStatusPollIntervalMs,
    idlePollIntervalMs: opsPollIdleIntervalMs
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

  authController = createAdminAuthController({
    refs,
    emitAdminStartupMetric,
    markAdminFirstInteractive,
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
    setBridgeStatusBadge: (...args) => opsController.setBridgeStatusBadge(...args),
    renderUsersEmpty: (...args) => overviewController.renderUsersEmpty(...args),
    startBridgeStatusWatch: (...args) => opsController.startBridgeStatusWatch(...args),
    stopBridgeStatusWatch: (...args) => opsController.stopBridgeStatusWatch(...args),
    scheduleOpsHealthPolling: (...args) => opsController.scheduleOpsHealthPolling(...args),
    stopOpsHealthPolling: (...args) => opsController.stopOpsHealthPolling(...args),
    refreshOverview: (...args) => overviewController.refreshOverview(...args),
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args),
    loadDiscoveryConfig: (...args) => discoveryController.loadDiscoveryConfig(...args),
    loadOpsHealthData: (...args) => opsController.loadOpsHealthData(...args),
    loadSyncStatus: (...args) => syncController.loadSyncStatus(...args),
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
    restoreActiveRunWatches
  };
}
