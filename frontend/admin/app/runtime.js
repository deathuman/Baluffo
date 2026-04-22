import { AdminConfig as adminConfig } from "../../shared/config/admin-config.js";
import {
  escapeHtml,
  showToast,
  setText
} from "../../shared/ui/index.js";
import { emitStartupMetric, logError, markFirstInteractive } from "../../shared/app-boot.js";
import { adminService } from "../services.js";
import { createAdminDispatcher, ADMIN_ACTIONS } from "../actions.js";
import {
  renderTotalsHtml,
  renderUsersTableHtml,
  renderUsersEmptyHtml,
  appendAdminLogRow
} from "../render.js";
import {
  getErrorMessage as getErrorMessageFromDomain,
  normalizeLogLevel as normalizeLogLevelFromDomain,
  createLogEvent as createLogEventFromDomain,
  formatLogEventText as formatLogEventTextFromDomain,
  mergeSourceStatusFromReport as mergeSourceStatusFromDomain,
  applySourceFilter as applySourceFilterFromDomain,
  getSourceJobsFoundCount as getSourceJobsFoundCountFromDomain,
  deriveSourceStatus as deriveSourceStatusFromDomain,
  deriveAdminRunsModel as deriveAdminRunsModelFromDomain,
  getOpsPollIntervalMs as getOpsPollIntervalMsFromDomain
} from "../domain.js";
import {
  fetchJobsFetchReportJson as fetchJobsFetchReportJsonFromData,
  emitAdminStartupMetric as emitAdminStartupMetricFromData,
  getBridge as getBridgeFromData,
  postBridge as postBridgeFromData
} from "../data-source.js";
import {
  readSourceFilter,
  writeSourceFilter,
  readShowZeroJobs,
  writeShowZeroJobs,
  readAdminLastJobsUrl,
  writeJobsAutoRefreshSignal
} from "../state-sync/index.js";
import { requestConfirmationDialog } from "../../local-data/profile-name-dialog.js";
import { cacheAdminDom } from "./dom.js";
import {
  isSyncBusy as isSyncBusyFromModule,
  syncAdminBusyUi as syncAdminBusyUiFromModule,
  setBusyFlag as setBusyFlagFromModule,
  resetBusyFlags as resetBusyFlagsFromModule,
  toAdminViewState as toAdminViewStateFromModule
} from "./busy-state.js";
import {
  isDiscoveryMobileViewport as isDiscoveryMobileViewportFromModule,
  setDiscoveryLogOpen as setDiscoveryLogOpenFromModule,
  syncDiscoveryLogDisclosure as syncDiscoveryLogDisclosureFromModule,
  createAdminDiscoveryController
} from "./discovery.js";
import {
  normalizeSourceFilter as normalizeSourceFilterFromModule,
  setSourceFilterValue
} from "./sources.js";
import { createAdminAuthController } from "./auth.js";
import { createAdminOpsController, formatBytes } from "./ops.js";
import {
  createAdminFetcherController,
  FETCHER_PRESET_META
} from "./fetcher.js";
import { createAdminSyncController } from "./sync.js";
import { createAdminRegistryController } from "./registry.js";
import { createAdminRuntimeState } from "./runtime/state.js";
import { createAdminStartupMetrics } from "./runtime/effects.js";
import { createBridgeCaller } from "./runtime/actions.js";
import { createAdminOverviewController } from "./runtime/overview.js";
import { setStatusText, toLocalTime } from "./runtime/view.js";
import { bindAdminRuntimeEvents } from "./runtime/events.js";
import { createRestoreActiveRunWatches } from "./live-task.js";

const JOBS_LAST_URL_KEY = adminConfig.JOBS_LAST_URL_KEY || "baluffo_jobs_last_url";
const JOBS_FETCHER_COMMAND = adminConfig.JOBS_FETCHER_COMMAND || "python -m src.jobs_fetcher --social-enabled";
const JOBS_FETCHER_TASK_LABEL = adminConfig.JOBS_FETCHER_TASK_LABEL || "Run jobs fetcher";
const JOBS_FETCH_REPORT_URL = adminConfig.JOBS_FETCH_REPORT_URL || "data/jobs-fetch-report.json";
const JOBS_AUTO_REFRESH_SIGNAL_KEY = adminConfig.JOBS_AUTO_REFRESH_SIGNAL_KEY || "baluffo_jobs_auto_refresh_signal";
const ADMIN_BRIDGE_BASE = adminConfig.ADMIN_BRIDGE_BASE || "http://127.0.0.1:8877";
const BRIDGE_STATUS_POLL_INTERVAL_MS = Number(adminConfig.BRIDGE_STATUS_POLL_INTERVAL_MS || 10000);
const OPS_POLL_IDLE_INTERVAL_MS = 10000;
const OPS_POLL_LIVE_INTERVAL_MS = 2000;
const ACTIVE_TASK_POLL_INTERVAL_MS = 500;
const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";
const ADMIN_SOURCE_FILTER_KEY = "baluffo_admin_source_filter";
const UNKNOWN_ERROR_TEXT = "unknown error";

const adminDispatch = createAdminDispatcher();
const state = createAdminRuntimeState();

let refs = {};
let authController;
let syncController;
let opsController;
let fetcherController;
let discoveryController;
let registryController;
let overviewController;
let restoreActiveRunWatches;
const startupMetrics = createAdminStartupMetrics({
  emitStartupMetric: (event, payload) => emitAdminStartupMetricFromData(ADMIN_BRIDGE_BASE, event, payload)
});
const callBridge = createBridgeCaller({
  setBridgeOnline: () => opsController?.setBridgeStatusBadge("online", "Bridge Online"),
  setBridgeOffline: () => opsController?.setBridgeStatusBadge("offline", "Bridge Offline")
});

/**
 * Entry map (Admin runtime):
 * - boot initializes refs/controllers and binds events.
 * - state: ./runtime/state.js
 * - effects: ./runtime/effects.js
 * - actions: ./runtime/actions.js
 * - view: ./runtime/view.js
 * - events: ./runtime/events.js
 */

function emitAdminStartupMetric(event, payload = {}) {
  emitStartupMetric(startupMetrics, event, payload);
}

function markAdminFirstInteractive(reason) {
  markFirstInteractive(startupMetrics, reason);
}

function getErrorMessage(err) {
  return getErrorMessageFromDomain(err, UNKNOWN_ERROR_TEXT);
}

function logAdminError(context, err) {
  logError("admin", context, err);
}

function normalizeLogLevel(level) {
  return normalizeLogLevelFromDomain(level);
}

function createLogEvent(scope, messageOrEvent, level = "info") {
  return createLogEventFromDomain(scope, messageOrEvent, level);
}

function formatLogEventText(event) {
  return formatLogEventTextFromDomain(event);
}

function appendLogRow(container, event) {
  appendAdminLogRow(container, event, {
    normalizeLogLevel,
    toLocalTime,
    formatLogEventText
  });
}

function getBridge(path) {
  return callBridge(() => getBridgeFromData(ADMIN_BRIDGE_BASE, path));
}

function postBridge(path, payload, options = {}) {
  return callBridge(() => postBridgeFromData(ADMIN_BRIDGE_BASE, path, payload, options));
}

async function fetchJobsFetchReportJson(options = {}) {
  const bridgePath = options?.live ? "/ops/fetch-report?view=live" : "/ops/fetch-report";
  try {
    const bridgeReport = await getBridge(bridgePath);
    if (bridgeReport && typeof bridgeReport === "object") {
      return bridgeReport;
    }
  } catch {
    // Fall through to static report fetch.
  }
  return fetchJobsFetchReportJsonFromData(JOBS_FETCH_REPORT_URL, options);
}

function getLastJobsUrl() {
  return readAdminLastJobsUrl(JOBS_LAST_URL_KEY, "jobs.html");
}

function setSourceStatus(text) {
  setStatusText(setText, refs.adminSourceStatusEl, text);
}

function toAdminViewState() {
  return toAdminViewStateFromModule(state.adminBusyState, {
    isUnlocked: true
  });
}

function syncAdminBusyUi() {
  syncAdminBusyUiFromModule({
    busyState: state.adminBusyState,
    viewState: toAdminViewState(),
    fetcherPresetMeta: FETCHER_PRESET_META,
    refs,
    onSyncDiscoveryLogDisclosure: syncDiscoveryLogDisclosure
  });
}

function setBusyFlag(key, value) {
  setBusyFlagFromModule(state.adminBusyState, key, value);
  syncAdminBusyUi();
}

function resetBusyFlags() {
  resetBusyFlagsFromModule(state.adminBusyState);
  syncAdminBusyUi();
}

function isSyncBusy() {
  return isSyncBusyFromModule(state.adminBusyState);
}

function isDiscoveryMobileViewport() {
  return isDiscoveryMobileViewportFromModule(window.innerWidth);
}

function setDiscoveryLogOpen(nextOpen) {
  return setDiscoveryLogOpenFromModule(refs.adminDiscoveryLogDetailsEl, nextOpen, {
    onSyncStart: () => {
      state.discoveryLogDetailsSyncing = true;
    },
    onSyncEnd: () => {
      state.discoveryLogDetailsSyncing = false;
    },
    schedule: callback => window.setTimeout(callback, 0)
  });
}

function syncDiscoveryLogDisclosure() {
  return syncDiscoveryLogDisclosureFromModule(refs.adminDiscoveryLogDetailsEl, {
    isMobileViewport: isDiscoveryMobileViewport,
    hasLiveDiscovery: Boolean(
      state.adminBusyState.discoveryRun
      || state.adminBusyState.discoveryWatch
      || state.adminBusyState.liveDiscoveryRunning
    ),
    discoveryLogUserToggled: state.discoveryLogUserToggled,
    discoveryLogPreferredOpen: state.discoveryLogPreferredOpen,
    setDiscoveryLogOpen
  });
}

function setSourceFilter(value) {
  state.activeSourceFilter = setSourceFilterValue(value, {
    normalizeSourceFilter: normalizeSourceFilterFromModule,
    writeSourceFilter,
    sourceFilterKey: ADMIN_SOURCE_FILTER_KEY,
    buttons: refs.adminSourceFilterBtnEls || []
  });
  return state.activeSourceFilter;
}

function composeControllers() {
  overviewController = createAdminOverviewController({
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
    getOpsPollIntervalMs: hasLiveRuns => getOpsPollIntervalMsFromDomain(hasLiveRuns, OPS_POLL_IDLE_INTERVAL_MS, OPS_POLL_LIVE_INTERVAL_MS),
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
    bridgeStatusPollIntervalMs: BRIDGE_STATUS_POLL_INTERVAL_MS,
    idlePollIntervalMs: OPS_POLL_IDLE_INTERVAL_MS
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
    activeProgressPollIntervalMs: ACTIVE_TASK_POLL_INTERVAL_MS,
    jobsAutoRefreshSignalKey: JOBS_AUTO_REFRESH_SIGNAL_KEY,
    jobsFetcherCommand: JOBS_FETCHER_COMMAND,
    jobsFetcherTaskLabel: JOBS_FETCHER_TASK_LABEL,
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
    activeProgressPollIntervalMs: ACTIVE_TASK_POLL_INTERVAL_MS,
    syncSourceTablesAfterTaskCompletion: (...args) => registryController?.syncSourceTablesAfterTaskCompletion?.(...args),
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args)
  });

  restoreActiveRunWatches = createRestoreActiveRunWatches({
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
    mergeSourceStatusFromReport: (...args) => mergeSourceStatusFromDomain(...args),
    applySourceFilter: rows => applySourceFilterFromDomain(rows, state.activeSourceFilter),
    getSourceJobsFoundCount: (...args) => getSourceJobsFoundCountFromDomain(...args),
    deriveSourceStatus: (...args) => deriveSourceStatusFromDomain(...args),
    readShowZeroJobs,
    normalizeSourceFilter: normalizeSourceFilterFromModule,
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
}

function cacheDom() {
  refs = cacheAdminDom(document);
}

function bootAdminPage() {
  state.activeSourceFilter = normalizeSourceFilterFromModule(readSourceFilter(ADMIN_SOURCE_FILTER_KEY, "all"));
  cacheDom();
  composeControllers();
  fetcherController.applyFetcherPresetMetadata();
  bindAdminRuntimeEvents({
    state,
    refs,
    onRestoreActiveRunWatches: restoreActiveRunWatches,
    getLastJobsUrl,
    onRefreshOverview: overviewController.refreshOverview,
    fetcherController,
    discoveryController,
    registryController,
    opsController,
    syncController,
    readShowZeroJobs,
    writeShowZeroJobs,
    showZeroJobsKey: ADMIN_SHOW_ZERO_JOBS_KEY,
    onSyncDiscoveryLogDisclosure: syncDiscoveryLogDisclosure,
    onSetSourceFilter: setSourceFilter
  });
  authController.initAdminPage();
  Promise.resolve(restoreActiveRunWatches?.()).catch(() => {});
}

export { bootAdminPage as boot };
