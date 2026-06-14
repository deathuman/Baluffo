import { AdminConfig as adminConfig } from "../../shared/config/admin-config.js";
import { awaitDesktopBootstrap } from "../../shared/local-data/desktop-client.js";
import { resolveBridgeLocalDataMode, resolveDesktopRuntimeMode } from "../../shared/local-data/runtime-context.js";
import { showToast, setText } from "../../shared/ui/index.js?v=6";
import { emitStartupMetric, logError, markFirstInteractive } from "../../shared/app-boot.js";
import { createPerfMarks } from "../../shared/perf-marks.js";
import {
  appendAdminLogRow
} from "../render.js?v=20";
import {
  getErrorMessage as getErrorMessageFromDomain,
  normalizeLogLevel as normalizeLogLevelFromDomain,
  createLogEvent as createLogEventFromDomain,
  formatLogEventText as formatLogEventTextFromDomain
} from "../domain.js";
import {
  fetchJobsFetchReportJson as fetchJobsFetchReportJsonFromData,
  emitAdminStartupMetric as emitAdminStartupMetricFromData,
  emitAdminStartupMetricsBatch as emitAdminStartupMetricsBatchFromData,
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
import { cacheAdminDom } from "./dom.js?v=12";
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
  syncDiscoveryLogDisclosure as syncDiscoveryLogDisclosureFromModule
} from "./discovery.js";
import {
  normalizeSourceFilter as normalizeSourceFilterFromModule,
  setSourceFilterValue
} from "./sources.js";
import {
  FETCHER_PRESET_META
} from "./fetcher.js?v=13";
import { createAdminRuntimeState } from "./runtime/state.js";
import { composeAdminControllers } from "./runtime/composition.js?v=25";
import { createAdminStartupMetrics } from "./runtime/effects.js";
import { createBridgeCaller } from "./runtime/actions.js";
import { resolveAdminBridgeBase } from "./runtime/bridge-base.js";
import { setStatusText, toLocalTime } from "./runtime/view.js";
import { bindAdminRuntimeEvents } from "./runtime/events.js?v=15";
import { applyAdminAdvancedBulkLayout } from "./bulk-actions.js";

const JOBS_LAST_URL_KEY = adminConfig.JOBS_LAST_URL_KEY || "baluffo_jobs_last_url";
const JOBS_FETCHER_COMMAND = adminConfig.JOBS_FETCHER_COMMAND || "python -m src.jobs_fetcher --social-enabled";
const JOBS_FETCHER_TASK_LABEL = adminConfig.JOBS_FETCHER_TASK_LABEL || "Run jobs fetcher";
const JOBS_FETCH_REPORT_URL = adminConfig.JOBS_FETCH_REPORT_URL || "data/jobs-fetch-report.json";
const JOBS_AUTO_REFRESH_SIGNAL_KEY = adminConfig.JOBS_AUTO_REFRESH_SIGNAL_KEY || "baluffo_jobs_auto_refresh_signal";
const ADMIN_BRIDGE_BASE = resolveAdminBridgeBase(adminConfig);
const BRIDGE_STATUS_POLL_INTERVAL_MS = Number(adminConfig.BRIDGE_STATUS_POLL_INTERVAL_MS || 10000);
const OPS_POLL_IDLE_INTERVAL_MS = 10000;
const OPS_POLL_LIVE_INTERVAL_MS = 2000;
const ACTIVE_TASK_POLL_INTERVAL_MS = 500;
const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";
const ADMIN_SOURCE_FILTER_KEY = "baluffo_admin_source_filter";
const UNKNOWN_ERROR_TEXT = "unknown error";
const state = createAdminRuntimeState();

let refs = {};
let authController, syncController, opsController, fetcherController, discoveryController;
let registryController, overviewController, actionCenterController, inspectorController;
let restoreActiveRunWatches;
const startupMetrics = createAdminStartupMetrics({
  emitStartupMetric: (event, payload) => emitAdminStartupMetricFromData(ADMIN_BRIDGE_BASE, event, payload),
  emitStartupMetricsBatch: metrics => emitAdminStartupMetricsBatchFromData(ADMIN_BRIDGE_BASE, metrics)
});
const adminPerfMarks = createPerfMarks(startupMetrics);
const callBridge = createBridgeCaller({
  setBridgeOnline: () => opsController?.setBridgeStatusBadge("online", "Bridge Online"),
  setBridgeOffline: () => opsController?.setBridgeStatusBadge("offline", "Bridge Offline")
});

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

function getBridge(path, options = {}) {
  return callBridge(() => getBridgeFromData(ADMIN_BRIDGE_BASE, path, options));
}

function postBridge(path, payload, options = {}) {
  return callBridge(() => postBridgeFromData(ADMIN_BRIDGE_BASE, path, payload, options));
}

async function waitForAdminBridgeReady() {
  const bridgeLocalDataMode = typeof window.__baluffoBridgeLocalDataMode === "boolean"
    ? window.__baluffoBridgeLocalDataMode
    : resolveBridgeLocalDataMode();
  if (!bridgeLocalDataMode && !resolveDesktopRuntimeMode()) {
    return true;
  }
  const enableLifecycle = Boolean(window.__baluffoDesktopMode || resolveDesktopRuntimeMode());
  return awaitDesktopBootstrap({ enableLifecycle });
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

function cacheDom() {
  adminPerfMarks.markStep("admin_dom_cache_start");
  refs = cacheAdminDom(document);
  applyAdminAdvancedBulkLayout({ doc: document, refs });
  adminPerfMarks.markStep("admin_dom_cache_end");
  adminPerfMarks.measureStep("admin_dom_cache", "admin_dom_cache_start", "admin_dom_cache_end");
}

function bootAdminPage() {
  state.activeSourceFilter = normalizeSourceFilterFromModule(readSourceFilter(ADMIN_SOURCE_FILTER_KEY, "all"));
  cacheDom();
  void waitForAdminBridgeReady().catch(err => logAdminError("Admin desktop bootstrap failed", err));
  ({
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
  } = composeAdminControllers({
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
    markAdminStep: adminPerfMarks.markStep,
    measureAdminStep: adminPerfMarks.measureStep,
    syncAdminBusyUi,
    syncDiscoveryLogDisclosure,
    resetBusyFlags,
    setSourceFilter,
    readShowZeroJobs,
    normalizeSourceFilter: normalizeSourceFilterFromModule,
    toLocalTime,
    appendLogRow,
    createLogEvent,
    activeProgressPollIntervalMs: ACTIVE_TASK_POLL_INTERVAL_MS,
    bridgeStatusPollIntervalMs: BRIDGE_STATUS_POLL_INTERVAL_MS,
    opsPollIdleIntervalMs: OPS_POLL_IDLE_INTERVAL_MS,
    opsPollLiveIntervalMs: OPS_POLL_LIVE_INTERVAL_MS,
    jobsAutoRefreshSignalKey: JOBS_AUTO_REFRESH_SIGNAL_KEY,
    jobsFetcherCommand: JOBS_FETCHER_COMMAND,
    jobsFetcherTaskLabel: JOBS_FETCHER_TASK_LABEL,
    requestConfirmationDialog,
    awaitBridgeReady: waitForAdminBridgeReady
  }));
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
  actionCenterController.startPolling(); inspectorController.init();
}

export { bootAdminPage as boot };
