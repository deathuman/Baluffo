import { AdminConfig as adminConfig } from "../../../admin-config.js";
import {
  escapeHtml,
  showToast,
  setText,
  bindUi,
  bindAsyncClick
} from "../../shared/ui/index.js";
import { adminService, adminPageService } from "../services.js";
import { createAdminDispatcher, ADMIN_ACTIONS } from "../actions.js";
import {
  renderTotalsHtml,
  renderUsersTableHtml,
  renderUsersEmptyHtml,
  appendAdminLogRow,
  renderSourcesTableHtml,
  renderAdminOpsAlerts,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsTrends,
  renderAdminOpsHistory
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
  normalizeOpsRuns as normalizeOpsRunsFromDomain,
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
import { setStatusText, toLocalTime } from "./runtime/view.js";
import { bindWindowResize } from "./runtime/events.js";

const JOBS_LAST_URL_KEY = adminConfig.JOBS_LAST_URL_KEY || "baluffo_jobs_last_url";
const JOBS_FETCHER_COMMAND = adminConfig.JOBS_FETCHER_COMMAND || "python scripts/jobs_fetcher.py";
const JOBS_FETCHER_TASK_LABEL = adminConfig.JOBS_FETCHER_TASK_LABEL || "Run jobs fetcher";
const JOBS_FETCH_REPORT_URL = adminConfig.JOBS_FETCH_REPORT_URL || "data/jobs-fetch-report.json";
const JOBS_AUTO_REFRESH_SIGNAL_KEY = adminConfig.JOBS_AUTO_REFRESH_SIGNAL_KEY || "baluffo_jobs_auto_refresh_signal";
const FETCH_REPORT_POLL_INTERVAL_MS = Number(adminConfig.FETCH_REPORT_POLL_INTERVAL_MS || 5000);
const FETCH_REPORT_POLL_TIMEOUT_MS = Number(adminConfig.FETCH_REPORT_POLL_TIMEOUT_MS || (10 * 60 * 1000));
const DISCOVERY_REPORT_POLL_INTERVAL_MS = Number(adminConfig.DISCOVERY_REPORT_POLL_INTERVAL_MS || 5000);
const DISCOVERY_REPORT_POLL_TIMEOUT_MS = Number(adminConfig.DISCOVERY_REPORT_POLL_TIMEOUT_MS || (10 * 60 * 1000));
const ADMIN_BRIDGE_BASE = adminConfig.ADMIN_BRIDGE_BASE || "http://127.0.0.1:8877";
const BRIDGE_STATUS_POLL_INTERVAL_MS = Number(adminConfig.BRIDGE_STATUS_POLL_INTERVAL_MS || 10000);
const OPS_POLL_IDLE_INTERVAL_MS = 10000;
const OPS_POLL_LIVE_INTERVAL_MS = 2000;
const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";
const ADMIN_SOURCE_FILTER_KEY = "baluffo_admin_source_filter";
const UNKNOWN_ERROR_TEXT = "unknown error";

const adminDispatch = createAdminDispatcher();
const state = createAdminRuntimeState({
  discoveryReportPollIntervalMs: DISCOVERY_REPORT_POLL_INTERVAL_MS,
  discoveryReportPollTimeoutMs: DISCOVERY_REPORT_POLL_TIMEOUT_MS
});

let refs = {};
let authController;
let syncController;
let opsController;
let fetcherController;
let discoveryController;
let registryController;
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
  startupMetrics.emit(event, payload);
}

function markAdminFirstInteractive(reason) {
  startupMetrics.markFirstInteractive(reason);
}

function getErrorMessage(err) {
  return getErrorMessageFromDomain(err, UNKNOWN_ERROR_TEXT);
}

function logAdminError(context, err) {
  console.error(`[admin] ${context}:`, err);
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

function postBridge(path, payload) {
  return callBridge(() => postBridgeFromData(ADMIN_BRIDGE_BASE, path, payload));
}

async function fetchJobsFetchReportJson() {
  try {
    const bridgeReport = await getBridge("/ops/fetch-report");
    if (bridgeReport && typeof bridgeReport === "object") {
      return bridgeReport;
    }
  } catch {
    // Fall through to static report fetch.
  }
  return fetchJobsFetchReportJsonFromData(JOBS_FETCH_REPORT_URL);
}

function getLastJobsUrl() {
  return readAdminLastJobsUrl(JOBS_LAST_URL_KEY, "jobs.html");
}

function setSourceStatus(text) {
  setStatusText(setText, refs.adminSourceStatusEl, text);
}

function toAdminViewState() {
  return toAdminViewStateFromModule(state.adminBusyState, {
    isUnlocked: Boolean(state.adminPin)
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

function renderTotals(totals) {
  if (refs.adminTotalsEl) refs.adminTotalsEl.innerHTML = renderTotalsHtml(totals, formatBytes);
}

function renderUsers(users) {
  if (!refs.adminUsersListEl) return;
  refs.adminUsersListEl.innerHTML = renderUsersTableHtml(users, formatBytes);
  refs.adminUsersListEl.querySelectorAll(".admin-wipe-btn").forEach(btn => {
    bindAsyncClick(btn, async () => {
      const uid = String(btn.dataset.uid || "");
      const name = String(btn.dataset.name || uid || "this account");
      await wipeAccount(uid, name);
    });
  });
}

function renderUsersEmpty(message) {
  if (refs.adminUsersListEl) refs.adminUsersListEl.innerHTML = renderUsersEmptyHtml(message);
}

async function wipeAccount(uid, name) {
  if (!state.adminPin) {
    showToast("Unlock admin before wiping accounts.", "error");
    return;
  }
  if (!uid) {
    showToast("Missing user id for wipe.", "error");
    return;
  }
  const confirmed = window.confirm(`Wipe account data for ${name || uid}? This cannot be undone.`);
  if (!confirmed) return;
  try {
    await adminService.wipeUserData(state.adminPin, uid);
    showToast("User account wiped.", "success");
    await refreshOverview();
  } catch (err) {
    showToast(`Could not wipe account: ${getErrorMessage(err)}`, "error");
  }
}

async function refreshOverview() {
  if (!state.adminPin) {
    showToast("Unlock admin to refresh overview.", "error");
    return;
  }
  try {
    const overview = await adminService.getAdminOverview(state.adminPin);
    renderTotals(overview?.totals || {});
    const users = Array.isArray(overview?.users) ? overview.users : [];
    if (users.length) {
      renderUsers(users);
    } else {
      renderUsersEmpty("No local users found.");
    }
    setSourceStatus(`Loaded ${users.length} user account(s).`);
    adminDispatch.dispatch({ type: ADMIN_ACTIONS.OVERVIEW_REFRESHED, payload: { at: new Date().toISOString() } });
  } catch (err) {
    renderUsersEmpty("Could not load admin overview.");
    setSourceStatus(`Admin overview unavailable: ${getErrorMessage(err)}`);
    showToast(`Could not load overview: ${getErrorMessage(err)}`, "error");
  }
}

function composeControllers() {
  opsController = createAdminOpsController({
    state,
    refs,
    getBridge,
    postBridge,
    normalizeOpsRuns: (runs, nowMs = Date.now()) => normalizeOpsRunsFromDomain(runs, nowMs),
    getOpsPollIntervalMs: hasLiveRuns => getOpsPollIntervalMsFromDomain(hasLiveRuns, OPS_POLL_IDLE_INTERVAL_MS, OPS_POLL_LIVE_INTERVAL_MS),
    renderAdminOpsAlerts,
    renderAdminOpsKpis,
    renderAdminOpsSchedule,
    renderAdminOpsFetcherMetrics,
    renderAdminOpsTrends,
    renderAdminOpsHistory,
    loadSyncStatus: options => syncController.loadSyncStatus(options),
    setBusyFlag,
    showToast,
    getErrorMessage,
    adminDispatch,
    adminActions: ADMIN_ACTIONS,
    escapeHtml,
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
    startOpsHealthPolling: (...args) => opsController.scheduleOpsHealthPolling(...args),
    fetchReportPollIntervalMs: FETCH_REPORT_POLL_INTERVAL_MS,
    fetchReportPollTimeoutMs: FETCH_REPORT_POLL_TIMEOUT_MS,
    jobsAutoRefreshSignalKey: JOBS_AUTO_REFRESH_SIGNAL_KEY,
    jobsFetcherCommand: JOBS_FETCHER_COMMAND,
    jobsFetcherTaskLabel: JOBS_FETCHER_TASK_LABEL,
    jobsFetchReportUrl: JOBS_FETCH_REPORT_URL,
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
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args)
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
    renderSourcesTableHtml,
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
    state,
    refs,
    services: { adminService, adminPageService },
    adminDispatch,
    adminActions: ADMIN_ACTIONS,
    emitAdminStartupMetric,
    markAdminFirstInteractive,
    syncAdminBusyUi,
    syncDiscoveryLogDisclosure,
    resetBusyFlags,
    setSourceFilter,
    setSourceStatus,
    setFetcherLogPlaceholder: (...args) => fetcherController.setFetcherLogPlaceholder(...args),
    setDiscoveryLogPlaceholder: (...args) => discoveryController.setDiscoveryLogPlaceholder(...args),
    setManualSourceFeedback: (...args) => registryController.setManualSourceFeedback(...args),
    setOpsPlaceholders: (...args) => opsController.setOpsPlaceholders(...args),
    setBridgeStatusBadge: (...args) => opsController.setBridgeStatusBadge(...args),
    renderUsersEmpty,
    startBridgeStatusWatch: (...args) => opsController.startBridgeStatusWatch(...args),
    stopBridgeStatusWatch: (...args) => opsController.stopBridgeStatusWatch(...args),
    scheduleOpsHealthPolling: (...args) => opsController.scheduleOpsHealthPolling(...args),
    stopOpsHealthPolling: (...args) => opsController.stopOpsHealthPolling(...args),
    refreshOverview,
    loadLatestFetcherReport: (...args) => fetcherController.loadLatestFetcherReport(...args),
    loadDiscoveryData: (...args) => registryController.loadDiscoveryData(...args),
    loadOpsHealthData: (...args) => opsController.loadOpsHealthData(...args),
    loadSyncStatus: (...args) => syncController.loadSyncStatus(...args),
    getErrorMessage,
    logAdminError,
    showToast
  });
}

function cacheDom() {
  refs = cacheAdminDom(document);
}

function bindEvents() {
  bindUi(refs.adminJobsBtnEl, "click", () => {
    window.location.href = getLastJobsUrl();
  });
  bindUi(refs.adminSavedBtnEl, "click", () => {
    window.location.href = "saved.html";
  });
  bindUi(refs.adminUnlockBtnEl, "click", authController.unlockAdmin);
  bindUi(refs.adminLockBtnEl, "click", authController.lockAdmin);

  if (refs.adminPinInputEl) {
    refs.adminPinInputEl.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        authController.unlockAdmin();
      }
    });
  }

  bindAsyncClick(refs.adminRefreshBtnEl, refreshOverview);
  bindAsyncClick(refs.adminRunFetcherBtnEl, () => fetcherController.triggerJobsFetcherTask({ preset: "default" }));
  bindAsyncClick(refs.adminRunFetcherIncrementalBtnEl, () => fetcherController.triggerJobsFetcherTask({ preset: "incremental" }));
  bindAsyncClick(refs.adminRunFetcherForceBtnEl, () => fetcherController.triggerJobsFetcherTask({ preset: "force_full" }));
  bindAsyncClick(refs.adminRefreshReportBtnEl, () => fetcherController.loadLatestFetcherReport());
  bindUi(refs.adminClearLogBtnEl, "click", () => fetcherController.setFetcherLogPlaceholder("Output log cleared."));
  bindAsyncClick(refs.adminRetryFailedBtnEl, async () => {
    fetcherController.appendFetcherLog(fetcherController.getFetcherPresetMeta("retry_failed").requestedLog, "warn");
    await fetcherController.triggerJobsFetcherTask({ preset: "retry_failed" });
  });
  bindAsyncClick(refs.adminCopyFailuresBtnEl, () => fetcherController.copyLatestFailureSummary());

  bindAsyncClick(refs.adminRunDiscoveryBtnEl, () => discoveryController.runDiscoveryTask());
  bindAsyncClick(refs.adminLoadDiscoveryBtnEl, () => registryController.loadDiscoveryData());
  bindAsyncClick(refs.adminApproveSourcesBtnEl, () => registryController.approveSelectedSources());
  bindAsyncClick(refs.adminRejectSourcesBtnEl, () => registryController.rejectSelectedSources());
  bindAsyncClick(refs.adminDeleteSourcesBtnEl, () => registryController.deleteSelectedSources());
  bindAsyncClick(refs.adminRestoreRejectedBtnEl, () => registryController.restoreRejectedSources());
  bindAsyncClick(refs.adminAddManualSourceBtnEl, () => registryController.addManualSource());

  if (refs.adminDiscoveryLogDetailsEl) {
    refs.adminDiscoveryLogDetailsEl.addEventListener("toggle", () => {
      if (state.discoveryLogDetailsSyncing) return;
      state.discoveryLogUserToggled = true;
      state.discoveryLogPreferredOpen = Boolean(refs.adminDiscoveryLogDetailsEl.open);
    });
  }

  bindWindowResize(() => {
    syncDiscoveryLogDisclosure();
  });

  if (refs.adminManualSourceUrlEl) {
    refs.adminManualSourceUrlEl.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        registryController.addManualSource().catch(() => {});
      }
    });
  }

  if (refs.adminShowZeroJobsToggleEl) {
    refs.adminShowZeroJobsToggleEl.checked = readShowZeroJobs(ADMIN_SHOW_ZERO_JOBS_KEY);
    refs.adminShowZeroJobsToggleEl.addEventListener("change", () => {
      writeShowZeroJobs(ADMIN_SHOW_ZERO_JOBS_KEY, Boolean(refs.adminShowZeroJobsToggleEl.checked));
      registryController.loadDiscoveryData().catch(() => {});
    });
  }

  bindAsyncClick(refs.adminRefreshOpsBtnEl, () => opsController.loadOpsHealthData());
  bindAsyncClick(refs.adminSyncTestBtnEl, () => syncController.testSyncConfig());
  bindAsyncClick(refs.adminSyncPullBtnEl, () => syncController.pullSourcesSync());
  bindAsyncClick(refs.adminSyncPushBtnEl, () => syncController.pushSourcesSync());

  [refs.adminSyncEnabledEl].forEach(el => {
    if (!el) return;
    el.addEventListener("input", () => {
      state.syncConfigDirty = true;
    });
    el.addEventListener("change", () => {
      state.syncConfigDirty = true;
      syncController.saveSyncConfig().catch(() => {});
    });
  });

  refs.adminSourceFilterBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      setSourceFilter(String(btn.dataset.sourceFilter || "all").toLowerCase());
      registryController.loadDiscoveryData().catch(() => {});
    });
  });
}

function bootAdminPage() {
  state.activeSourceFilter = normalizeSourceFilterFromModule(readSourceFilter(ADMIN_SOURCE_FILTER_KEY, "all"));
  cacheDom();
  composeControllers();
  fetcherController.applyFetcherPresetMetadata();
  bindEvents();
  authController.initAdminPage();
}

export { bootAdminPage as boot };
