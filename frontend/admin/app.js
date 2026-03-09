import { AdminConfig as adminConfig } from "../../admin-config.js";
import {
  escapeHtml,
  showToast,
  setText,
  bindUi,
  bindAsyncClick
} from "../shared/ui/index.js";
import { getLastJobsUrl as getLastJobsUrlFromData } from "../shared/data/index.js";
import { isAdminApiReady, adminService, adminPageService } from "./services.js";
import { createAdminDispatcher, ADMIN_ACTIONS } from "./actions.js";
import {
  renderTotalsHtml,
  renderUsersTableHtml,
  renderUsersEmptyHtml,
  renderSourcesTableHtml,
  appendAdminLogRow,
  renderAdminOpsAlerts,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsTrends,
  renderAdminOpsHistory
} from "./render.js";
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
} from "./domain.js";
import {
  fetchJobsFetchReportJson as fetchJobsFetchReportJsonFromData,
  getBridge as getBridgeFromData,
  postBridge as postBridgeFromData
} from "./data-source.js";
import {
  readSourceFilter,
  writeSourceFilter,
  readShowZeroJobs,
  writeShowZeroJobs
} from "./state-sync/index.js";
import { safeWriteJsonLocal } from "../local-data/storage-gateway.js";
let adminSourceStatusEl;
let adminPinGateEl;
let adminContentEl;
let adminPinInputEl;
let adminUnlockBtnEl;
let adminLockBtnEl;
let adminRefreshBtnEl;
let adminRunFetcherBtnEl;
let adminRunFetcherIncrementalBtnEl;
let adminRunFetcherForceBtnEl;
let adminRefreshReportBtnEl;
let adminClearLogBtnEl;
let adminRetryFailedBtnEl;
let adminCopyFailuresBtnEl;
let adminTotalsEl;
let adminUsersListEl;
let adminJobsBtnEl;
let adminSavedBtnEl;
let adminFetcherLogEl;
let adminRunDiscoveryBtnEl;
let adminLoadDiscoveryBtnEl;
let adminApproveSourcesBtnEl;
let adminRejectSourcesBtnEl;
let adminDeleteSourcesBtnEl;
let adminDiscoverySummaryEl;
let adminManualSourceUrlEl;
let adminAddManualSourceBtnEl;
let adminManualSourceFeedbackEl;
let adminPendingSourcesEl;
let adminActiveSourcesEl;
let adminRejectedSourcesEl;
let adminRestoreRejectedBtnEl;
let adminDiscoveryLogEl;
let adminDiscoveryLogDetailsEl;
let adminBridgeStatusBadgeEl;
let adminShowZeroJobsToggleEl;
let adminRefreshOpsBtnEl;
let adminSyncPullBtnEl;
let adminSyncPushBtnEl;
let adminSyncTestBtnEl;
let adminSyncStatusEl;
let adminSyncEnabledEl;
let adminSyncConfigHintEl;
let adminOpsAlertsEl;
let adminOpsKpisEl;
let adminOpsScheduleEl;
let adminOpsFetcherMetricsEl;
let adminOpsTrendsEl;
let adminOpsHistoryEl;
let adminFetcherProgressBadgeEl;
let adminDiscoveryProgressBadgeEl;
let adminOpsProgressBadgeEl;
let adminSourceFilterBtnEls = [];

let adminPin = "";
/**
 * @typedef {Object} AdminLogEvent
 * @property {string} timestamp
 * @property {string} level
 * @property {string} scope
 * @property {string} sourceId
 * @property {string} message
 */
/**
 * @typedef {Object} ManualSourceAddResult
 * @property {"added"|"duplicate"|"invalid"} status
 * @property {string} [sourceId]
 * @property {Object} [source]
 * @property {string} [message]
 */
/**
 * @typedef {Object} SourceCheckTriggerResult
 * @property {boolean} started
 * @property {string} [runId]
 * @property {boolean} [ok]
 * @property {string} [error]
 * @property {string} [errorCode]
 * @property {number} [jobsFound]
 * @property {boolean} [weakSignal]
 * @property {string[]} [suggestedUrls]
 * @property {boolean} [browserFallbackAttempted]
 * @property {boolean} [browserFallbackUsed]
 */
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
const FETCHER_FALLBACK_MESSAGES = {
  bridgeUnavailable: "Admin bridge unavailable, using VS Code task fallback.",
  presetNeedsBridge: "VS Code task fallback supports default fetcher runs only. Start admin bridge and retry.",
  launchPrimary: taskLabel => `Triggered VS Code task URI (primary): ${taskLabel}`,
  launchSecondary: "Triggered VS Code task URI fallback (quoted task label).",
  manualHint: "If VS Code did not open, run the manual command fallback shown below.",
  copiedManualCommand: command => `Copied manual command fallback: ${command}`,
  manualCommand: command => `Manual command fallback: ${command}`
};
const FETCHER_PRESET_META = {
  default: {
    preset: "default",
    buttonKey: "default",
    busyLabel: "Fetcher Running...",
    title: "Run the standard fetcher flow with current defaults (parallel workers, domain limits, circuit breaker).",
    ariaLabel: "Run jobs fetcher with default options"
  },
  incremental: {
    preset: "incremental",
    buttonKey: "incremental",
    busyLabel: "Incremental Running...",
    title: "Run incremental mode: skip recently successful sources based on TTL and reuse existing output.",
    ariaLabel: "Run incremental fetcher"
  },
  force_full: {
    preset: "force_full",
    buttonKey: "force",
    busyLabel: "Force Running...",
    title: "Run full fetch while ignoring circuit breaker quarantine for temporarily blocked sources.",
    ariaLabel: "Run fetcher ignoring circuit breaker"
  },
  retry_failed: {
    preset: "retry_failed",
    buttonKey: "retry",
    busyLabel: "Retry Running...",
    title: "Run fetcher only for sources that failed in the latest report, bypassing circuit breaker.",
    ariaLabel: "Retry failed sources only",
    requestedLog: "Retry failed sources requested."
  }
};

let fetcherCompletionPollTimer = null;
let fetcherCompletionPollDeadline = 0;
let fetcherLaunchAtMs = 0;
let fetcherLiveProgressState = null;
let discoveryCompletionPollTimer = null;
let discoveryCompletionPollDeadline = 0;
let discoveryLaunchAtMs = 0;
let discoveryLiveProgressState = null;
let discoveryLogRemoteOffset = 0;
let bridgeStatusPollTimer = null;
let opsHealthPollTimer = null;
let latestFetcherReportCache = null;
let latestOpsHealthCache = null;
let latestSyncStatusCache = null;
let discoveryLogDetailsSyncing = false;
let discoveryLogUserToggled = false;
let discoveryLogPreferredOpen = true;
let syncConfigDirty = false;
const adminPageState = {
  activeSourceFilter: readSourceFilterPreference(),
  selectedSourceIds: new Set()
};
const adminBusyState = {
  fetcherRun: false,
  fetcherWatch: false,
  fetcherReportLoad: false,
  syncRun: false,
  discoveryRun: false,
  discoveryWatch: false,
  discoveryLoad: false,
  discoveryWrite: false,
  manualAdd: false,
  manualCheck: false,
  opsLoad: false,
  liveFetchRunning: false,
  liveDiscoveryRunning: false,
  liveSyncRunning: false
};
const adminDispatch = createAdminDispatcher();
let activeSourceFilter = adminPageState.activeSourceFilter;

function getErrorMessage(err) {
  return getErrorMessageFromDomain(err, UNKNOWN_ERROR_TEXT);
}

function logAdminError(context, err) {
  console.error(`[admin] ${context}:`, err);
}

function isFetcherBusy() {
  return Boolean(
    adminBusyState.fetcherRun
    || adminBusyState.fetcherWatch
    || adminBusyState.fetcherReportLoad
    || adminBusyState.liveFetchRunning
  );
}

function isDiscoveryBusy() {
  return Boolean(
    adminBusyState.discoveryRun
    || adminBusyState.discoveryWatch
    || adminBusyState.discoveryLoad
    || adminBusyState.discoveryWrite
    || adminBusyState.manualAdd
    || adminBusyState.manualCheck
    || adminBusyState.liveDiscoveryRunning
  );
}

function isOpsBusy() {
  return Boolean(adminBusyState.opsLoad);
}

function isSyncBusy() {
  return Boolean(adminBusyState.syncRun || adminBusyState.liveSyncRunning);
}

function setBusyBadge(el, state, text) {
  if (!el) return;
  const normalized = String(state || "idle").toLowerCase();
  el.classList.remove("idle", "running");
  el.classList.add(normalized === "running" ? "running" : "idle");
  el.textContent = String(text || "");
}

function setButtonBusy(el, busy, busyText) {
  if (!el) return;
  if (!el.dataset.idleLabel) {
    el.dataset.idleLabel = String(el.textContent || "");
  }
  el.disabled = Boolean(busy);
  el.setAttribute("aria-disabled", busy ? "true" : "false");
  if (busy && busyText) {
    el.textContent = String(busyText);
  } else if (!busy && typeof el.dataset.idleLabel === "string") {
    el.textContent = el.dataset.idleLabel;
  }
}

function syncAdminBusyUi() {
  const fetcherBusy = isFetcherBusy();
  const discoveryBusy = isDiscoveryBusy();
  const opsBusy = isOpsBusy();
  const syncBusy = isSyncBusy();

  setButtonBusy(adminRunFetcherBtnEl, fetcherBusy, FETCHER_PRESET_META.default.busyLabel);
  setButtonBusy(adminRunFetcherIncrementalBtnEl, fetcherBusy, FETCHER_PRESET_META.incremental.busyLabel);
  setButtonBusy(adminRunFetcherForceBtnEl, fetcherBusy, FETCHER_PRESET_META.force_full.busyLabel);
  setButtonBusy(adminRetryFailedBtnEl, fetcherBusy, FETCHER_PRESET_META.retry_failed.busyLabel);
  setButtonBusy(adminRefreshReportBtnEl, Boolean(adminBusyState.fetcherReportLoad), "Loading Report...");
  setButtonBusy(adminRefreshBtnEl, false);
  setButtonBusy(adminRefreshOpsBtnEl, opsBusy, "Refreshing...");
  setButtonBusy(adminSyncTestBtnEl, syncBusy, "Testing...");
  setButtonBusy(adminSyncPullBtnEl, syncBusy, "Pull Running...");
  setButtonBusy(adminSyncPushBtnEl, syncBusy, "Push Running...");
  [adminSyncEnabledEl].forEach(el => {
    if (!el) return;
    el.disabled = syncBusy;
    el.setAttribute("aria-disabled", syncBusy ? "true" : "false");
  });

  setButtonBusy(adminRunDiscoveryBtnEl, discoveryBusy, "Discovery Running...");
  setButtonBusy(adminLoadDiscoveryBtnEl, discoveryBusy, "Loading...");
  setButtonBusy(adminApproveSourcesBtnEl, discoveryBusy, "Working...");
  setButtonBusy(adminRejectSourcesBtnEl, discoveryBusy, "Working...");
  setButtonBusy(adminDeleteSourcesBtnEl, discoveryBusy, "Working...");
  setButtonBusy(adminRestoreRejectedBtnEl, discoveryBusy, "Working...");
  setButtonBusy(adminAddManualSourceBtnEl, discoveryBusy, "Adding...");
  if (adminManualSourceUrlEl) {
    adminManualSourceUrlEl.disabled = discoveryBusy;
    adminManualSourceUrlEl.setAttribute("aria-disabled", discoveryBusy ? "true" : "false");
  }
  adminSourceFilterBtnEls.forEach(btn => {
    btn.disabled = discoveryBusy;
    btn.setAttribute("aria-disabled", discoveryBusy ? "true" : "false");
  });

  const fetcherLabel = adminBusyState.fetcherWatch
    ? "Fetcher Running"
    : adminBusyState.liveFetchRunning
      ? "Fetcher Running"
    : adminBusyState.fetcherReportLoad
      ? "Loading Report"
      : "Fetcher Idle";
  const discoveryLabel = adminBusyState.liveDiscoveryRunning ? "Discovery Running" : (discoveryBusy ? "Discovery Busy" : "Discovery Idle");
  const opsLabel = opsBusy ? "Ops Refreshing" : "Ops Idle";
  setBusyBadge(adminFetcherProgressBadgeEl, fetcherBusy ? "running" : "idle", fetcherLabel);
  setBusyBadge(adminDiscoveryProgressBadgeEl, discoveryBusy ? "running" : "idle", discoveryLabel);
  setBusyBadge(adminOpsProgressBadgeEl, opsBusy ? "running" : "idle", opsLabel);
  syncDiscoveryLogDisclosure();
}

function isDiscoveryMobileViewport() {
  return window.matchMedia("(max-width: 768px)").matches;
}

function setDiscoveryLogOpen(nextOpen) {
  if (!adminDiscoveryLogDetailsEl) return;
  const desired = Boolean(nextOpen);
  if (adminDiscoveryLogDetailsEl.open === desired) return;
  discoveryLogDetailsSyncing = true;
  adminDiscoveryLogDetailsEl.open = desired;
  window.setTimeout(() => {
    discoveryLogDetailsSyncing = false;
  }, 0);
}

function syncDiscoveryLogDisclosure() {
  if (!adminDiscoveryLogDetailsEl) return;
  const hasLiveDiscovery = Boolean(
    adminBusyState.discoveryRun || adminBusyState.discoveryWatch || adminBusyState.liveDiscoveryRunning
  );
  if (hasLiveDiscovery) {
    setDiscoveryLogOpen(true);
    return;
  }
  if (discoveryLogUserToggled) {
    setDiscoveryLogOpen(discoveryLogPreferredOpen);
    return;
  }
  setDiscoveryLogOpen(!isDiscoveryMobileViewport());
}

function setBusyFlag(key, value) {
  if (!Object.prototype.hasOwnProperty.call(adminBusyState, key)) return;
  adminBusyState[key] = Boolean(value);
  syncAdminBusyUi();
}

function resetBusyFlags() {
  Object.keys(adminBusyState).forEach(key => {
    adminBusyState[key] = false;
  });
  syncAdminBusyUi();
}

function bootAdminPage() {
  cacheDom();
  applyFetcherPresetMetadata();
  bindEvents();
  initAdminPage();
}

function getFetcherPresetMeta(preset) {
  const key = String(preset || "default").trim().toLowerCase();
  return FETCHER_PRESET_META[key] || FETCHER_PRESET_META.default;
}

function getFetcherPresetButtons() {
  return [
    { preset: "default", el: adminRunFetcherBtnEl },
    { preset: "incremental", el: adminRunFetcherIncrementalBtnEl },
    { preset: "force_full", el: adminRunFetcherForceBtnEl },
    { preset: "retry_failed", el: adminRetryFailedBtnEl }
  ];
}

function applyFetcherPresetMetadata() {
  getFetcherPresetButtons().forEach(item => {
    const btn = item?.el;
    if (!btn) return;
    const meta = getFetcherPresetMeta(item.preset);
    btn.dataset.fetcherPreset = meta.preset;
    if (meta.title) btn.title = meta.title;
    if (meta.ariaLabel) btn.setAttribute("aria-label", meta.ariaLabel);
  });
}


function cacheDom() {
  adminSourceStatusEl = document.getElementById("admin-source-status");
  adminPinGateEl = document.getElementById("admin-pin-gate");
  adminContentEl = document.getElementById("admin-content");
  adminPinInputEl = document.getElementById("admin-pin-input");
  adminUnlockBtnEl = document.getElementById("admin-unlock-btn");
  adminLockBtnEl = document.getElementById("admin-lock-btn");
  adminRefreshBtnEl = document.getElementById("admin-refresh-btn");
  adminRunFetcherBtnEl = document.getElementById("admin-run-fetcher-btn");
  adminRunFetcherIncrementalBtnEl = document.getElementById("admin-run-fetcher-incremental-btn");
  adminRunFetcherForceBtnEl = document.getElementById("admin-run-fetcher-force-btn");
  adminRefreshReportBtnEl = document.getElementById("admin-refresh-report-btn");
  adminClearLogBtnEl = document.getElementById("admin-clear-log-btn");
  adminRetryFailedBtnEl = document.getElementById("admin-retry-failed-btn");
  adminCopyFailuresBtnEl = document.getElementById("admin-copy-failures-btn");
  adminTotalsEl = document.getElementById("admin-totals");
  adminUsersListEl = document.getElementById("admin-users-list");
  adminJobsBtnEl = document.getElementById("admin-jobs-btn");
  adminSavedBtnEl = document.getElementById("admin-saved-btn");
  adminFetcherLogEl = document.getElementById("admin-fetcher-log");
  adminRunDiscoveryBtnEl = document.getElementById("admin-run-discovery-btn");
  adminLoadDiscoveryBtnEl = document.getElementById("admin-load-discovery-btn");
  adminApproveSourcesBtnEl = document.getElementById("admin-approve-sources-btn");
  adminRejectSourcesBtnEl = document.getElementById("admin-reject-sources-btn");
  adminDeleteSourcesBtnEl = document.getElementById("admin-delete-sources-btn");
  adminDiscoverySummaryEl = document.getElementById("admin-discovery-summary");
  adminManualSourceUrlEl = document.getElementById("admin-manual-source-url");
  adminAddManualSourceBtnEl = document.getElementById("admin-add-manual-source-btn");
  adminManualSourceFeedbackEl = document.getElementById("admin-manual-source-feedback");
  adminPendingSourcesEl = document.getElementById("admin-pending-sources");
  adminActiveSourcesEl = document.getElementById("admin-active-sources");
  adminRejectedSourcesEl = document.getElementById("admin-rejected-sources");
  adminRestoreRejectedBtnEl = document.getElementById("admin-restore-rejected-btn");
  adminDiscoveryLogEl = document.getElementById("admin-discovery-log");
  adminDiscoveryLogDetailsEl = document.getElementById("admin-discovery-log-details");
  adminBridgeStatusBadgeEl = document.getElementById("admin-bridge-status-badge");
  adminShowZeroJobsToggleEl = document.getElementById("admin-show-zero-jobs-toggle");
  adminRefreshOpsBtnEl = document.getElementById("admin-refresh-ops-btn");
  adminSyncPullBtnEl = document.getElementById("admin-sync-pull-btn");
  adminSyncPushBtnEl = document.getElementById("admin-sync-push-btn");
  adminSyncTestBtnEl = document.getElementById("admin-sync-test-btn");
  adminSyncStatusEl = document.getElementById("admin-sync-status");
  adminSyncEnabledEl = document.getElementById("admin-sync-enabled");
  adminSyncConfigHintEl = document.getElementById("admin-sync-config-hint");
  adminOpsAlertsEl = document.getElementById("admin-ops-alerts");
  adminOpsKpisEl = document.getElementById("admin-ops-kpis");
  adminOpsScheduleEl = document.getElementById("admin-ops-schedule");
  adminOpsFetcherMetricsEl = document.getElementById("admin-ops-fetcher-metrics");
  adminOpsTrendsEl = document.getElementById("admin-ops-trends");
  adminOpsHistoryEl = document.getElementById("admin-ops-history");
  adminFetcherProgressBadgeEl = document.getElementById("admin-fetcher-progress-badge");
  adminDiscoveryProgressBadgeEl = document.getElementById("admin-discovery-progress-badge");
  adminOpsProgressBadgeEl = document.getElementById("admin-ops-progress-badge");
  adminSourceFilterBtnEls = Array.from(document.querySelectorAll(".admin-source-filter-btn"));
}

function bindEvents() {

  bindUi(adminJobsBtnEl, "click", () => {
    const target = getLastJobsUrl();
    window.location.href = target;
  });
  bindUi(adminSavedBtnEl, "click", () => {
    window.location.href = "saved.html";
  });
  bindUi(adminUnlockBtnEl, "click", unlockAdmin);

  if (adminPinInputEl) {
    adminPinInputEl.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        unlockAdmin();
      }
    });
  }

  bindAsyncClick(adminRefreshBtnEl, refreshOverview);
  bindAsyncClick(adminRunFetcherBtnEl, () => triggerJobsFetcherTask({ preset: "default" }));
  bindAsyncClick(adminRunFetcherIncrementalBtnEl, () => triggerJobsFetcherTask({ preset: "incremental" }));
  bindAsyncClick(adminRunFetcherForceBtnEl, () => triggerJobsFetcherTask({ preset: "force_full" }));
  bindAsyncClick(adminRefreshReportBtnEl, loadLatestFetcherReport);
  bindUi(adminClearLogBtnEl, "click", () => setFetcherLogPlaceholder("Output log cleared."));
  bindAsyncClick(adminRetryFailedBtnEl, async () => {
    appendFetcherLog(getFetcherPresetMeta("retry_failed").requestedLog || "Retry failed sources requested.", "warn");
    await triggerJobsFetcherTask({ preset: "retry_failed" });
  });
  bindAsyncClick(adminCopyFailuresBtnEl, copyLatestFailureSummary);
  bindUi(adminLockBtnEl, "click", lockAdmin);
  bindAsyncClick(adminRunDiscoveryBtnEl, runDiscoveryTask);
  bindAsyncClick(adminLoadDiscoveryBtnEl, loadDiscoveryData);
  bindAsyncClick(adminApproveSourcesBtnEl, approveSelectedSources);
  bindAsyncClick(adminRejectSourcesBtnEl, rejectSelectedSources);
  bindAsyncClick(adminDeleteSourcesBtnEl, deleteSelectedSources);
  bindAsyncClick(adminRestoreRejectedBtnEl, restoreRejectedSources);
  bindAsyncClick(adminAddManualSourceBtnEl, addManualSource);

  if (adminDiscoveryLogDetailsEl) {
    adminDiscoveryLogDetailsEl.addEventListener("toggle", () => {
      if (discoveryLogDetailsSyncing) return;
      discoveryLogUserToggled = true;
      discoveryLogPreferredOpen = Boolean(adminDiscoveryLogDetailsEl.open);
    });
  }

  window.addEventListener("resize", () => {
    syncDiscoveryLogDisclosure();
  });

  if (adminManualSourceUrlEl) {
    adminManualSourceUrlEl.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        addManualSource().catch(() => {});
      }
    });
  }

  if (adminShowZeroJobsToggleEl) {
    adminShowZeroJobsToggleEl.checked = readShowZeroJobsPreference();
    adminShowZeroJobsToggleEl.addEventListener("change", () => {
      writeShowZeroJobsPreference(Boolean(adminShowZeroJobsToggleEl.checked));
      loadDiscoveryData().catch(() => {});
    });
  }

  bindAsyncClick(adminRefreshOpsBtnEl, loadOpsHealthData);
  bindAsyncClick(adminSyncTestBtnEl, testSyncConfig);
  bindAsyncClick(adminSyncPullBtnEl, pullSourcesSync);
  bindAsyncClick(adminSyncPushBtnEl, pushSourcesSync);
  [adminSyncEnabledEl].forEach(el => {
    if (!el) return;
    el.addEventListener("input", () => {
      syncConfigDirty = true;
    });
    el.addEventListener("change", () => {
      syncConfigDirty = true;
      saveSyncConfig().catch(() => {});
    });
  });

  adminSourceFilterBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const next = String(btn.dataset.sourceFilter || "all").toLowerCase();
      setSourceFilter(next);
      loadDiscoveryData().catch(() => {});
    });
  });
}

function initAdminPage() {
  syncAdminBusyUi();
  syncDiscoveryLogDisclosure();
  setSourceFilter(activeSourceFilter);
  setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
  setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
  setManualSourceFeedback("Unlock admin to add a manual source.", "muted");
  setOpsPlaceholders();
  setBridgeStatusBadge("checking", "Bridge Checking");
  if (!adminPageService.isAvailable() || !isAdminApiReady()) {
    setSourceStatus("Local storage provider unavailable.");
    if (adminPinGateEl) adminPinGateEl.classList.add("hidden");
    renderUsersEmpty("Admin view is unavailable in this browser.");
    return;
  }
}

function getLastJobsUrl() {
  return getLastJobsUrlFromData(JOBS_LAST_URL_KEY, "jobs.html");
}

function setSourceStatus(text) {
  setText(adminSourceStatusEl, text);
}

function unlockAdmin() {
  const nextPin = String(adminPinInputEl?.value || "").trim();
  if (!nextPin) {
    showToast("Enter admin PIN.", "error");
    return;
  }
  if (!adminService.verifyAdminPin(nextPin)) {
    showToast("Invalid admin PIN.", "error");
    setSourceStatus("Invalid PIN. Access denied.");
    return;
  }

  adminPin = nextPin;
  syncConfigDirty = false;
  resetBusyFlags();
  adminDispatch.dispatch({ type: ADMIN_ACTIONS.UNLOCKED });
  setSourceStatus("Admin access granted.");
  if (adminBridgeStatusBadgeEl) adminBridgeStatusBadgeEl.classList.remove("hidden");
  if (adminPinGateEl) adminPinGateEl.classList.add("hidden");
  if (adminContentEl) adminContentEl.classList.remove("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.remove("hidden");
  if (adminPinInputEl) adminPinInputEl.value = "";
  setFetcherLogPlaceholder("Loading latest jobs fetch report...");
  setDiscoveryLogPlaceholder("Loading source discovery data...");
  setManualSourceFeedback("", "muted");
  setOpsPlaceholders("Loading operations health...");
  if (adminSyncStatusEl) adminSyncStatusEl.textContent = "Loading sync status...";
  startBridgeStatusWatch();
  scheduleOpsHealthPolling(900);
  refreshOverview().catch(err => {
    logAdminError("Failed to refresh admin overview", err);
  });
  loadLatestFetcherReport({ silent: true }).catch(err => {
    logAdminError("Failed to load jobs fetch report", err);
  });
  loadDiscoveryData().catch(err => {
    logAdminError("Failed to load discovery data", err);
  });
  loadOpsHealthData().catch(err => {
    logAdminError("Failed to load ops health data", err);
  });
  loadSyncStatus({ silent: true, forceForm: true }).catch(err => {
    logAdminError("Failed to load sync status", err);
  });
}

function lockAdmin() {
  adminPin = "";
  syncConfigDirty = false;
  latestSyncStatusCache = null;
  resetBusyFlags();
  adminDispatch.dispatch({ type: ADMIN_ACTIONS.LOCKED });
  if (adminPinGateEl) adminPinGateEl.classList.remove("hidden");
  if (adminContentEl) adminContentEl.classList.add("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.add("hidden");
  if (adminBridgeStatusBadgeEl) adminBridgeStatusBadgeEl.classList.add("hidden");
  renderUsersEmpty("");
  if (adminTotalsEl) adminTotalsEl.innerHTML = "";
  stopBridgeStatusWatch();
  stopOpsHealthPolling();
  setBridgeStatusBadge("checking", "Bridge Locked");
  setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
  setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
  setManualSourceFeedback("Unlock admin to add a manual source.", "muted");
  setOpsPlaceholders();
  setSourceStatus("Enter admin PIN to access user overview.");
}

async function triggerJobsFetcherTask(runOptions = {}) {
  if (!adminPin) {
    showToast("Unlock admin before running fetcher.", "error");
    return;
  }
  if (isFetcherBusy()) {
    showToast("Fetcher task is already running.", "info");
    return;
  }
  setBusyFlag("fetcherRun", true);
  const preset = String(runOptions?.preset || "default");
  const presetMeta = getFetcherPresetMeta(preset);
  const payload = { ...runOptions };
  try {
    const bridge = await postBridge("/tasks/run-fetcher", payload);
    if (bridge && bridge.started) {
      const presetLabel = String(bridge?.preset || presetMeta.preset || "default");
      const argsLabel = Array.isArray(bridge?.args) ? bridge.args.join(" ") : "";
      appendFetcherLog(
        `Triggered fetcher via local admin bridge (preset ${presetLabel})${argsLabel ? `, args: ${argsLabel}` : ""}.`
      );
      setSourceStatus("Triggered local fetcher task via admin bridge.");
      showToast("Fetcher started via admin bridge.", "success");
      loadOpsHealthData().catch(() => {});
      loadLatestFetcherReport({ silent: true }).catch(() => {});
      startFetcherCompletionWatch();
      return;
    }
  } catch {
    appendFetcherLog(FETCHER_FALLBACK_MESSAGES.bridgeUnavailable, "warn");
  } finally {
    setBusyFlag("fetcherRun", false);
  }
  if (presetMeta.preset !== "default") {
    appendFetcherLog(FETCHER_FALLBACK_MESSAGES.presetNeedsBridge, "error");
    showToast("Fetcher preset requires admin bridge.", "error");
    return;
  }
  appendFetcherLog("Preparing jobs fetcher task launch from admin panel.");
  showToast("Attempting fetcher launch...", "info");
  const taskArgQuoted = encodeURIComponent(JSON.stringify(JOBS_FETCHER_TASK_LABEL));
  const taskArgRaw = encodeURIComponent(JOBS_FETCHER_TASK_LABEL);
  const taskUris = [
    `vscode://command/workbench.action.tasks.runTask?${taskArgRaw}`,
    `vscode://command/workbench.action.tasks.runTask?${taskArgQuoted}`
  ];

  try {
    launchVsCodeUri(taskUris[0]);
    appendFetcherLog(FETCHER_FALLBACK_MESSAGES.launchPrimary(JOBS_FETCHER_TASK_LABEL));
    setSourceStatus("Triggered VS Code task to run jobs fetcher. Check VS Code terminal for progress.");
    window.setTimeout(() => {
      launchVsCodeUri(taskUris[1]);
      appendFetcherLog(FETCHER_FALLBACK_MESSAGES.launchSecondary);
    }, 180);
    appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualHint, "warn");
    showToast("Fetcher task launch requested. Check VS Code.", "info");
  } catch (err) {
    logAdminError("Could not trigger VS Code task", err);
    appendFetcherLog(`Could not trigger VS Code task automatically: ${getErrorMessage(err)}`, "error");
    showToast("Could not trigger VS Code task. Run python scripts/jobs_fetcher.py", "error");
    setSourceStatus("Could not trigger jobs fetcher task automatically.");
    return;
  }

  if (navigator?.clipboard?.writeText) {
    navigator.clipboard.writeText(JOBS_FETCHER_COMMAND)
      .then(() => {
        appendFetcherLog(FETCHER_FALLBACK_MESSAGES.copiedManualCommand(JOBS_FETCHER_COMMAND));
      })
      .catch(() => {
        appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualCommand(JOBS_FETCHER_COMMAND), "warn");
      });
  } else {
    appendFetcherLog(FETCHER_FALLBACK_MESSAGES.manualCommand(JOBS_FETCHER_COMMAND), "warn");
  }

  loadLatestFetcherReport({ silent: true }).catch(fetchErr => {
    logAdminError("Could not load fetch report after task trigger", fetchErr);
  });
  startFetcherCompletionWatch();
}

function launchVsCodeUri(uri) {
  const launchLink = document.createElement("a");
  launchLink.href = uri;
  launchLink.style.display = "none";
  document.body.appendChild(launchLink);
  launchLink.click();
  launchLink.remove();
}

async function refreshOverview() {
  if (!adminPin) return;

  setSourceStatus("Loading admin overview...");
  try {
    const overviewResult = await adminService.getAdminOverview(adminPin);
    if (!overviewResult.ok) {
      throw new Error(overviewResult.error || "Could not load admin overview.");
    }
    const overview = overviewResult.data || { users: [], totals: {} };
    renderTotals(overview.totals);
    renderUsers(overview.users);
    setSourceStatus(`Loaded ${overview.users.length} user profiles.`);
    adminDispatch.dispatch({ type: ADMIN_ACTIONS.OVERVIEW_REFRESHED, payload: { at: new Date().toISOString() } });
  } catch (err) {
    logAdminError("Could not load admin overview", err);
    showToast("Could not load admin overview.", "error");
    if (String(err?.message || "").toLowerCase().includes("pin")) {
      lockAdmin();
      return;
    }
    setSourceStatus("Could not load admin overview.");
  }
}

function renderTotals(totals) {
  if (!adminTotalsEl) return;
  adminTotalsEl.innerHTML = renderTotalsHtml(totals, formatBytes);
}

function renderUsers(users) {
  if (!Array.isArray(users) || users.length === 0) {
    renderUsersEmpty("No stored profiles found.");
    return;
  }
  if (!adminUsersListEl) return;

  adminUsersListEl.innerHTML = renderUsersTableHtml(users, formatBytes);

  adminUsersListEl.querySelectorAll(".admin-wipe-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const uid = btn.dataset.uid || "";
      const name = btn.dataset.name || uid;
      await wipeAccount(uid, name);
    });
  });
}

function renderUsersEmpty(message) {
  if (!adminUsersListEl) return;
  adminUsersListEl.innerHTML = renderUsersEmptyHtml(message);
}

async function wipeAccount(uid, name) {
  if (!uid || !adminPin) return;

  const confirmed = window.confirm(`Permanently wipe account "${name}"? This deletes profile, saved jobs, notes, and attachments.`);
  if (!confirmed) return;

  try {
    const wipeResult = await adminService.wipeAccountAdmin(adminPin, uid);
    if (!wipeResult.ok) throw new Error(wipeResult.error || "Could not wipe account.");
    showToast(`Wiped account ${name}.`, "success");
    await refreshOverview();
  } catch (err) {
    logAdminError("Could not wipe account", err);
    showToast("Could not wipe account.", "error");
    if (String(err?.message || "").toLowerCase().includes("pin")) {
      lockAdmin();
    }
  }
}

function formatBytes(bytes) {
  const value = Math.max(0, Number(bytes) || 0);
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`;
  return `${(value / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function setDiscoveryLogPlaceholder(message) {
  if (!adminDiscoveryLogEl) return;
  adminDiscoveryLogEl.innerHTML = "";
  discoveryLogRemoteOffset = 0;
  appendDiscoveryLog(message, "muted");
}

function setManualSourceFeedback(message, level = "muted") {
  if (!adminManualSourceFeedbackEl) return;
  const normalized = String(level || "muted").toLowerCase();
  adminManualSourceFeedbackEl.textContent = String(message || "");
  adminManualSourceFeedbackEl.classList.remove("success", "warn", "error", "muted");
  adminManualSourceFeedbackEl.classList.add(
    normalized === "success" ? "success" : normalized === "warn" ? "warn" : normalized === "error" ? "error" : "muted"
  );
}

function setOpsPlaceholders(message = "Unlock admin to view operations health.") {
  if (adminSyncStatusEl) {
    adminSyncStatusEl.textContent = message;
  }
  if (adminSyncConfigHintEl) {
    adminSyncConfigHintEl.textContent = "GitHub App credentials are packaged with the app.";
  }
  if (adminOpsAlertsEl) {
    adminOpsAlertsEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
  }
  if (adminOpsKpisEl) {
    adminOpsKpisEl.innerHTML = "";
  }
  if (adminOpsScheduleEl) {
    adminOpsScheduleEl.innerHTML = "";
  }
  if (adminOpsFetcherMetricsEl) {
    adminOpsFetcherMetricsEl.innerHTML = "";
  }
  if (adminOpsTrendsEl) {
    adminOpsTrendsEl.textContent = message;
  }
  if (adminOpsHistoryEl) {
    adminOpsHistoryEl.innerHTML = `<div class="no-results">${escapeHtml(message)}</div>`;
  }
}

function populateSyncConfigForm(savedConfig, options = {}) {
  if (syncConfigDirty && !options.force) return;
  const config = savedConfig || {};
  if (adminSyncEnabledEl) {
    adminSyncEnabledEl.checked = config.enabled === null ? true : Boolean(config.enabled);
  }
}

function collectSyncConfigPayload() {
  return { enabled: Boolean(adminSyncEnabledEl?.checked) };
}

function renderSyncStatus(statusPayload, options = {}) {
  if (!adminSyncStatusEl) return;
  populateSyncConfigForm(statusPayload?.savedConfig || {}, { force: Boolean(options.forceForm) });
  const config = statusPayload?.config || {};
  const runtime = statusPayload?.runtime || {};
  const state = String(config?.state || "disabled");
  const missing = Array.isArray(config?.missing) ? config.missing : [];
  const configMessage = String(config?.message || "").trim();
  const authMode = String(config?.authMode || "github_app");
  const configPath = String(config?.configPath || "").trim();
  if (adminSyncConfigHintEl) {
    adminSyncConfigHintEl.textContent = configPath
      ? `GitHub App mode: ${authMode}. Packaged config: ${configPath}`
      : "GitHub App credentials are packaged with the app.";
  }
  const repo = String(config?.repo || "unknown");
  const branch = String(config?.branch || "main");
  const path = String(config?.path || "baluffo/source-sync.json");
  const lastPullAt = String(runtime?.lastPullAt || "");
  const lastPushAt = String(runtime?.lastPushAt || "");
  const lastError = String(runtime?.lastError || "");
  const lastResult = String(runtime?.lastResult || "");
  const lastAction = String(runtime?.lastAction || "");
  const badgeLabel = state === "ready"
    ? "Ready"
    : state === "misconfigured"
      ? "Needs Attention"
      : "Disabled";
  const summaryText = state === "disabled"
    ? "Source sync is disabled on this machine. Remote state remains untouched until you enable it again."
    : state === "misconfigured"
      ? `Source sync cannot run yet.${missing.length ? ` Missing: ${missing.join(", ")}.` : ""}${configMessage ? ` ${configMessage}` : ""}`
      : `Connected to ${repo} and ready to keep the shared source registry in sync.`;
  const meta = [
    ["Mode", authMode],
    ["Repository", repo],
    ["Branch", branch],
    ["Remote Path", path],
    ["Last Pull", lastPullAt ? toLocalTime(new Date(lastPullAt)) : "Never"],
    ["Last Push", lastPushAt ? toLocalTime(new Date(lastPushAt)) : "Never"],
    ["Last Action", lastAction || "None"],
    ["Last Result", lastResult || "None"]
  ];
  const metaHtml = meta.map(([label, value]) => `
    <div class="admin-sync-meta-item">
      <span class="admin-sync-meta-label">${escapeHtml(label)}</span>
      <div class="admin-sync-meta-value">${escapeHtml(value)}</div>
    </div>
  `).join("");
  const errorHtml = lastError
    ? `<div class="admin-sync-error">${escapeHtml(lastError)}</div>`
    : "";
  adminSyncStatusEl.innerHTML = `
    <div class="admin-sync-status-head">
      <span class="admin-sync-badge ${escapeHtml(state)}">${escapeHtml(badgeLabel)}</span>
      <span class="admin-sync-inline-note">${escapeHtml(config?.enabled ? "Local sync enabled" : "Local sync disabled")}</span>
    </div>
    <p class="admin-sync-summary">${escapeHtml(summaryText)}</p>
    <div class="admin-sync-meta-grid">${metaHtml}</div>
    ${errorHtml}
  `;
}

async function loadSyncStatus(options = {}) {
  if (!adminPin) return null;
  const silent = Boolean(options?.silent);
  const forceForm = Boolean(options?.forceForm);
  try {
    const payload = await getBridge("/sync/status");
    latestSyncStatusCache = payload || null;
    renderSyncStatus(payload || {}, { forceForm });
    return payload || null;
  } catch (err) {
    if (adminSyncStatusEl) adminSyncStatusEl.textContent = `Sync status unavailable: ${getErrorMessage(err)}`;
    if (!silent) showToast(`Could not load sync status: ${getErrorMessage(err)}`, "error");
    throw err;
  }
}

async function saveSyncConfig() {
  if (!adminPin) {
    showToast("Unlock admin to save sync settings.", "error");
    return;
  }
  if (isSyncBusy()) {
    showToast("Sync task is already running.", "info");
    return;
  }
  setBusyFlag("syncRun", true);
  try {
    const payload = collectSyncConfigPayload();
    const result = await postBridge("/sync/config", payload);
    latestSyncStatusCache = result || null;
    syncConfigDirty = false;
    renderSyncStatus(result || {}, { forceForm: true });
    showToast("Source sync preference updated.", "success");
  } catch (err) {
    showToast(`Could not save sync settings: ${getErrorMessage(err)}`, "error");
  } finally {
    setBusyFlag("syncRun", false);
  }
}

async function testSyncConfig() {
  if (!adminPin) {
    showToast("Unlock admin to test sync settings.", "error");
    return;
  }
  if (isSyncBusy()) {
    showToast("Sync task is already running.", "info");
    return;
  }
  setBusyFlag("syncRun", true);
  try {
    const result = await postBridge("/sync/test", {});
    if (result?.ok) {
      const remoteFound = Boolean(result?.remoteFound);
      const message = remoteFound ? "Sync test passed. Remote snapshot found." : "Sync test passed. Remote snapshot not created yet.";
      showToast(message, "success");
      await loadSyncStatus({ silent: true });
      return;
    }
    showToast(`Sync test failed: ${String(result?.error || "unknown error")}`, "error");
  } catch (err) {
    showToast(`Sync test failed: ${getErrorMessage(err)}`, "error");
  } finally {
    setBusyFlag("syncRun", false);
  }
}

async function pullSourcesSync() {
  if (!adminPin) {
    showToast("Unlock admin to pull source sync.", "error");
    return;
  }
  if (isSyncBusy()) {
    showToast("Sync task is already running.", "info");
    return;
  }
  setBusyFlag("syncRun", true);
  try {
    const result = await postBridge("/tasks/run-sync-pull", {});
    if (result?.started) {
      showToast("Sources sync pull started.", "success");
      await loadOpsHealthData();
      scheduleOpsHealthPolling(900);
      return;
    }
    showToast(`Sources sync pull failed: ${String(result?.error || "unknown error")}`, "error");
  } catch (err) {
    showToast(`Sources sync pull failed: ${getErrorMessage(err)}`, "error");
  } finally {
    setBusyFlag("syncRun", false);
  }
}

async function pushSourcesSync() {
  if (!adminPin) {
    showToast("Unlock admin to push source sync.", "error");
    return;
  }
  if (isSyncBusy()) {
    showToast("Sync task is already running.", "info");
    return;
  }
  setBusyFlag("syncRun", true);
  try {
    const result = await postBridge("/tasks/run-sync-push", {});
    if (result?.started) {
      showToast("Sources sync push started.", "success");
      await loadOpsHealthData();
      scheduleOpsHealthPolling(900);
      return;
    }
    showToast(`Sources sync push failed: ${String(result?.error || "unknown error")}`, "error");
  } catch (err) {
    showToast(`Sources sync push failed: ${getErrorMessage(err)}`, "error");
  } finally {
    setBusyFlag("syncRun", false);
  }
}

function stopOpsHealthPolling() {
  if (!opsHealthPollTimer) return;
  clearTimeout(opsHealthPollTimer);
  opsHealthPollTimer = null;
}

function scheduleOpsHealthPolling(delayMs) {
  stopOpsHealthPolling();
  if (!adminPin) return;
  const waitMs = Math.max(600, Number(delayMs) || OPS_POLL_IDLE_INTERVAL_MS);
  opsHealthPollTimer = setTimeout(() => {
    loadOpsHealthData({ fromPoll: true }).catch(() => {});
  }, waitMs);
}

async function loadOpsHealthData(options = {}) {
  if (!adminPin) return;
  if (adminBusyState.opsLoad) {
    if (options?.fromPoll) scheduleOpsHealthPolling(OPS_POLL_IDLE_INTERVAL_MS);
    return;
  }
  setBusyFlag("opsLoad", true);
  const showLoadingState = !options?.fromPoll && !latestOpsHealthCache;
  if (showLoadingState && adminOpsTrendsEl) adminOpsTrendsEl.textContent = "Loading operations health...";
  try {
    const [health, historyPayload] = await Promise.all([
      getBridge("/ops/health"),
      getBridge("/ops/history?limit=80")
    ]);
    let fetcherMetrics = null;
    try {
      fetcherMetrics = await getBridge("/ops/fetcher-metrics?windowRuns=80");
    } catch {
      fetcherMetrics = null;
    }
    latestOpsHealthCache = health || null;
    const runModel = normalizeOpsRuns(historyPayload?.runs || [], Date.now());
    const liveTypes = new Set(Array.isArray(runModel?.liveTypes) ? runModel.liveTypes : []);
    setBusyFlag("liveFetchRunning", liveTypes.has("fetch"));
    setBusyFlag("liveDiscoveryRunning", liveTypes.has("discovery"));
    setBusyFlag("liveSyncRunning", liveTypes.has("sync"));

    renderAdminOpsAlerts(adminOpsAlertsEl, health?.alerts || [], {
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
    renderAdminOpsKpis(adminOpsKpisEl, health?.kpis || {}, String(health?.status || "healthy"));
    renderAdminOpsSchedule(adminOpsScheduleEl, health?.schedule || {}, latestOpsHealthCache);
    renderAdminOpsFetcherMetrics(adminOpsFetcherMetricsEl, fetcherMetrics || {});
    renderAdminOpsHistory(adminOpsHistoryEl, runModel);
    renderAdminOpsTrends(adminOpsTrendsEl, historyPayload?.runs || []);
    loadSyncStatus({ silent: true }).catch(() => {});
    adminDispatch.dispatch({ type: ADMIN_ACTIONS.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
    scheduleOpsHealthPolling(getOpsPollIntervalMs(Boolean(runModel?.hasLiveRuns)));
  } catch (err) {
    setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
    setBusyFlag("liveFetchRunning", false);
    setBusyFlag("liveDiscoveryRunning", false);
    setBusyFlag("liveSyncRunning", false);
    scheduleOpsHealthPolling(OPS_POLL_IDLE_INTERVAL_MS);
  } finally {
    setBusyFlag("opsLoad", false);
  }
}

function isValidSourceFilter(value) {
  return ["all", "error", "excluded", "zero", "healthy"].includes(String(value || "").toLowerCase());
}

function setSourceFilter(value) {
  const next = isValidSourceFilter(value) ? String(value).toLowerCase() : "all";
  activeSourceFilter = next;
  writeSourceFilterPreference(next);
  adminSourceFilterBtnEls.forEach(btn => {
    const key = String(btn.dataset.sourceFilter || "").toLowerCase();
    btn.classList.toggle("active", key === next);
  });
}

function readSourceFilterPreference() {
  const value = readSourceFilter(ADMIN_SOURCE_FILTER_KEY, "all");
  return isValidSourceFilter(value) ? value : "all";
}

function writeSourceFilterPreference(value) {
  writeSourceFilter(ADMIN_SOURCE_FILTER_KEY, isValidSourceFilter(value) ? value : "all");
}

function mergeSourceStatusFromReport(rows, report, mode) {
  return mergeSourceStatusFromDomain(rows, report, mode);
}

function applySourceFilter(rows) {
  return applySourceFilterFromDomain(rows, activeSourceFilter);
}

function createLogEvent(scope, messageOrEvent, level = "info") {
  return createLogEventFromDomain(scope, messageOrEvent, level);
}

function formatLogEventText(event) {
  return formatLogEventTextFromDomain(event);
}

function appendLogRow(container, event, maxRows = 220) {
  appendAdminLogRow(container, event, {
    maxRows,
    normalizeLogLevel,
    toLocalTime,
    formatLogEventText
  });
}

function appendDiscoveryLog(message, level = "info") {
  if (!adminDiscoveryLogEl) return;
  const event = createLogEvent("discovery", message, level);
  appendLogRow(adminDiscoveryLogEl, event);
}

function appendDiscoveryLogEvent(eventLike, fallbackLevel = "muted") {
  if (!adminDiscoveryLogEl) return;
  const event = createLogEvent("discovery", eventLike, fallbackLevel);
  appendLogRow(adminDiscoveryLogEl, event);
}

function appendDiscoveryServerLogText(text) {
  const payload = String(text || "");
  if (!payload) return;
  payload.split(/\r?\n/).forEach(line => {
    const trimmed = String(line || "").trim();
    if (!trimmed) return;
    const match = trimmed.match(/^\[([^\]]+)\]\s*(.*)$/);
    if (match) {
      appendDiscoveryLogEvent({
        timestamp: match[1],
        level: "muted",
        scope: "discovery",
        message: match[2] || ""
      }, "muted");
      return;
    }
    appendDiscoveryLog(trimmed, "muted");
  });
}

async function loadDiscoveryLogChunk(options = {}) {
  if (!adminPin) return null;
  const reset = Boolean(options?.reset);
  const offset = reset ? 0 : Math.max(0, Number(discoveryLogRemoteOffset) || 0);
  const payload = await getBridge(`/discovery/log?offset=${offset}`);
  if (reset) {
    discoveryLogRemoteOffset = 0;
  }
  appendDiscoveryServerLogText(String(payload?.text || ""));
  discoveryLogRemoteOffset = Math.max(0, Number(payload?.nextOffset) || 0);
  return payload || null;
}

function formatManualCheckFailureMessage(checkResult) {
  const code = String(checkResult?.errorCode || "").toLowerCase();
  if (code === "browser_fallback_unavailable") return "Manual source check failed (browser fallback is not installed).";
  if (code === "not_found") return "Manual source check failed (404 not found).";
  if (code === "forbidden") return "Manual source check failed (403 forbidden).";
  if (code === "ssl_error") return "Manual source check failed (SSL certificate/hostname issue).";
  if (code === "dns_error") return "Manual source check failed (DNS/host resolution issue).";
  if (code === "timeout") return "Manual source check failed (timeout).";
  return "Manual source check failed.";
}

function setFetcherLogPlaceholder(message) {
  if (!adminFetcherLogEl) return;
  adminFetcherLogEl.innerHTML = "";
  appendFetcherLog(message, "muted");
}

function appendFetcherLog(message, level = "info") {
  if (!adminFetcherLogEl) return;
  const event = createLogEvent("fetcher", message, level);
  appendLogRow(adminFetcherLogEl, event);
}

function formatFetcherRuntimeOptions(report) {
  const runtime = report?.runtime || {};
  const maxWorkers = Number(runtime.maxWorkers || 0);
  const maxPerDomain = Number(runtime.maxPerDomain || 0);
  const sourceTtlMinutes = Number(runtime.sourceTtlMinutes || 0);
  const circuitBreakerFailures = Number(runtime.circuitBreakerFailures || 0);
  const circuitBreakerCooldownMinutes = Number(runtime.circuitBreakerCooldownMinutes || 0);
  const selectedSourceCount = Number(runtime.selectedSourceCount || 0);
  const seedFromExistingOutput = Boolean(runtime.seedFromExistingOutput);
  const ignoreCircuitBreaker = Boolean(runtime.ignoreCircuitBreaker);
  if (
    maxWorkers <= 0
    && maxPerDomain <= 0
    && sourceTtlMinutes <= 0
    && circuitBreakerFailures <= 0
    && circuitBreakerCooldownMinutes <= 0
    && selectedSourceCount <= 0
    && !seedFromExistingOutput
    && !ignoreCircuitBreaker
  ) {
    return "";
  }
  return [
    `workers ${maxWorkers || "n/a"}`,
    `per-domain ${maxPerDomain || "n/a"}`,
    `ttl ${sourceTtlMinutes || 0}m`,
    `circuit ${circuitBreakerFailures || 0}/${circuitBreakerCooldownMinutes || 0}m`,
    `selected ${selectedSourceCount || 0}`,
    `seed ${seedFromExistingOutput ? "on" : "off"}`,
    `ignore-cb ${ignoreCircuitBreaker ? "on" : "off"}`
  ].join(", ");
}

function formatLifecycleSummary(report) {
  const summary = report?.summary || {};
  const active = Number(summary.lifecycleActiveCount || 0);
  const likelyRemoved = Number(summary.lifecycleLikelyRemovedCount || 0);
  const archived = Number(summary.lifecycleArchivedCount || 0);
  const tracked = Number(summary.lifecycleTrackedCount || 0);
  if (active <= 0 && likelyRemoved <= 0 && archived <= 0 && tracked <= 0) {
    return "";
  }
  return `Lifecycle: active ${active.toLocaleString()}, likely removed ${likelyRemoved.toLocaleString()}, archived ${archived.toLocaleString()}, tracked ${tracked.toLocaleString()}`;
}

function normalizeLogLevel(level) {
  return normalizeLogLevelFromDomain(level);
}

async function loadLatestFetcherReport(options = {}) {
  if (!adminPin) {
    if (!options?.silent) {
      showToast("Unlock admin to load fetch report.", "error");
    }
    return;
  }
  const silent = Boolean(options.silent);
  if (adminBusyState.fetcherReportLoad) {
    if (!silent) showToast("Fetch report loading already in progress.", "info");
    return;
  }
  setBusyFlag("fetcherReportLoad", true);
  try {
    if (!silent) {
      appendFetcherLog("Loading latest jobs fetch report...");
    }

    const report = await fetchJobsFetchReportJson();
    if (!report) {
      appendFetcherLog("Could not load fetch report: unavailable or not yet generated.", "error");
      if (!silent) {
        showToast("Could not load jobs fetch report.", "error");
      }
      return;
    }
    latestFetcherReportCache = report;

    const summary = report?.summary || {};
    appendFetcherLog(
      `Summary: output ${Number(summary.outputCount || 0).toLocaleString()}, merged ${Number(summary.mergedCount || 0).toLocaleString()}, failed ${Number(summary.failedSources || 0)}, excluded ${Number(summary.excludedSources || 0)}.`,
      "success"
    );
    const lifecycleLabel = formatLifecycleSummary(report);
    if (lifecycleLabel) {
      appendFetcherLog(lifecycleLabel, "muted");
    }
    const runtimeLabel = formatFetcherRuntimeOptions(report);
    if (runtimeLabel) {
      appendFetcherLog(`Runtime options: ${runtimeLabel}.`, "muted");
    }

    const sources = Array.isArray(report?.sources) ? report.sources : [];
    if (!sources.length) {
      appendFetcherLog("No source entries found in report.", "warn");
      return;
    }

    sources.forEach(source => {
      const status = String(source?.status || "unknown").toLowerCase();
      const line = `${source?.name || "unknown"} [${status}] fetched ${Number(source?.fetchedCount || 0).toLocaleString()}, kept ${Number(source?.keptCount || 0).toLocaleString()}, duration ${Number(source?.durationMs || 0)}ms${source?.error ? `, note: ${source.error}` : ""}`;
      const level = status === "error" ? "error" : status === "excluded" ? "warn" : "info";
      appendFetcherLog(line, level);

      const details = Array.isArray(source?.details) ? source.details : [];
      details.forEach(detail => {
        const detailStatus = String(detail?.status || "unknown").toLowerCase();
        const detailLevel = detailStatus === "error" ? "error" : detailStatus === "excluded" ? "warn" : "muted";
        const detailLine =
          `  - ${detail?.name || detail?.studio || "source"} [${detailStatus}] ` +
          `fetched ${Number(detail?.fetchedCount || 0).toLocaleString()}, kept ${Number(detail?.keptCount || 0).toLocaleString()}` +
          `${detail?.error ? `, note: ${detail.error}` : ""}`;
        appendFetcherLog(detailLine, detailLevel);
      });
    });

    loadOpsHealthData().catch(() => {});
  } finally {
    setBusyFlag("fetcherReportLoad", false);
  }
}

async function fetchJobsFetchReportJson() {
  return fetchJobsFetchReportJsonFromData(JOBS_FETCH_REPORT_URL);
}

async function copyLatestFailureSummary() {
  const report = latestFetcherReportCache || await fetchJobsFetchReportJson();
  if (!report) {
    showToast("No fetch report available to copy.", "error");
    return;
  }
  latestFetcherReportCache = report;
  const sources = Array.isArray(report?.sources) ? report.sources : [];
  const failures = sources.filter(row => String(row?.status || "").toLowerCase() === "error");
  if (!failures.length) {
    showToast("No failed sources in latest report.", "info");
    return;
  }
  const summary = failures.map(row =>
    `${row?.name || "unknown"}: ${row?.error || "error"}`
  ).join("\n");
  if (navigator?.clipboard?.writeText) {
    try {
      await navigator.clipboard.writeText(summary);
      showToast("Failure summary copied.", "success");
      return;
    } catch {
      // Continue to fallback.
    }
  }
  appendFetcherLog(`Failure summary:\n${summary}`, "warn");
  showToast("Could not access clipboard. Summary logged.", "warn");
}

function startFetcherCompletionWatch() {
  stopFetcherCompletionWatch();
  setBusyFlag("fetcherWatch", true);
  fetcherLaunchAtMs = Date.now();
  fetcherCompletionPollDeadline = fetcherLaunchAtMs + FETCH_REPORT_POLL_TIMEOUT_MS;
  fetcherLiveProgressState = {
    summarySignature: "",
    runtimeSignature: "",
    sourceSignatures: new Map(),
    lastHeartbeatAtMs: 0
  };
  appendFetcherLog("Watching fetch report for completion to trigger jobs auto-refresh...");
  scheduleFetcherCompletionPoll(900);
}

function stopFetcherCompletionWatch() {
  if (fetcherCompletionPollTimer) {
    clearTimeout(fetcherCompletionPollTimer);
    fetcherCompletionPollTimer = null;
  }
  fetcherLiveProgressState = null;
  setBusyFlag("fetcherWatch", false);
}

function scheduleFetcherCompletionPoll(delayMs) {
  fetcherCompletionPollTimer = setTimeout(() => {
    pollFetcherCompletion().catch(err => {
      logAdminError("Fetcher completion poll failed", err);
      scheduleFetcherCompletionPoll(FETCH_REPORT_POLL_INTERVAL_MS);
    });
  }, delayMs);
}

async function pollFetcherCompletion() {
  const now = Date.now();
  if (now >= fetcherCompletionPollDeadline) {
    appendFetcherLog("Could not confirm completion from report within timeout window.", "warn");
    stopFetcherCompletionWatch();
    return;
  }

  const report = await fetchJobsFetchReportJson();
  const startedMs = parseReportTimestampMs(report?.startedAt);
  const isCurrentRunReport = startedMs >= (fetcherLaunchAtMs - 1000);
  if (isCurrentRunReport) {
    appendFetcherProgressFromReport(report, now);
  }
  const finishedMs = parseReportTimestampMs(report?.finishedAt);
  if (finishedMs >= (fetcherLaunchAtMs - 1000)) {
    const summary = report?.summary || {};
    appendFetcherLog(
      `Fetcher run completed: output ${Number(summary.outputCount || 0).toLocaleString()}, failed sources ${Number(summary.failedSources || 0)}.`,
      "success"
    );
    const lifecycleLabel = formatLifecycleSummary(report);
    if (lifecycleLabel) {
      appendFetcherLog(lifecycleLabel, "muted");
    }
    const runtimeLabel = formatFetcherRuntimeOptions(report);
    if (runtimeLabel) {
      appendFetcherLog(`Completed with runtime options: ${runtimeLabel}.`, "muted");
    }
    emitJobsAutoRefreshSignal(report);
    stopFetcherCompletionWatch();
    return;
  }

  scheduleFetcherCompletionPoll(FETCH_REPORT_POLL_INTERVAL_MS);
}

function appendFetcherProgressFromReport(report, nowMs) {
  const state = fetcherLiveProgressState;
  if (!state) return;
  const summary = report?.summary || {};
  const outputCount = Number(summary.outputCount || 0);
  const mergedCount = Number(summary.mergedCount || 0);
  const rawFetchedCount = Number(summary.rawFetchedCount || 0);
  const failedSources = Number(summary.failedSources || 0);
  const excludedSources = Number(summary.excludedSources || 0);
  const successfulSources = Number(summary.successfulSources || 0);
  const lifecycleActive = Number(summary.lifecycleActiveCount || 0);
  const lifecycleLikelyRemoved = Number(summary.lifecycleLikelyRemovedCount || 0);
  const lifecycleArchived = Number(summary.lifecycleArchivedCount || 0);

  const summarySignature = [
    outputCount,
    mergedCount,
    rawFetchedCount,
    failedSources,
    excludedSources,
    successfulSources,
    lifecycleActive,
    lifecycleLikelyRemoved,
    lifecycleArchived
  ].join("|");
  if (summarySignature !== state.summarySignature) {
    state.summarySignature = summarySignature;
    appendFetcherLog(
      `Progress: output ${outputCount.toLocaleString()}, merged ${mergedCount.toLocaleString()}, fetched ${rawFetchedCount.toLocaleString()}, ok ${successfulSources}, failed ${failedSources}, excluded ${excludedSources}, lifecycle active ${lifecycleActive.toLocaleString()}, likely removed ${lifecycleLikelyRemoved.toLocaleString()}, archived ${lifecycleArchived.toLocaleString()}.`,
      failedSources > 0 ? "warn" : "info"
    );
  }
  const runtimeLabel = formatFetcherRuntimeOptions(report);
  if (runtimeLabel && runtimeLabel !== state.runtimeSignature) {
    state.runtimeSignature = runtimeLabel;
    appendFetcherLog(`Progress runtime: ${runtimeLabel}.`, "muted");
  }

  const sources = Array.isArray(report?.sources) ? report.sources : [];
  sources.forEach(source => {
    const name = String(source?.name || "unknown");
    const status = String(source?.status || "unknown").toLowerCase();
    const signature = [
      status,
      Number(source?.fetchedCount || 0),
      Number(source?.keptCount || 0),
      Number(source?.durationMs || 0),
      String(source?.error || "")
    ].join("|");
    if (state.sourceSignatures.get(name) === signature) return;
    state.sourceSignatures.set(name, signature);
    const level = status === "error" ? "error" : status === "excluded" ? "warn" : "muted";
    appendFetcherLog(
      `Progress source: ${name} [${status}] fetched ${Number(source?.fetchedCount || 0).toLocaleString()}, kept ${Number(source?.keptCount || 0).toLocaleString()}, duration ${Number(source?.durationMs || 0)}ms${source?.error ? `, note: ${source.error}` : ""}`,
      level
    );
  });

  if ((nowMs - Number(state.lastHeartbeatAtMs || 0)) >= 20000) {
    state.lastHeartbeatAtMs = nowMs;
    appendFetcherLog("Fetcher still running. Waiting for more source updates...", "muted");
  }
}

function parseReportTimestampMs(value) {
  if (!value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function emitJobsAutoRefreshSignal(report) {
  const signal = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    createdAt: new Date().toISOString(),
    finishedAt: String(report?.finishedAt || ""),
    source: "admin_fetcher"
  };

  try {
    safeWriteJsonLocal(JOBS_AUTO_REFRESH_SIGNAL_KEY, signal);
    appendFetcherLog("Signaled jobs page to auto-refresh from unified feed.", "success");
  } catch {
    appendFetcherLog("Could not write auto-refresh signal to localStorage.", "warn");
  }
}

async function callBridge(request) {
  try {
    const data = await request();
    setBridgeStatusBadge("online", "Bridge Online");
    return data;
  } catch (error) {
    setBridgeStatusBadge("offline", "Bridge Offline");
    throw error;
  }
}

async function getBridge(path) {
  return callBridge(() => getBridgeFromData(ADMIN_BRIDGE_BASE, path));
}

async function postBridge(path, payload) {
  return callBridge(() => postBridgeFromData(ADMIN_BRIDGE_BASE, path, payload));
}

async function runDiscoveryTask() {
  if (!adminPin) {
    showToast("Unlock admin before running discovery.", "error");
    return;
  }
  if (isDiscoveryBusy()) {
    showToast("Discovery operation already in progress.", "info");
    return;
  }
  setBusyFlag("discoveryRun", true);
  setBusyFlag("liveDiscoveryRunning", true);
  discoveryLogRemoteOffset = 0;
  appendDiscoveryLog("Triggering source discovery task...");
  try {
    await postBridge("/tasks/run-discovery", {});
    appendDiscoveryLog("Source discovery task started.", "success");
    showToast("Source discovery started.", "success");
    startDiscoveryCompletionWatch();
    loadOpsHealthData().catch(() => {});
    scheduleOpsHealthPolling(250);
  } catch (err) {
    appendDiscoveryLog(`Could not trigger discovery task: ${getErrorMessage(err)}`, "error");
    showToast("Could not trigger source discovery task.", "error");
    setBusyFlag("liveDiscoveryRunning", false);
  } finally {
    setBusyFlag("discoveryRun", false);
  }
}

function startDiscoveryCompletionWatch() {
  stopDiscoveryCompletionWatch();
  setBusyFlag("discoveryWatch", true);
  discoveryLaunchAtMs = Date.now();
  discoveryCompletionPollDeadline = discoveryLaunchAtMs + DISCOVERY_REPORT_POLL_TIMEOUT_MS;
  discoveryLogRemoteOffset = 0;
  discoveryLiveProgressState = {
    summarySignature: "",
    candidateCount: 0,
    failureCount: 0,
    lastHeartbeatAtMs: 0
  };
  appendDiscoveryLog("Watching discovery report for live progress...");
  loadDiscoveryLogChunk({ reset: true }).catch(() => {});
  scheduleDiscoveryCompletionPoll(250);
}

function stopDiscoveryCompletionWatch() {
  if (discoveryCompletionPollTimer) {
    clearTimeout(discoveryCompletionPollTimer);
    discoveryCompletionPollTimer = null;
  }
  discoveryLiveProgressState = null;
  setBusyFlag("discoveryWatch", false);
}

function scheduleDiscoveryCompletionPoll(delayMs) {
  discoveryCompletionPollTimer = setTimeout(() => {
    pollDiscoveryCompletion().catch(err => {
      logAdminError("Discovery completion poll failed", err);
      scheduleDiscoveryCompletionPoll(DISCOVERY_REPORT_POLL_INTERVAL_MS);
    });
  }, delayMs);
}

async function pollDiscoveryCompletion() {
  const now = Date.now();
  if (now >= discoveryCompletionPollDeadline) {
    appendDiscoveryLog("Could not confirm discovery completion from report within timeout window.", "warn");
    stopDiscoveryCompletionWatch();
    return;
  }

  const [report] = await Promise.all([
    getBridge("/discovery/report"),
    loadDiscoveryLogChunk().catch(() => null)
  ]);
  const startedMs = parseReportTimestampMs(report?.startedAt);
  const isCurrentRunReport = startedMs >= (discoveryLaunchAtMs - 1000);
  if (isCurrentRunReport) {
    appendDiscoveryProgressFromReport(report, now);
  }
  const finishedMs = parseReportTimestampMs(report?.finishedAt);
  if (finishedMs >= (discoveryLaunchAtMs - 1000)) {
    const summary = report?.summary || {};
    const queuedCount = Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0);
    const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const failedCount = Number(summary.failedProbeCount || 0);
    appendDiscoveryLog(
      `Discovery run completed: found ${Number(summary.foundEndpointCount ?? 0)}, probed ${probedCount}, queued (new) ${queuedCount}, failed ${failedCount}.`,
      failedCount > 0 ? "warn" : "success"
    );
    await Promise.allSettled([loadDiscoveryData(), loadOpsHealthData()]);
    stopDiscoveryCompletionWatch();
    return;
  }

  scheduleDiscoveryCompletionPoll(DISCOVERY_REPORT_POLL_INTERVAL_MS);
}

function appendDiscoveryProgressFromReport(report, nowMs) {
  const state = discoveryLiveProgressState;
  if (!state) return;
  const summary = report?.summary || {};
  const foundCount = Number(summary.foundEndpointCount ?? 0);
  const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
  const queuedCount = Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0);
  const failedCount = Number(summary.failedProbeCount || 0);
  const skippedCount = Number(summary.skippedDuplicateCount || 0);
  const invalidCount = Number(summary.skippedInvalidCount || 0);

  const summarySignature = [
    foundCount,
    probedCount,
    queuedCount,
    failedCount,
    skippedCount,
    invalidCount
  ].join("|");
  if (summarySignature !== state.summarySignature) {
    state.summarySignature = summarySignature;
    appendDiscoveryLog(
      `Progress: found ${foundCount}, probed ${probedCount}, queued (new) ${queuedCount}, failed ${failedCount}, skipped dupes ${skippedCount}, skipped invalid ${invalidCount}.`,
      failedCount > 0 ? "warn" : "info"
    );
  }

  const candidates = Array.isArray(report?.candidates) ? report.candidates : [];
  if (candidates.length > state.candidateCount) {
    const newRows = candidates.slice(state.candidateCount, candidates.length).slice(-3);
    newRows.forEach(row => {
      appendDiscoveryLog(
        `Queued candidate: ${String(row?.name || "unknown")} [${String(row?.adapter || "unknown")}] jobs ${Number(row?.jobsFound || 0)}.`,
        "muted"
      );
    });
    state.candidateCount = candidates.length;
  } else {
    state.candidateCount = candidates.length;
  }

  const failures = Array.isArray(report?.failures) ? report.failures : [];
  if (failures.length > state.failureCount) {
    const newFailures = failures.slice(state.failureCount, failures.length).slice(-3);
    newFailures.forEach(item => {
      const stage = String(item?.stage || "probe");
      const name = String(item?.name || item?.domain || "unknown");
      appendDiscoveryLog(`Probe issue: ${name} [${stage}] ${String(item?.error || "unknown error")}`, "warn");
    });
    state.failureCount = failures.length;
  } else {
    state.failureCount = failures.length;
  }

  if ((nowMs - Number(state.lastHeartbeatAtMs || 0)) >= 20000) {
    state.lastHeartbeatAtMs = nowMs;
    appendDiscoveryLog("Discovery still running. Waiting for more probe updates...", "muted");
  }
}

async function addManualSource() {
  if (!adminPin) {
    showToast("Unlock admin before adding a source.", "error");
    return;
  }
  if (isDiscoveryBusy()) {
    showToast("Another discovery operation is running.", "info");
    return;
  }
  setBusyFlag("manualAdd", true);
  const url = String(adminManualSourceUrlEl?.value || "").trim();
  if (!url) {
    setManualSourceFeedback("invalid URL", "error");
    showToast("Enter a source URL.", "error");
    setBusyFlag("manualAdd", false);
    return;
  }
  try {
    /** @type {ManualSourceAddResult} */
    const addResult = await postBridge("/sources/manual", { url });
    const status = String(addResult?.status || "").toLowerCase();

    if (status === "invalid") {
      setManualSourceFeedback("invalid URL", "error");
      appendDiscoveryLog(`Manual source invalid: ${String(addResult?.message || "invalid URL")}`, "error");
      showToast(String(addResult?.message || "Invalid source URL."), "error");
      return;
    }

    if (status === "duplicate") {
      setManualSourceFeedback("duplicate skipped", "warn");
      appendDiscoveryLog("Manual source duplicate skipped.", "warn");
      showToast("Source already exists. Skipped duplicate.", "info");
      return;
    }

    if (status !== "added") {
      setManualSourceFeedback("check failed", "error");
      showToast("Could not add manual source.", "error");
      return;
    }

    if (adminManualSourceUrlEl) adminManualSourceUrlEl.value = "";
    setManualSourceFeedback("added", "success");
    if (String(addResult?.source?.adapter || "").toLowerCase() === "static") {
      appendDiscoveryLog("No known provider detected, using generic website scraping.", "warn");
    }
    appendDiscoveryLog("Manual source added.", "success");

    const sourceId = String(addResult?.sourceId || "");
    if (sourceId) {
      setBusyFlag("manualCheck", true);
      setManualSourceFeedback("check started", "muted");
      /** @type {SourceCheckTriggerResult} */
      const checkResult = await postBridge("/discovery/check-source", { sourceId });
      if (!checkResult?.started || checkResult?.ok === false) {
        setManualSourceFeedback("check failed", "error");
        appendDiscoveryLog(`Manual source check failed: ${String(checkResult?.error || "unknown error")}`, "error");
        if (Array.isArray(checkResult?.suggestedUrls) && checkResult.suggestedUrls.length) {
          appendDiscoveryLog(`Try alternate URL(s): ${checkResult.suggestedUrls.join(" | ")}`, "warn");
        }
        if (checkResult?.browserFallbackAttempted) {
          appendDiscoveryLog("Browser fallback was attempted during this check.", "muted");
        }
        showToast(formatManualCheckFailureMessage(checkResult), "error");
      } else {
        appendDiscoveryLog(
          `Manual source check completed (jobs found: ${Number(checkResult?.jobsFound || 0)}${checkResult?.weakSignal ? ", weak signal" : ""}).`,
          "success"
        );
        if (checkResult?.browserFallbackUsed) {
          appendDiscoveryLog("Generic browser fallback was used to bypass a blocked page.", "warn");
        }
        showToast("Manual source added and checked.", "success");
      }
    }

    await loadDiscoveryData();
    await loadOpsHealthData();
  } finally {
    setBusyFlag("manualCheck", false);
    setBusyFlag("manualAdd", false);
  }
}

function renderSourcesTable(container, rows, mode = "pending") {
  if (!container) return;
  container.innerHTML = renderSourcesTableHtml(rows, mode, formatSourceJobsFound, deriveSourceStatus);
}

function formatSourceJobsFound(row) {
  const value = getSourceJobsFoundCount(row);
  if (!Number.isFinite(value) || value < 0) {
    return "N/A";
  }
  return value.toLocaleString();
}

function getSourceJobsFoundCount(row) {
  return getSourceJobsFoundCountFromDomain(row);
}

function deriveSourceStatus(row) {
  return deriveSourceStatusFromDomain(row);
}

function normalizeOpsRuns(runs, nowMs = Date.now()) {
  return normalizeOpsRunsFromDomain(runs, nowMs);
}

function getOpsPollIntervalMs(hasLiveRuns) {
  return getOpsPollIntervalMsFromDomain(hasLiveRuns, OPS_POLL_IDLE_INTERVAL_MS, OPS_POLL_LIVE_INTERVAL_MS);
}

function getCheckedSourceIds(selector) {
  return Array.from(document.querySelectorAll(selector))
    .filter(el => el instanceof HTMLInputElement && el.checked)
    .map(el => String(el.dataset.sourceId || ""))
    .filter(Boolean);
}

function selectedIds(selector) {
  return getCheckedSourceIds(selector);
}

function getCheckedSources(selector) {
  return Array.from(document.querySelectorAll(selector))
    .filter(el => el instanceof HTMLInputElement && el.checked)
    .map(el => ({
      id: String(el.dataset.sourceId || "").trim(),
      url: String(el.dataset.sourceUrl || "").trim()
    }))
    .filter(item => item.id || item.url);
}

function selectedSourcesAcrossDiscoveryBuckets() {
  const out = [];
  const seen = new Set();
  const rows = [
    ...getCheckedSources(".pending-source-checkbox"),
    ...getCheckedSources(".active-source-checkbox"),
    ...getCheckedSources(".rejected-source-checkbox")
  ];
  rows.forEach(item => {
    const key = `${item.id}|${item.url}`;
    if (!key || seen.has(key)) return;
    seen.add(key);
    out.push(item);
  });
  return out;
}

async function approveSelectedSources() {
  if (!adminPin) return;
  if (isDiscoveryBusy()) {
    showToast("Another discovery operation is running.", "info");
    return;
  }
  const ids = selectedIds(".pending-source-checkbox");
  if (!ids.length) {
    showToast("Select pending sources to approve.", "info");
    return;
  }
  setBusyFlag("discoveryWrite", true);
  try {
    const result = await postBridge("/registry/approve", { ids });
    appendDiscoveryLog(`Approved ${Number(result?.approved || 0)} source(s).`, "success");
    showToast("Sources approved.", "success");
    await loadDiscoveryData();
    await loadOpsHealthData();
  } catch (err) {
    appendDiscoveryLog(`Approve failed: ${getErrorMessage(err)}`, "error");
    showToast("Could not approve sources.", "error");
  } finally {
    setBusyFlag("discoveryWrite", false);
  }
}

async function rejectSelectedSources() {
  if (!adminPin) return;
  if (isDiscoveryBusy()) {
    showToast("Another discovery operation is running.", "info");
    return;
  }
  const ids = selectedIds(".pending-source-checkbox");
  if (!ids.length) {
    showToast("Select pending sources to reject.", "info");
    return;
  }
  setBusyFlag("discoveryWrite", true);
  try {
    const result = await postBridge("/registry/reject", { ids });
    appendDiscoveryLog(`Rejected ${Number(result?.rejected || 0)} source(s).`, "warn");
    showToast("Sources rejected.", "success");
    await loadDiscoveryData();
    await loadOpsHealthData();
  } catch (err) {
    appendDiscoveryLog(`Reject failed: ${getErrorMessage(err)}`, "error");
    showToast("Could not reject sources.", "error");
  } finally {
    setBusyFlag("discoveryWrite", false);
  }
}

async function restoreRejectedSources() {
  if (!adminPin) return;
  if (isDiscoveryBusy()) {
    showToast("Another discovery operation is running.", "info");
    return;
  }
  const ids = selectedIds(".rejected-source-checkbox");
  if (!ids.length) {
    showToast("Select rejected sources to restore.", "info");
    return;
  }
  setBusyFlag("discoveryWrite", true);
  try {
    const result = await postBridge("/registry/restore-rejected", { ids });
    appendDiscoveryLog(`Restored ${Number(result?.restored || 0)} rejected source(s) to pending.`, "success");
    showToast("Rejected sources restored to pending.", "success");
    await loadDiscoveryData();
    await loadOpsHealthData();
  } catch (err) {
    appendDiscoveryLog(`Restore failed: ${getErrorMessage(err)}`, "error");
    showToast("Could not restore rejected sources.", "error");
  } finally {
    setBusyFlag("discoveryWrite", false);
  }
}

async function deleteSelectedSources() {
  if (!adminPin) return;
  if (isDiscoveryBusy()) {
    showToast("Another discovery operation is running.", "info");
    return;
  }
  const sources = selectedSourcesAcrossDiscoveryBuckets();
  const ids = Array.from(new Set(sources.map(item => item.id).filter(Boolean)));
  const urls = Array.from(new Set(sources.map(item => item.url).filter(Boolean)));
  if (!ids.length && !urls.length) {
    showToast("Select sources to delete.", "info");
    return;
  }
  const confirmed = window.confirm(`Delete ${sources.length} selected source(s) from registry? This cannot be undone.`);
  if (!confirmed) return;
  setBusyFlag("discoveryWrite", true);
  try {
    const result = await postBridge("/registry/delete", { ids, urls });
    appendDiscoveryLog(`Deleted ${Number(result?.deleted || 0)} source(s).`, "warn");
    showToast("Selected sources deleted.", "success");
    await loadDiscoveryData();
    await loadOpsHealthData();
  } catch (err) {
    appendDiscoveryLog(`Delete failed: ${getErrorMessage(err)}`, "error");
    showToast("Could not delete selected sources.", "error");
  } finally {
    setBusyFlag("discoveryWrite", false);
  }
}

async function loadDiscoveryData() {
  if (!adminPin) return;
  if (adminBusyState.discoveryLoad) return;
  setBusyFlag("discoveryLoad", true);
  appendDiscoveryLog("Loading source discovery report and registries...");
  try {
    const [report, pending, active, rejected] = await Promise.all([
      getBridge("/discovery/report"),
      getBridge("/registry/pending"),
      getBridge("/registry/active"),
      getBridge("/registry/rejected")
    ]);
    const latestFetchReport = latestFetcherReportCache || await fetchJobsFetchReportJson();
    latestFetcherReportCache = latestFetchReport || latestFetcherReportCache;
    const summary = report?.summary || {};
    const foundCount = Number(summary.foundEndpointCount ?? summary.probedCount ?? 0);
    const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const queuedCount = Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0);
    const skippedCount = Number(summary.skippedDuplicateCount || 0);
    const failedCount = Number(summary.failedProbeCount || 0);
    const pendingRows = mergeSourceStatusFromReport(Array.isArray(pending?.sources) ? pending.sources : [], latestFetchReport, "pending");
    const activeRows = mergeSourceStatusFromReport(Array.isArray(active?.sources) ? active.sources : [], latestFetchReport, "active");
    const rejectedRows = mergeSourceStatusFromReport(Array.isArray(rejected?.sources) ? rejected.sources : [], latestFetchReport, "rejected");
    const hiddenZeroJobsCount = pendingRows.filter(row => getSourceJobsFoundCount(row) === 0).length;
    const showZeroJobs = readShowZeroJobsPreference();
    const visiblePendingRowsPreFilter = showZeroJobs
      ? pendingRows
      : pendingRows.filter(row => getSourceJobsFoundCount(row) !== 0);
    const visiblePendingRows = applySourceFilter(visiblePendingRowsPreFilter);
    const visibleActiveRows = applySourceFilter(activeRows);
    const visibleRejectedRows = applySourceFilter(rejectedRows);

    if (adminDiscoverySummaryEl) {
      adminDiscoverySummaryEl.textContent =
        `Found ${foundCount} | Probed ${probedCount} | Queued (new) ${queuedCount} | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending ${Number(pending?.summary?.pendingCount || 0)} | Active ${Number(active?.summary?.activeCount || 0)} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
    }
    renderSourcesTable(adminPendingSourcesEl, visiblePendingRows, "pending");
    renderSourcesTable(adminActiveSourcesEl, visibleActiveRows, "active");
    renderSourcesTable(adminRejectedSourcesEl, visibleRejectedRows, "rejected");
    appendDiscoveryLog(
      `Discovery summary: found ${foundCount}, probed ${probedCount}, queued (new) ${queuedCount}, failed ${failedCount}, skipped duplicates ${skippedCount}.`,
      "info"
    );
    const topFailures = Array.isArray(report?.topFailures) ? report.topFailures : [];
    if (topFailures.length) {
      const line = topFailures
        .slice(0, 3)
        .map(item => `${String(item?.key || "unknown")} (${Number(item?.count || 0)})`)
        .join(", ");
      appendDiscoveryLog(`Top failures: ${line}`, "warn");
    }
    appendDiscoveryLog("Source discovery data loaded.", "success");
    adminDispatch.dispatch({ type: ADMIN_ACTIONS.DISCOVERY_REFRESHED, payload: { at: new Date().toISOString() } });
  } catch (err) {
    appendDiscoveryLog(`Could not load source discovery data: ${getErrorMessage(err)}`, "error");
    if (adminDiscoverySummaryEl) {
      adminDiscoverySummaryEl.textContent = "Source discovery bridge unavailable. Start `Run admin bridge` task.";
    }
  } finally {
    setBusyFlag("discoveryLoad", false);
  }
}

function readShowZeroJobsPreference() {
  return readShowZeroJobs(ADMIN_SHOW_ZERO_JOBS_KEY);
}

function writeShowZeroJobsPreference(enabled) {
  writeShowZeroJobs(ADMIN_SHOW_ZERO_JOBS_KEY, Boolean(enabled));
}

function setBridgeStatusBadge(state, label) {
  if (!adminBridgeStatusBadgeEl) return;
  const normalized = String(state || "checking").toLowerCase();
  adminBridgeStatusBadgeEl.classList.remove("online", "offline", "checking");
  adminBridgeStatusBadgeEl.classList.add(
    normalized === "online" ? "online" : normalized === "offline" ? "offline" : "checking"
  );
  adminBridgeStatusBadgeEl.textContent = label || "Bridge Checking";
  adminBridgeStatusBadgeEl.classList.remove("refresh-pulse");
  void adminBridgeStatusBadgeEl.offsetWidth;
  adminBridgeStatusBadgeEl.classList.add("refresh-pulse");
}

function startBridgeStatusWatch() {
  stopBridgeStatusWatch();
  pollBridgeStatus({ forceChecking: true }).catch(() => {});
  bridgeStatusPollTimer = setInterval(() => {
    pollBridgeStatus().catch(() => {});
  }, BRIDGE_STATUS_POLL_INTERVAL_MS);
}

function stopBridgeStatusWatch() {
  if (!bridgeStatusPollTimer) return;
  clearInterval(bridgeStatusPollTimer);
  bridgeStatusPollTimer = null;
}

async function pollBridgeStatus(options = {}) {
  if (!adminPin) {
    setBridgeStatusBadge("checking", "Bridge Locked");
    return;
  }
  if (options.forceChecking) {
    setBridgeStatusBadge("checking", "Bridge Checking");
  }
  try {
    const summaryPayload = await getBridge("/registry/summary");
    const summary = summaryPayload?.summary || {};
    const activeCount = Number(summary?.activeCount || 0);
    const pendingCount = Number(summary?.pendingCount || 0);
    setBridgeStatusBadge("online", `Bridge Online (${activeCount} active, ${pendingCount} pending)`);
  } catch {
    setBridgeStatusBadge("offline", "Bridge Offline");
  }
}

function toLocalTime(value) {
  try {
    return value.toLocaleTimeString([], {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false
    });
  } catch {
    return "--:--:--";
  }
}

export { bootAdminPage as boot };




