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
  getSourceJobsFoundCount as getSourceJobsFoundCountFromDomain
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
let adminDiscoverySummaryEl;
let adminPendingSourcesEl;
let adminActiveSourcesEl;
let adminRejectedSourcesEl;
let adminRestoreRejectedBtnEl;
let adminDiscoveryLogEl;
let adminBridgeStatusBadgeEl;
let adminShowZeroJobsToggleEl;
let adminRefreshOpsBtnEl;
let adminOpsAlertsEl;
let adminOpsKpisEl;
let adminOpsScheduleEl;
let adminOpsTrendsEl;
let adminOpsHistoryEl;
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
const JOBS_LAST_URL_KEY = adminConfig.JOBS_LAST_URL_KEY || "baluffo_jobs_last_url";
const JOBS_FETCHER_COMMAND = adminConfig.JOBS_FETCHER_COMMAND || "python scripts/jobs_fetcher.py";
const JOBS_FETCHER_TASK_LABEL = adminConfig.JOBS_FETCHER_TASK_LABEL || "Run jobs fetcher";
const JOBS_FETCH_REPORT_URL = adminConfig.JOBS_FETCH_REPORT_URL || "data/jobs-fetch-report.json";
const JOBS_AUTO_REFRESH_SIGNAL_KEY = adminConfig.JOBS_AUTO_REFRESH_SIGNAL_KEY || "baluffo_jobs_auto_refresh_signal";
const FETCH_REPORT_POLL_INTERVAL_MS = Number(adminConfig.FETCH_REPORT_POLL_INTERVAL_MS || 5000);
const FETCH_REPORT_POLL_TIMEOUT_MS = Number(adminConfig.FETCH_REPORT_POLL_TIMEOUT_MS || (10 * 60 * 1000));
const ADMIN_BRIDGE_BASE = adminConfig.ADMIN_BRIDGE_BASE || "http://127.0.0.1:8877";
const BRIDGE_STATUS_POLL_INTERVAL_MS = Number(adminConfig.BRIDGE_STATUS_POLL_INTERVAL_MS || 10000);
const ADMIN_SHOW_ZERO_JOBS_KEY = "baluffo_admin_show_zero_jobs_sources";
const ADMIN_SOURCE_FILTER_KEY = "baluffo_admin_source_filter";
const UNKNOWN_ERROR_TEXT = "unknown error";

let fetcherCompletionPollTimer = null;
let fetcherCompletionPollDeadline = 0;
let fetcherLaunchAtMs = 0;
let bridgeStatusPollTimer = null;
let latestFetcherReportCache = null;
let latestOpsHealthCache = null;
const adminPageState = {
  activeSourceFilter: readSourceFilterPreference(),
  selectedSourceIds: new Set()
};
const adminDispatch = createAdminDispatcher();
let activeSourceFilter = adminPageState.activeSourceFilter;

function getErrorMessage(err) {
  return getErrorMessageFromDomain(err, UNKNOWN_ERROR_TEXT);
}

function logAdminError(context, err) {
  console.error(`[admin] ${context}:`, err);
}

function bootAdminPage() {
  cacheDom();
  bindEvents();
  initAdminPage();
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
  adminDiscoverySummaryEl = document.getElementById("admin-discovery-summary");
  adminPendingSourcesEl = document.getElementById("admin-pending-sources");
  adminActiveSourcesEl = document.getElementById("admin-active-sources");
  adminRejectedSourcesEl = document.getElementById("admin-rejected-sources");
  adminRestoreRejectedBtnEl = document.getElementById("admin-restore-rejected-btn");
  adminDiscoveryLogEl = document.getElementById("admin-discovery-log");
  adminBridgeStatusBadgeEl = document.getElementById("admin-bridge-status-badge");
  adminShowZeroJobsToggleEl = document.getElementById("admin-show-zero-jobs-toggle");
  adminRefreshOpsBtnEl = document.getElementById("admin-refresh-ops-btn");
  adminOpsAlertsEl = document.getElementById("admin-ops-alerts");
  adminOpsKpisEl = document.getElementById("admin-ops-kpis");
  adminOpsScheduleEl = document.getElementById("admin-ops-schedule");
  adminOpsTrendsEl = document.getElementById("admin-ops-trends");
  adminOpsHistoryEl = document.getElementById("admin-ops-history");
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
  bindAsyncClick(adminRunFetcherBtnEl, triggerJobsFetcherTask);
  bindAsyncClick(adminRefreshReportBtnEl, loadLatestFetcherReport);
  bindUi(adminClearLogBtnEl, "click", () => setFetcherLogPlaceholder("Output log cleared."));
  bindAsyncClick(adminRetryFailedBtnEl, async () => {
    appendFetcherLog("Retry failed sources requested (v1 runs full fetcher).", "warn");
    await triggerJobsFetcherTask();
  });
  bindAsyncClick(adminCopyFailuresBtnEl, copyLatestFailureSummary);
  bindUi(adminLockBtnEl, "click", lockAdmin);
  bindAsyncClick(adminRunDiscoveryBtnEl, runDiscoveryTask);
  bindAsyncClick(adminLoadDiscoveryBtnEl, loadDiscoveryData);
  bindAsyncClick(adminApproveSourcesBtnEl, approveSelectedSources);
  bindAsyncClick(adminRejectSourcesBtnEl, rejectSelectedSources);
  bindAsyncClick(adminRestoreRejectedBtnEl, restoreRejectedSources);

  if (adminShowZeroJobsToggleEl) {
    adminShowZeroJobsToggleEl.checked = readShowZeroJobsPreference();
    adminShowZeroJobsToggleEl.addEventListener("change", () => {
      writeShowZeroJobsPreference(Boolean(adminShowZeroJobsToggleEl.checked));
      loadDiscoveryData().catch(() => {});
    });
  }

  bindAsyncClick(adminRefreshOpsBtnEl, loadOpsHealthData);

  adminSourceFilterBtnEls.forEach(btn => {
    btn.addEventListener("click", () => {
      const next = String(btn.dataset.sourceFilter || "all").toLowerCase();
      setSourceFilter(next);
      loadDiscoveryData().catch(() => {});
    });
  });
}

function initAdminPage() {
  setSourceFilter(activeSourceFilter);
  setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
  setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
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
  adminDispatch.dispatch({ type: ADMIN_ACTIONS.UNLOCKED });
  setSourceStatus("Admin access granted.");
  if (adminPinGateEl) adminPinGateEl.classList.add("hidden");
  if (adminContentEl) adminContentEl.classList.remove("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.remove("hidden");
  if (adminPinInputEl) adminPinInputEl.value = "";
  setFetcherLogPlaceholder("Loading latest jobs fetch report...");
  setDiscoveryLogPlaceholder("Loading source discovery data...");
  setOpsPlaceholders("Loading operations health...");
  startBridgeStatusWatch();
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
}

function lockAdmin() {
  adminPin = "";
  adminDispatch.dispatch({ type: ADMIN_ACTIONS.LOCKED });
  if (adminPinGateEl) adminPinGateEl.classList.remove("hidden");
  if (adminContentEl) adminContentEl.classList.add("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.add("hidden");
  renderUsersEmpty("");
  if (adminTotalsEl) adminTotalsEl.innerHTML = "";
  stopBridgeStatusWatch();
  setBridgeStatusBadge("checking", "Bridge Locked");
  setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
  setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
  setOpsPlaceholders();
  setSourceStatus("Enter admin PIN to access user overview.");
}

async function triggerJobsFetcherTask() {
  if (!adminPin) {
    showToast("Unlock admin before running fetcher.", "error");
    return;
  }
  try {
    const bridge = await postBridge("/tasks/run-fetcher", {});
    if (bridge && bridge.started) {
      appendFetcherLog("Triggered fetcher via local admin bridge.");
      setSourceStatus("Triggered local fetcher task via admin bridge.");
      showToast("Fetcher started via admin bridge.", "success");
      loadOpsHealthData().catch(() => {});
      loadLatestFetcherReport({ silent: true }).catch(() => {});
      startFetcherCompletionWatch();
      return;
    }
  } catch {
    appendFetcherLog("Admin bridge unavailable, falling back to VS Code task launch.", "warn");
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
    appendFetcherLog(`Triggered VS Code task URI (primary): ${JOBS_FETCHER_TASK_LABEL}`);
    setSourceStatus("Triggered VS Code task to run jobs fetcher. Check VS Code terminal for progress.");
    window.setTimeout(() => {
      launchVsCodeUri(taskUris[1]);
      appendFetcherLog("Triggered compatibility URI fallback for VS Code task launch.");
    }, 180);
    appendFetcherLog("If VS Code did not open, run the fallback command shown below.", "warn");
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
        appendFetcherLog(`Copied fallback command: ${JOBS_FETCHER_COMMAND}`);
      })
      .catch(() => {
        appendFetcherLog(`Fallback command: ${JOBS_FETCHER_COMMAND}`, "warn");
      });
  } else {
    appendFetcherLog(`Fallback command: ${JOBS_FETCHER_COMMAND}`, "warn");
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
  appendDiscoveryLog(message, "muted");
}

function setOpsPlaceholders(message = "Unlock admin to view operations health.") {
  if (adminOpsAlertsEl) {
    adminOpsAlertsEl.innerHTML = `<div class="muted">${escapeHtml(message)}</div>`;
  }
  if (adminOpsKpisEl) {
    adminOpsKpisEl.innerHTML = "";
  }
  if (adminOpsScheduleEl) {
    adminOpsScheduleEl.innerHTML = "";
  }
  if (adminOpsTrendsEl) {
    adminOpsTrendsEl.textContent = message;
  }
  if (adminOpsHistoryEl) {
    adminOpsHistoryEl.innerHTML = `<div class="no-results">${escapeHtml(message)}</div>`;
  }
}

async function loadOpsHealthData() {
  if (!adminPin) return;
  if (adminOpsTrendsEl) adminOpsTrendsEl.textContent = "Loading operations health...";
  try {
    const [health, historyPayload] = await Promise.all([
      getBridge("/ops/health"),
      getBridge("/ops/history?limit=30")
    ]);
    latestOpsHealthCache = health || null;
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
    renderAdminOpsHistory(adminOpsHistoryEl, historyPayload?.runs || []);
    renderAdminOpsTrends(adminOpsTrendsEl, historyPayload?.runs || []);
    adminDispatch.dispatch({ type: ADMIN_ACTIONS.OPS_REFRESHED, payload: { at: new Date().toISOString() } });
  } catch (err) {
    setOpsPlaceholders(`Ops health unavailable: ${getErrorMessage(err)}`);
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
  fetcherLaunchAtMs = Date.now();
  fetcherCompletionPollDeadline = fetcherLaunchAtMs + FETCH_REPORT_POLL_TIMEOUT_MS;
  appendFetcherLog("Watching fetch report for completion to trigger jobs auto-refresh...");
  scheduleFetcherCompletionPoll(900);
}

function stopFetcherCompletionWatch() {
  if (!fetcherCompletionPollTimer) return;
  clearTimeout(fetcherCompletionPollTimer);
  fetcherCompletionPollTimer = null;
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
  const finishedMs = parseReportTimestampMs(report?.finishedAt);
  if (finishedMs >= (fetcherLaunchAtMs - 1000)) {
    const summary = report?.summary || {};
    appendFetcherLog(
      `Fetcher run completed: output ${Number(summary.outputCount || 0).toLocaleString()}, failed sources ${Number(summary.failedSources || 0)}.`,
      "success"
    );
    emitJobsAutoRefreshSignal(report);
    stopFetcherCompletionWatch();
    return;
  }

  scheduleFetcherCompletionPoll(FETCH_REPORT_POLL_INTERVAL_MS);
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

async function getBridge(path) {
  try {
    const data = await getBridgeFromData(ADMIN_BRIDGE_BASE, path);
    setBridgeStatusBadge("online", "Bridge Online");
    return data;
  } catch (error) {
    setBridgeStatusBadge("offline", "Bridge Offline");
    throw error;
  }
}

async function postBridge(path, payload) {
  try {
    const data = await postBridgeFromData(ADMIN_BRIDGE_BASE, path, payload);
    setBridgeStatusBadge("online", "Bridge Online");
    return data;
  } catch (error) {
    setBridgeStatusBadge("offline", "Bridge Offline");
    throw error;
  }
}

async function runDiscoveryTask() {
  if (!adminPin) {
    showToast("Unlock admin before running discovery.", "error");
    return;
  }
  appendDiscoveryLog("Triggering source discovery task...");
  try {
    await postBridge("/tasks/run-discovery-full", {});
    appendDiscoveryLog("Source discovery task started.", "success");
    showToast("Source discovery started.", "success");
    loadOpsHealthData().catch(() => {});
    setTimeout(() => {
      loadDiscoveryData().catch(() => {});
      loadOpsHealthData().catch(() => {});
    }, 2500);
  } catch (err) {
    appendDiscoveryLog(`Could not trigger discovery task: ${getErrorMessage(err)}`, "error");
    showToast("Could not trigger source discovery task.", "error");
  }
}

function renderSourcesTable(container, rows, mode = "pending") {
  if (!container) return;
  container.innerHTML = renderSourcesTableHtml(rows, mode, formatSourceJobsFound);
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

function getCheckedSourceIds(selector) {
  return Array.from(document.querySelectorAll(selector))
    .filter(el => el instanceof HTMLInputElement && el.checked)
    .map(el => String(el.dataset.sourceId || ""))
    .filter(Boolean);
}

function selectedIds(selector) {
  return getCheckedSourceIds(selector);
}

async function approveSelectedSources() {
  if (!adminPin) return;
  const ids = selectedIds(".pending-source-checkbox");
  if (!ids.length) {
    showToast("Select pending sources to approve.", "info");
    return;
  }
  try {
    const result = await postBridge("/registry/approve", { ids });
    appendDiscoveryLog(`Approved ${Number(result?.approved || 0)} source(s).`, "success");
    showToast("Sources approved.", "success");
    await loadDiscoveryData();
    await loadOpsHealthData();
  } catch (err) {
    appendDiscoveryLog(`Approve failed: ${getErrorMessage(err)}`, "error");
    showToast("Could not approve sources.", "error");
  }
}

async function rejectSelectedSources() {
  if (!adminPin) return;
  const ids = selectedIds(".pending-source-checkbox");
  if (!ids.length) {
    showToast("Select pending sources to reject.", "info");
    return;
  }
  try {
    const result = await postBridge("/registry/reject", { ids });
    appendDiscoveryLog(`Rejected ${Number(result?.rejected || 0)} source(s).`, "warn");
    showToast("Sources rejected.", "success");
    await loadDiscoveryData();
    await loadOpsHealthData();
  } catch (err) {
    appendDiscoveryLog(`Reject failed: ${getErrorMessage(err)}`, "error");
    showToast("Could not reject sources.", "error");
  }
}

async function restoreRejectedSources() {
  if (!adminPin) return;
  const ids = selectedIds(".rejected-source-checkbox");
  if (!ids.length) {
    showToast("Select rejected sources to restore.", "info");
    return;
  }
  try {
    const result = await postBridge("/registry/restore-rejected", { ids });
    appendDiscoveryLog(`Restored ${Number(result?.restored || 0)} rejected source(s) to pending.`, "success");
    showToast("Rejected sources restored to pending.", "success");
    await loadDiscoveryData();
    await loadOpsHealthData();
  } catch (err) {
    appendDiscoveryLog(`Restore failed: ${getErrorMessage(err)}`, "error");
    showToast("Could not restore rejected sources.", "error");
  }
}

async function loadDiscoveryData() {
  if (!adminPin) return;
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
        `Found ${foundCount} | Probed ${probedCount} | Queued ${queuedCount} | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending ${Number(pending?.summary?.pendingCount || 0)} | Active ${Number(active?.summary?.activeCount || 0)} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
    }
    renderSourcesTable(adminPendingSourcesEl, visiblePendingRows, "pending");
    renderSourcesTable(adminActiveSourcesEl, visibleActiveRows, "active");
    renderSourcesTable(adminRejectedSourcesEl, visibleRejectedRows, "rejected");
    appendDiscoveryLog(
      `Discovery summary: found ${foundCount}, probed ${probedCount}, queued ${queuedCount}, failed ${failedCount}, skipped duplicates ${skippedCount}.`,
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




