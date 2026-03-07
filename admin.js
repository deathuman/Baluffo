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

let adminPin = "";
const JOBS_LAST_URL_KEY = "baluffo_jobs_last_url";
const JOBS_FETCHER_COMMAND = "python scripts/jobs_fetcher.py";
const JOBS_FETCHER_TASK_LABEL = "Run jobs fetcher";
const JOBS_FETCH_REPORT_URL = "data/jobs-fetch-report.json";
const JOBS_AUTO_REFRESH_SIGNAL_KEY = "baluffo_jobs_auto_refresh_signal";
const FETCH_REPORT_POLL_INTERVAL_MS = 5000;
const FETCH_REPORT_POLL_TIMEOUT_MS = 10 * 60 * 1000;
const ADMIN_BRIDGE_BASE = "http://127.0.0.1:8877";
const BRIDGE_STATUS_POLL_INTERVAL_MS = 10000;

let fetcherCompletionPollTimer = null;
let fetcherCompletionPollDeadline = 0;
let fetcherLaunchAtMs = 0;
let bridgeStatusPollTimer = null;

function bootAdminPage() {
  cacheDom();
  bindEvents();
  initAdminPage();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", bootAdminPage);
} else {
  bootAdminPage();
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
}

function bindEvents() {
  if (adminJobsBtnEl) {
    adminJobsBtnEl.addEventListener("click", () => {
      const target = getLastJobsUrl();
      window.location.href = target;
    });
  }

  if (adminSavedBtnEl) {
    adminSavedBtnEl.addEventListener("click", () => {
      window.location.href = "saved.html";
    });
  }

  if (adminUnlockBtnEl) {
    adminUnlockBtnEl.addEventListener("click", () => {
      unlockAdmin();
    });
  }

  if (adminPinInputEl) {
    adminPinInputEl.addEventListener("keydown", event => {
      if (event.key === "Enter") {
        event.preventDefault();
        unlockAdmin();
      }
    });
  }

  if (adminRefreshBtnEl) {
    adminRefreshBtnEl.addEventListener("click", async () => {
      await refreshOverview();
    });
  }

  if (adminRunFetcherBtnEl) {
    adminRunFetcherBtnEl.addEventListener("click", async () => {
      await triggerJobsFetcherTask();
    });
  }

  if (adminRefreshReportBtnEl) {
    adminRefreshReportBtnEl.addEventListener("click", async () => {
      await loadLatestFetcherReport();
    });
  }

  if (adminClearLogBtnEl) {
    adminClearLogBtnEl.addEventListener("click", () => {
      setFetcherLogPlaceholder("Output log cleared.");
    });
  }

  if (adminLockBtnEl) {
    adminLockBtnEl.addEventListener("click", () => {
      lockAdmin();
    });
  }

  if (adminRunDiscoveryBtnEl) {
    adminRunDiscoveryBtnEl.addEventListener("click", async () => {
      await runDiscoveryTask();
    });
  }

  if (adminLoadDiscoveryBtnEl) {
    adminLoadDiscoveryBtnEl.addEventListener("click", async () => {
      await loadDiscoveryData();
    });
  }

  if (adminApproveSourcesBtnEl) {
    adminApproveSourcesBtnEl.addEventListener("click", async () => {
      await approveSelectedSources();
    });
  }

  if (adminRejectSourcesBtnEl) {
    adminRejectSourcesBtnEl.addEventListener("click", async () => {
      await rejectSelectedSources();
    });
  }

  if (adminRestoreRejectedBtnEl) {
    adminRestoreRejectedBtnEl.addEventListener("click", async () => {
      await restoreRejectedSources();
    });
  }
}

function initAdminPage() {
  const api = window.JobAppLocalData;
  setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
  setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
  setBridgeStatusBadge("checking", "Bridge Checking");
  if (!api || !api.isReady()) {
    setSourceStatus("Local storage provider unavailable.");
    if (adminPinGateEl) adminPinGateEl.classList.add("hidden");
    renderUsersEmpty("Admin view is unavailable in this browser.");
    return;
  }
}

function getLastJobsUrl() {
  try {
    const url = sessionStorage.getItem(JOBS_LAST_URL_KEY);
    if (!url) return "jobs.html";
    if (!url.startsWith("/") && !url.startsWith("jobs.html")) return "jobs.html";
    return url;
  } catch {
    return "jobs.html";
  }
}

function setSourceStatus(text) {
  if (!adminSourceStatusEl) return;
  adminSourceStatusEl.textContent = text;
}

function unlockAdmin() {
  const api = window.JobAppLocalData;
  const nextPin = String(adminPinInputEl?.value || "").trim();
  if (!nextPin) {
    showToast("Enter admin PIN.", "error");
    return;
  }
  if (!api || !api.verifyAdminPin || !api.verifyAdminPin(nextPin)) {
    showToast("Invalid admin PIN.", "error");
    setSourceStatus("Invalid PIN. Access denied.");
    return;
  }

  adminPin = nextPin;
  setSourceStatus("Admin access granted.");
  if (adminPinGateEl) adminPinGateEl.classList.add("hidden");
  if (adminContentEl) adminContentEl.classList.remove("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.remove("hidden");
  if (adminPinInputEl) adminPinInputEl.value = "";
  setFetcherLogPlaceholder("Loading latest jobs fetch report...");
  setDiscoveryLogPlaceholder("Loading source discovery data...");
  startBridgeStatusWatch();
  refreshOverview().catch(err => {
    console.error("Failed to refresh admin overview:", err);
  });
  loadLatestFetcherReport({ silent: true }).catch(err => {
    console.error("Failed to load jobs fetch report:", err);
  });
  loadDiscoveryData().catch(err => {
    console.error("Failed to load discovery data:", err);
  });
}

function lockAdmin() {
  adminPin = "";
  if (adminPinGateEl) adminPinGateEl.classList.remove("hidden");
  if (adminContentEl) adminContentEl.classList.add("hidden");
  if (adminLockBtnEl) adminLockBtnEl.classList.add("hidden");
  renderUsersEmpty("");
  if (adminTotalsEl) adminTotalsEl.innerHTML = "";
  stopBridgeStatusWatch();
  setBridgeStatusBadge("checking", "Bridge Locked");
  setFetcherLogPlaceholder("Unlock admin to view fetcher logs and latest report details.");
  setDiscoveryLogPlaceholder("Unlock admin to manage source discovery approvals.");
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
    console.error("Could not trigger VS Code task:", err);
    appendFetcherLog(`Could not trigger VS Code task automatically: ${err?.message || "unknown error"}`, "error");
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
    console.error("Could not load fetch report after task trigger:", fetchErr);
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
  const api = window.JobAppLocalData;
  if (!api || !adminPin) return;

  setSourceStatus("Loading admin overview...");
  try {
    const overview = await api.getAdminOverview(adminPin);
    renderTotals(overview.totals);
    renderUsers(overview.users);
    setSourceStatus(`Loaded ${overview.users.length} user profiles.`);
  } catch (err) {
    console.error("Could not load admin overview:", err);
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
  if (!totals) {
    adminTotalsEl.innerHTML = "";
    return;
  }

  adminTotalsEl.innerHTML = `
    <div class="admin-total-card">
      <div class="admin-total-label">Users</div>
      <div class="admin-total-value">${Number(totals.usersCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Saved Jobs</div>
      <div class="admin-total-value">${Number(totals.savedJobsCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Notes Size</div>
      <div class="admin-total-value">${formatBytes(totals.notesBytes || 0)}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Attachments</div>
      <div class="admin-total-value">${Number(totals.attachmentsCount || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Attachment Size</div>
      <div class="admin-total-value">${formatBytes(totals.attachmentsBytes || 0)}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Total Size</div>
      <div class="admin-total-value">${formatBytes(totals.totalBytes || 0)}</div>
    </div>
  `;
}

function renderUsers(users) {
  if (!Array.isArray(users) || users.length === 0) {
    renderUsersEmpty("No stored profiles found.");
    return;
  }
  if (!adminUsersListEl) return;

  adminUsersListEl.innerHTML = `
    <div class="jobs-table-header">
      <div class="admin-row-header">
        <div>Name</div>
        <div>User ID</div>
        <div>Saved Jobs</div>
        <div>Notes Size</div>
        <div>Attachments</div>
        <div>Attachment Size</div>
        <div>Total Size</div>
        <div>Actions</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${users.map(renderUserRow).join("")}
    </div>
  `;

  adminUsersListEl.querySelectorAll(".admin-wipe-btn").forEach(btn => {
    btn.addEventListener("click", async () => {
      const uid = btn.dataset.uid || "";
      const name = btn.dataset.name || uid;
      await wipeAccount(uid, name);
    });
  });
}

function renderUserRow(user) {
  const uid = escapeHtml(user.uid || "");
  const name = escapeHtml(user.name || user.uid || "Unknown");
  const email = escapeHtml(user.email || "");
  const label = email ? `${name} (${email})` : name;

  return `
    <div class="admin-user-row">
      <div class="admin-cell" data-label="Name">${label}</div>
      <div class="admin-cell admin-uid" data-label="User ID">${uid}</div>
      <div class="admin-cell" data-label="Saved Jobs">${Number(user.savedJobsCount || 0).toLocaleString()}</div>
      <div class="admin-cell" data-label="Notes Size">${formatBytes(user.notesBytes || 0)}</div>
      <div class="admin-cell" data-label="Attachments">${Number(user.attachmentsCount || 0).toLocaleString()}</div>
      <div class="admin-cell" data-label="Attachment Size">${formatBytes(user.attachmentsBytes || 0)}</div>
      <div class="admin-cell" data-label="Total Size">${formatBytes(user.totalBytes || 0)}</div>
      <div class="admin-cell" data-label="Actions">
        <button class="btn back-btn admin-wipe-btn" data-uid="${uid}" data-name="${name}">Wipe Account</button>
      </div>
    </div>
  `;
}

function renderUsersEmpty(message) {
  if (!adminUsersListEl) return;
  adminUsersListEl.innerHTML = message
    ? `<div class="no-results">${escapeHtml(message)}</div>`
    : "";
}

async function wipeAccount(uid, name) {
  if (!uid || !adminPin) return;
  const api = window.JobAppLocalData;
  if (!api) return;

  const confirmed = window.confirm(`Permanently wipe account "${name}"? This deletes profile, saved jobs, notes, and attachments.`);
  if (!confirmed) return;

  try {
    await api.wipeAccountAdmin(adminPin, uid);
    showToast(`Wiped account ${name}.`, "success");
    await refreshOverview();
  } catch (err) {
    console.error("Could not wipe account:", err);
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

function escapeHtml(text) {
  return String(text || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function showToast(message, type = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  document.body.appendChild(toast);
  requestAnimationFrame(() => toast.classList.add("visible"));
  setTimeout(() => {
    toast.classList.remove("visible");
    setTimeout(() => toast.remove(), 220);
  }, 2600);
}

function setDiscoveryLogPlaceholder(message) {
  if (!adminDiscoveryLogEl) return;
  adminDiscoveryLogEl.innerHTML = "";
  appendDiscoveryLog(message, "muted");
}

function appendDiscoveryLog(message, level = "info") {
  if (!adminDiscoveryLogEl) return;
  const row = document.createElement("div");
  row.className = `admin-fetcher-line ${normalizeLogLevel(level)}`;
  const stamp = document.createElement("span");
  stamp.className = "admin-fetcher-time";
  stamp.textContent = toLocalTime(new Date());
  const text = document.createElement("span");
  text.className = "admin-fetcher-text";
  text.textContent = message;
  row.append(stamp, text);
  adminDiscoveryLogEl.appendChild(row);
  while (adminDiscoveryLogEl.children.length > 220) {
    adminDiscoveryLogEl.removeChild(adminDiscoveryLogEl.firstChild);
  }
  adminDiscoveryLogEl.scrollTop = adminDiscoveryLogEl.scrollHeight;
}

function setFetcherLogPlaceholder(message) {
  if (!adminFetcherLogEl) return;
  adminFetcherLogEl.innerHTML = "";
  appendFetcherLog(message, "muted");
}

function appendFetcherLog(message, level = "info") {
  if (!adminFetcherLogEl) return;
  const row = document.createElement("div");
  row.className = `admin-fetcher-line ${normalizeLogLevel(level)}`;

  const stamp = document.createElement("span");
  stamp.className = "admin-fetcher-time";
  stamp.textContent = toLocalTime(new Date());

  const text = document.createElement("span");
  text.className = "admin-fetcher-text";
  text.textContent = message;

  row.append(stamp, text);
  adminFetcherLogEl.appendChild(row);

  while (adminFetcherLogEl.children.length > 220) {
    adminFetcherLogEl.removeChild(adminFetcherLogEl.firstChild);
  }
  adminFetcherLogEl.scrollTop = adminFetcherLogEl.scrollHeight;
}

function normalizeLogLevel(level) {
  const value = String(level || "info").toLowerCase();
  if (value === "error") return "log-error";
  if (value === "warn" || value === "warning") return "log-warn";
  if (value === "success") return "log-success";
  if (value === "muted") return "log-muted";
  return "log-info";
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
}

async function fetchJobsFetchReportJson() {
  try {
    const response = await fetch(`${JOBS_FETCH_REPORT_URL}?t=${Date.now()}`, { cache: "no-store" });
    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }
    return await response.json();
  } catch {
    return null;
  }
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
      console.error("Fetcher completion poll failed:", err);
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
    localStorage.setItem(JOBS_AUTO_REFRESH_SIGNAL_KEY, JSON.stringify(signal));
    appendFetcherLog("Signaled jobs page to auto-refresh from unified feed.", "success");
  } catch {
    appendFetcherLog("Could not write auto-refresh signal to localStorage.", "warn");
  }
}

async function getBridge(path) {
  try {
    const response = await fetch(`${ADMIN_BRIDGE_BASE}${path}?t=${Date.now()}`, {
      method: "GET",
      cache: "no-store"
    });
    if (!response.ok) {
      throw new Error(`Bridge GET ${path} failed with HTTP ${response.status}`);
    }
    setBridgeStatusBadge("online", "Bridge Online");
    return await response.json();
  } catch (error) {
    setBridgeStatusBadge("offline", "Bridge Offline");
    throw error;
  }
}

async function postBridge(path, payload) {
  try {
    const response = await fetch(`${ADMIN_BRIDGE_BASE}${path}`, {
      method: "POST",
      cache: "no-store",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload || {})
    });
    if (!response.ok) {
      throw new Error(`Bridge POST ${path} failed with HTTP ${response.status}`);
    }
    setBridgeStatusBadge("online", "Bridge Online");
    return await response.json();
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
    setTimeout(() => {
      loadDiscoveryData().catch(() => {});
    }, 2500);
  } catch (err) {
    appendDiscoveryLog(`Could not trigger discovery task: ${err?.message || "unknown error"}`, "error");
    showToast("Could not trigger source discovery task.", "error");
  }
}

function renderSourcesTable(container, rows, mode = "pending") {
  if (!container) return;
  if (!Array.isArray(rows) || rows.length === 0) {
    const emptyText = mode === "pending"
      ? "No pending sources."
      : mode === "rejected"
        ? "No rejected sources."
        : "No active sources.";
    container.innerHTML = `<div class="no-results">${emptyText}</div>`;
    return;
  }
  const isPending = mode === "pending";
  const isRejected = mode === "rejected";
  const leadHeader = isPending || isRejected ? "Select" : "State";
  container.innerHTML = `
    <div class="jobs-table-header">
      <div class="admin-row-header admin-source-row-header">
        <div>${leadHeader}</div>
        <div>Name</div>
        <div>Adapter</div>
        <div>Studio</div>
        <div>Jobs Found</div>
        <div>NL</div>
        <div>Remote</div>
        <div>Source ID</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${rows.map(row => {
        const sourceId = escapeHtml(String(row.id || ""));
        const name = escapeHtml(String(row.name || ""));
        const adapter = escapeHtml(String(row.adapter || ""));
        const studio = escapeHtml(String(row.studio || ""));
        const jobsFound = formatSourceJobsFound(row);
        const nl = row.nlPriority ? "Yes" : "No";
        const remote = row.remoteFriendly ? "Yes" : "No";
        const leadCell = isPending
          ? `<input type="checkbox" class="pending-source-checkbox" data-source-id="${sourceId}">`
          : isRejected
            ? `<input type="checkbox" class="rejected-source-checkbox" data-source-id="${sourceId}">`
            : `<span class="muted">Active</span>`;
        return `
          <div class="admin-user-row admin-source-row">
            <div class="admin-cell" data-label="${leadHeader}">${leadCell}</div>
            <div class="admin-cell" data-label="Name">${name}</div>
            <div class="admin-cell" data-label="Adapter">${adapter}</div>
            <div class="admin-cell" data-label="Studio">${studio}</div>
            <div class="admin-cell" data-label="Jobs Found">${jobsFound}</div>
            <div class="admin-cell" data-label="NL">${nl}</div>
            <div class="admin-cell" data-label="Remote">${remote}</div>
            <div class="admin-cell admin-uid" data-label="Source ID">${sourceId}</div>
          </div>
        `;
      }).join("")}
    </div>
  `;
}

function formatSourceJobsFound(row) {
  const value = getSourceJobsFoundCount(row);
  if (!Number.isFinite(value) || value < 0) {
    return "N/A";
  }
  return value.toLocaleString();
}

function getSourceJobsFoundCount(row) {
  const value = Number(
    row?.jobsFound
      ?? row?.sampleCount
      ?? row?.fetchedCount
      ?? row?.lastFetchedCount
      ?? NaN
  );
  return Number.isFinite(value) ? value : NaN;
}

function selectedIds(selector) {
  return Array.from(document.querySelectorAll(selector))
    .filter(el => el instanceof HTMLInputElement && el.checked)
    .map(el => String(el.dataset.sourceId || ""))
    .filter(Boolean);
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
  } catch (err) {
    appendDiscoveryLog(`Approve failed: ${err?.message || "unknown error"}`, "error");
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
  } catch (err) {
    appendDiscoveryLog(`Reject failed: ${err?.message || "unknown error"}`, "error");
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
  } catch (err) {
    appendDiscoveryLog(`Restore failed: ${err?.message || "unknown error"}`, "error");
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
    const summary = report?.summary || {};
    const foundCount = Number(summary.foundEndpointCount ?? summary.probedCount ?? 0);
    const probedCount = Number(summary.probedCandidateCount ?? summary.probedCount ?? 0);
    const queuedCount = Number(summary.queuedCandidateCount ?? summary.newCandidateCount ?? 0);
    const skippedCount = Number(summary.skippedDuplicateCount || 0);
    const failedCount = Number(summary.failedProbeCount || 0);
    const pendingRows = Array.isArray(pending?.sources) ? pending.sources : [];
    const hiddenZeroJobsCount = pendingRows.filter(row => getSourceJobsFoundCount(row) === 0).length;
    const visiblePendingRows = pendingRows.filter(row => getSourceJobsFoundCount(row) !== 0);

    if (adminDiscoverySummaryEl) {
      adminDiscoverySummaryEl.textContent =
        `Found ${foundCount} | Probed ${probedCount} | Queued ${queuedCount} | Failed ${failedCount} | Skipped dupes ${skippedCount} | Pending ${Number(pending?.summary?.pendingCount || 0)} | Active ${Number(active?.summary?.activeCount || 0)} | Rejected ${Number(rejected?.summary?.rejectedCount || 0)} | Hidden zero-jobs ${hiddenZeroJobsCount}`;
    }
    renderSourcesTable(adminPendingSourcesEl, visiblePendingRows, "pending");
    renderSourcesTable(adminActiveSourcesEl, active?.sources || [], "active");
    renderSourcesTable(adminRejectedSourcesEl, rejected?.sources || [], "rejected");
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
  } catch (err) {
    appendDiscoveryLog(`Could not load source discovery data: ${err?.message || "unknown error"}`, "error");
    if (adminDiscoverySummaryEl) {
      adminDiscoverySummaryEl.textContent = "Source discovery bridge unavailable. Start `Run admin bridge` task.";
    }
  }
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
