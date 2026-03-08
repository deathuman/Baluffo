import { escapeHtml, showToast, setText } from "../shared/ui/index.js";

export function adminEscapeHtml(value) {
  return escapeHtml(value);
}

export function setAdminStatus(el, text) {
  setText(el, text);
}

export function showAdminToast(message, type = "info", options = {}) {
  showToast(message, type, options);
}

export function renderTotalsHtml(totals, formatBytes) {
  if (!totals) return "";
  return `
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

export function renderUserRowHtml(user, formatBytes) {
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

export function renderUsersTableHtml(users, formatBytes) {
  return `
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
      ${users.map(user => renderUserRowHtml(user, formatBytes)).join("")}
    </div>
  `;
}

export function renderUsersEmptyHtml(message) {
  return message ? `<div class="no-results">${escapeHtml(message)}</div>` : "";
}

export function renderSourcesTableHtml(rows, mode, formatSourceJobsFound) {
  if (!Array.isArray(rows) || rows.length === 0) {
    const emptyText = mode === "pending"
      ? "No pending sources."
      : mode === "rejected"
        ? "No rejected sources."
        : "No active sources.";
    return `<div class="no-results">${emptyText}</div>`;
  }
  const isPending = mode === "pending";
  const isRejected = mode === "rejected";
  const leadHeader = isPending || isRejected ? "Select" : "State";
  return `
    <div class="jobs-table-header">
      <div class="admin-row-header admin-source-row-header">
        <div>${leadHeader}</div>
        <div>Name</div>
        <div>Adapter</div>
        <div>Studio</div>
        <div>Status</div>
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
        const status = escapeHtml(String(row._lastStatus || row.status || "n/a"));
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
            <div class="admin-cell" data-label="Status"><span class="admin-status-chip ${status === "error" ? "critical" : status === "excluded" ? "warning" : "healthy"}">${status}</span></div>
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

export function appendAdminLogRow(container, event, options = {}) {
  if (!container) return;
  const maxRows = Number(options.maxRows || 220);
  const normalizeLogLevel = options.normalizeLogLevel || (value => value);
  const toLocalTime = options.toLocalTime || (value => String(value));
  const formatLogEventText = options.formatLogEventText || (row => String(row?.message || ""));

  const row = document.createElement("div");
  row.className = `admin-fetcher-line ${normalizeLogLevel(event.level)}`;
  row.dataset.timestamp = event.timestamp;
  row.dataset.level = event.level;
  row.dataset.scope = event.scope;
  row.dataset.sourceId = event.sourceId;

  const stamp = document.createElement("span");
  stamp.className = "admin-fetcher-time";
  stamp.textContent = toLocalTime(new Date(event.timestamp));

  const text = document.createElement("span");
  text.className = "admin-fetcher-text";
  text.textContent = formatLogEventText(event);

  row.append(stamp, text);
  container.appendChild(row);

  while (container.children.length > maxRows) {
    container.removeChild(container.firstChild);
  }
  container.scrollTop = container.scrollHeight;
}

function formatDuration(ms) {
  const value = Math.max(0, Number(ms) || 0);
  if (!value) return "0s";
  if (value < 1000) return `${value}ms`;
  if (value < 60_000) return `${(value / 1000).toFixed(1)}s`;
  return `${(value / 60_000).toFixed(1)}m`;
}

function formatDateTime(value) {
  const parsed = Date.parse(String(value || ""));
  if (!Number.isFinite(parsed)) return "unknown";
  return new Date(parsed).toLocaleString();
}

function formatScheduleCell(entry) {
  const interval = Number(entry?.intervalHours || 0);
  const next = formatDateTime(entry?.nextRunAt || "");
  if (interval > 0) return `every ${interval}h, next ${next}`;
  if (String(entry?.note || "") === "manual_task") return "manual task (no interval)";
  return "unknown";
}

function formatLastRunCell(lastRun) {
  const type = String(lastRun?.type || "");
  const status = String(lastRun?.status || "");
  const finished = formatDateTime(lastRun?.finishedAt || "");
  if (!type) return "none";
  return `${type} ${status} @ ${finished}`;
}

function formatSignedInt(value) {
  const num = Number(value) || 0;
  return num > 0 ? `+${num}` : `${num}`;
}

export function renderAdminOpsAlerts(alertsEl, alerts, handlers = {}) {
  if (!alertsEl) return;
  const rows = Array.isArray(alerts) ? alerts : [];
  if (!rows.length) {
    alertsEl.innerHTML = '<div class="admin-alert-banner healthy">No active alerts.</div>';
    return;
  }
  alertsEl.innerHTML = rows.map(alert => {
    const id = escapeHtml(String(alert?.id || ""));
    const severity = String(alert?.severity || "warning").toLowerCase();
    const cls = severity === "critical" ? "critical" : "warning";
    return `
      <div class="admin-alert-banner ${cls}">
        <div class="admin-alert-message">${escapeHtml(String(alert?.message || id))}</div>
        <button class="btn back-btn admin-alert-ack-btn" data-alert-id="${id}">Dismiss</button>
      </div>
    `;
  }).join("");

  alertsEl.querySelectorAll(".admin-alert-ack-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      if (typeof handlers.onAck === "function") {
        handlers.onAck(String(btn.dataset.alertId || ""));
      }
    });
  });
}

export function renderAdminOpsKpis(kpisEl, kpis, status) {
  if (!kpisEl) return;
  const successRate = Number(kpis?.sevenDayFetchSuccessRate || 0);
  const failedRatio = Number(kpis?.failedSourceRatioLatest || 0);
  const pending = Number(kpis?.pendingApprovalsCount || 0);
  const avgMs = Number(kpis?.avgFetchDurationMs7d || 0);
  const statusClass = status === "critical" ? "critical" : status === "warning" ? "warning" : "healthy";
  kpisEl.innerHTML = `
    <div class="admin-total-card">
      <div class="admin-total-label">Ops Status</div>
      <div class="admin-total-value"><span class="admin-status-chip ${statusClass}">${escapeHtml(status)}</span></div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Last Successful Fetch</div>
      <div class="admin-total-value">${escapeHtml(String(kpis?.lastSuccessfulFetchAge || "unknown"))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Fetch Success (7d)</div>
      <div class="admin-total-value">${(successRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Avg Fetch Duration (7d)</div>
      <div class="admin-total-value">${formatDuration(avgMs)}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Failed Source Ratio</div>
      <div class="admin-total-value">${(failedRatio * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Pending Approvals</div>
      <div class="admin-total-value">${pending.toLocaleString()}</div>
    </div>
  `;
}

export function renderAdminOpsSchedule(scheduleEl, schedule, latestOpsHealthCache) {
  if (!scheduleEl) return;
  const fetcher = schedule?.fetcher || {};
  const discovery = schedule?.discovery || {};
  scheduleEl.innerHTML = `
    <div class="admin-ops-schedule-item"><strong>Fetcher</strong>: ${formatScheduleCell(fetcher)}</div>
    <div class="admin-ops-schedule-item"><strong>Discovery</strong>: ${formatScheduleCell(discovery)}</div>
    <div class="admin-ops-schedule-item"><strong>Last Run</strong>: ${formatLastRunCell(latestOpsHealthCache?.kpis?.lastRunResult || {})}</div>
  `;
}

export function renderAdminOpsTrends(trendsEl, runs) {
  if (!trendsEl) return;
  const rows = Array.isArray(runs) ? runs : [];
  const fetchRuns = rows.filter(row => String(row?.type || "") === "fetch");
  const latest = fetchRuns[fetchRuns.length - 1];
  const prev = fetchRuns[fetchRuns.length - 2];
  if (!latest || !prev) {
    trendsEl.textContent = "Trends: not enough fetch history yet.";
    return;
  }
  const latestOutput = Number(latest?.summary?.outputCount || 0);
  const prevOutput = Number(prev?.summary?.outputCount || 0);
  const latestFailed = Number(latest?.summary?.failedSources || 0);
  const prevFailed = Number(prev?.summary?.failedSources || 0);
  trendsEl.textContent =
    `Trends: output Δ ${formatSignedInt(latestOutput - prevOutput)} (latest ${latestOutput.toLocaleString()}); failed sources Δ ${formatSignedInt(latestFailed - prevFailed)}.`;
}

export function renderAdminOpsHistory(historyEl, runs) {
  if (!historyEl) return;
  const rows = Array.isArray(runs) ? runs : [];
  if (!rows.length) {
    historyEl.innerHTML = '<div class="no-results">No run history yet.</div>';
    return;
  }
  const sorted = [...rows].sort((a, b) =>
    String(b?.finishedAt || b?.startedAt || "").localeCompare(String(a?.finishedAt || a?.startedAt || ""))
  );
  historyEl.innerHTML = `
    <div class="jobs-table-header">
      <div class="admin-row-header admin-ops-history-header">
        <div>Type</div>
        <div>Status</div>
        <div>Duration</div>
        <div>Output/Queued</div>
        <div>Failed</div>
        <div>Finished</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${sorted.map(row => {
        const type = escapeHtml(String(row?.type || "unknown"));
        const status = escapeHtml(String(row?.status || "unknown"));
        const duration = formatDuration(Number(row?.durationMs || 0));
        const summary = row?.summary || {};
        const outputOrQueued = row?.type === "discovery"
          ? Number(summary?.queuedCandidateCount || 0)
          : Number(summary?.outputCount || 0);
        const failed = row?.type === "discovery"
          ? Number(summary?.failedProbeCount || 0)
          : Number(summary?.failedSources || 0);
        const finished = escapeHtml(formatDateTime(row?.finishedAt || row?.startedAt || ""));
        return `
          <div class="admin-user-row admin-source-row admin-ops-history-row">
            <div class="admin-cell">${type}</div>
            <div class="admin-cell"><span class="admin-status-chip ${status === "error" ? "critical" : status === "warning" ? "warning" : "healthy"}">${status}</span></div>
            <div class="admin-cell">${duration}</div>
            <div class="admin-cell">${Number(outputOrQueued).toLocaleString()}</div>
            <div class="admin-cell">${Number(failed).toLocaleString()}</div>
            <div class="admin-cell">${finished}</div>
          </div>
        `;
      }).join("")}
    </div>
  `;
}
