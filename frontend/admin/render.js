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

export function renderSourcesTableHtml(rows, mode, formatSourceJobsFound, resolveSourceStatus) {
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
  const isActive = mode === "active";
  const leadHeader = "Select";
  return `
    <div class="jobs-table-header">
      <div class="admin-row-header admin-source-row-header">
        <div>${leadHeader}</div>
        <div>Name</div>
        <div>Adapter</div>
        <div>Studio</div>
        <div>Status</div>
        <div>Jobs</div>
      </div>
    </div>
    <div class="jobs-table-body">
      ${rows.map(row => {
        const sourceIdRaw = String(row.id || "").trim();
        const sourceId = escapeHtml(sourceIdRaw);
        const name = escapeHtml(String(row.name || ""));
        const adapter = escapeHtml(String(row.adapter || ""));
        const studio = escapeHtml(String(row.studio || ""));
        const resolvedStatus = typeof resolveSourceStatus === "function"
          ? resolveSourceStatus(row)
          : String(row._lastStatus || row.status || "not_run");
        const normalizedStatus = String(resolvedStatus || "").toLowerCase();
        const statusLabel = normalizedStatus === "not_run" || normalizedStatus === "n/a"
          ? "not run yet"
          : String(resolvedStatus || "not run yet");
        const status = escapeHtml(statusLabel);
        const statusErrorDetail = String(row?._lastError || row?.lastProbeError || row?.error || "").trim();
        const statusTitle = normalizedStatus === "error" && statusErrorDetail
          ? ` title="${escapeHtml(`Error: ${statusErrorDetail}`)}"`
          : "";
        const statusClass = normalizedStatus === "error"
          ? "critical"
          : normalizedStatus === "excluded"
            ? "warning"
            : normalizedStatus === "warning" || normalizedStatus === "not_run" || normalizedStatus === "n/a"
              ? "warning"
              : "healthy";
        const jobsFound = formatSourceJobsFound(row);
        const sourceUrl = escapeHtml(String(
          row.listing_url
          || row.api_url
          || row.feed_url
          || row.board_url
          || (Array.isArray(row.pages) ? (row.pages[0] || "") : "")
          || ""
        ));
        const sourceIdTitle = escapeHtml(sourceIdRaw || "missing source id");
        const sourceIdAria = escapeHtml(`Source ID: ${sourceIdRaw || "missing source id"}`);
        const idIconHtml = `<span class="admin-source-id-inline" title="${sourceIdTitle}" aria-label="${sourceIdAria}">i</span>`;
        const leadCell = isPending
          ? `<span class="admin-select-cell-inner"><input type="checkbox" class="pending-source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}">${idIconHtml}</span>`
          : isRejected
            ? `<span class="admin-select-cell-inner"><input type="checkbox" class="rejected-source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}">${idIconHtml}</span>`
            : isActive
              ? `<span class="admin-select-cell-inner"><input type="checkbox" class="active-source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}">${idIconHtml}</span>`
              : `<span class="muted">N/A</span>`;
        return `
          <div class="admin-user-row admin-source-row">
            <div class="admin-cell" data-label="${leadHeader}">${leadCell}</div>
            <div class="admin-cell" data-label="Name">${name}</div>
            <div class="admin-cell" data-label="Adapter">${adapter}</div>
            <div class="admin-cell" data-label="Studio">${studio}</div>
            <div class="admin-cell" data-label="Status"><span class="admin-status-chip ${statusClass}"${statusTitle}>${status}</span></div>
            <div class="admin-cell" data-label="Jobs">${jobsFound}</div>
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

function sanitizeSlowSourceName(value, maxLen = 64) {
  const text = String(value || "")
    .replace(/[^\x20-\x7E]/g, " ")
    .replace(/\s+/g, " ")
    .trim();
  if (!text) return "unknown-source";
  if (text.length <= maxLen) return text;
  return `${text.slice(0, Math.max(1, maxLen - 3)).trim()}...`;
}

function formatLastRunCell(lastRun) {
  const type = String(lastRun?.type || "");
  const status = String(lastRun?.status || "");
  const finished = formatDateTime(lastRun?.finishedAt || "");
  if (!type) return "none";
  return `${type} ${status} @ ${finished}`;
}

function buildRunStatusTooltip(row) {
  const status = String(row?.status || "").toLowerCase();
  if (status !== "warning" && status !== "error") return "";
  const type = String(row?.type || "").toLowerCase();
  const summary = row?.summary || {};
  const parts = [];
  if (type === "discovery") {
    const failed = Number(summary?.failedProbeCount || 0);
    const probed = Number(summary?.probedCandidateCount || 0);
    const queued = Number(summary?.queuedCandidateCount || 0);
    parts.push(`Failed probes: ${failed}`);
    if (probed > 0) parts.push(`Probed: ${probed}`);
    if (queued >= 0) parts.push(`Queued (new): ${queued}`);
  } else {
    const failed = Number(summary?.failedSources || 0);
    const sourceCount = Number(summary?.sourceCount || 0);
    const output = Number(summary?.outputCount || 0);
    parts.push(`Failed sources: ${failed}`);
    if (sourceCount > 0) parts.push(`Sources: ${sourceCount}`);
    parts.push(`Output: ${output}`);
  }
  const durationMs = Number(row?.durationMs || 0);
  if (durationMs > 0) parts.push(`Duration: ${formatDuration(durationMs)}`);
  const stamp = formatDateTime(row?.finishedAt || row?.startedAt || "");
  if (stamp && stamp !== "unknown") parts.push(`Finished: ${stamp}`);
  return parts.join(" | ");
}

function getRunStatusChipClass(status) {
  const token = String(status || "").toLowerCase();
  if (token === "error") return "critical";
  if (token === "warning") return "warning";
  if (token === "running" || token === "started") return "healthy";
  return "healthy";
}

function formatSignedInt(value) {
  const num = Number(value) || 0;
  return num > 0 ? `+${num}` : `${num}`;
}

function stableOpsSignature(value) {
  try {
    if (Array.isArray(value)) {
      return JSON.stringify(value.map(item => item || {}));
    }
    return JSON.stringify(value || {});
  } catch {
    return String(value || "");
  }
}

export function renderAdminOpsAlerts(alertsEl, alerts, handlers = {}) {
  if (!alertsEl) return;
  const canPatchInPlace = Boolean(alertsEl && alertsEl.dataset);
  const rows = Array.isArray(alerts) ? alerts : [];
  const signature = stableOpsSignature(rows.map(alert => ({
    id: String(alert?.id || ""),
    severity: String(alert?.severity || ""),
    message: String(alert?.message || "")
  })));
  if (canPatchInPlace && alertsEl.dataset.opsAlertsSig === signature) return;
  if (canPatchInPlace) alertsEl.dataset.opsAlertsSig = signature;
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
  const canPatchInPlace = Boolean(kpisEl && kpisEl.dataset);
  const signature = stableOpsSignature({
    status: String(status || ""),
    sevenDayFetchSuccessRate: Number(kpis?.sevenDayFetchSuccessRate || 0),
    failedSourceRatioLatest: Number(kpis?.failedSourceRatioLatest || 0),
    pendingApprovalsCount: Number(kpis?.pendingApprovalsCount || 0),
    avgFetchDurationMs7d: Number(kpis?.avgFetchDurationMs7d || 0),
    lastSuccessfulFetchAge: String(kpis?.lastSuccessfulFetchAge || "")
  });
  if (canPatchInPlace && kpisEl.dataset.opsKpisSig === signature) return;
  if (canPatchInPlace) kpisEl.dataset.opsKpisSig = signature;
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
  const canPatchInPlace = Boolean(scheduleEl && scheduleEl.dataset);
  const signature = stableOpsSignature({
    schedule: schedule || {},
    lastRunResult: latestOpsHealthCache?.kpis?.lastRunResult || {}
  });
  if (canPatchInPlace && scheduleEl.dataset.opsScheduleSig === signature) return;
  if (canPatchInPlace) scheduleEl.dataset.opsScheduleSig = signature;
  const fetcher = schedule?.fetcher || {};
  const discovery = schedule?.discovery || {};
  scheduleEl.innerHTML = `
    <div class="admin-ops-schedule-item"><strong>Fetcher</strong>: ${formatScheduleCell(fetcher)}</div>
    <div class="admin-ops-schedule-item"><strong>Discovery</strong>: ${formatScheduleCell(discovery)}</div>
    <div class="admin-ops-schedule-item"><strong>Last Run</strong>: ${formatLastRunCell(latestOpsHealthCache?.kpis?.lastRunResult || {})}</div>
  `;
}

export function renderAdminOpsFetcherMetrics(metricsEl, metrics) {
  if (!metricsEl) return;
  const latest = metrics?.latestRun || {};
  const history = metrics?.history || {};
  const signature = stableOpsSignature({
    latestRun: {
      inputCount: Number(latest?.inputCount || 0),
      outputCount: Number(latest?.outputCount || 0),
      duplicateRate: Number(latest?.duplicateRate || 0),
      sourceFailureRate: Number(latest?.sourceFailureRate || 0),
      failedSources: Number(latest?.failedSources || 0),
      sourceCount: Number(latest?.sourceCount || 0)
    },
    history: {
      windowRuns: Number(history?.windowRuns || 0),
      medianDurationMs: Number(history?.medianDurationMs || 0),
      averageDurationMs: Number(history?.averageDurationMs || 0)
    },
    slowestSources: Array.isArray(latest?.slowestSources) ? latest.slowestSources : []
  });
  if (metricsEl.dataset.opsFetcherMetricsSig === signature) return;
  metricsEl.dataset.opsFetcherMetricsSig = signature;

  const failed = Number(latest?.failedSources || 0);
  const sourceCount = Math.max(0, Number(latest?.sourceCount || 0));
  const duplicateRate = Math.max(0, Number(latest?.duplicateRate || 0));
  const outputYieldRate = Math.max(0, Number(latest?.outputYieldRate || 0));
  const failureRate = Math.max(0, Number(latest?.sourceFailureRate || 0));
  const slowest = Array.isArray(latest?.slowestSources) ? latest.slowestSources : [];
  const slowestSummary = slowest.length
    ? slowest
      .slice(0, 3)
      .map(row => `${sanitizeSlowSourceName(row?.name)} (${formatDuration(Number(row?.durationMs || 0))})`)
      .filter(Boolean)
      .join(" | ")
    : "No source timing data yet.";

  metricsEl.innerHTML = `
    <div class="admin-total-card">
      <div class="admin-total-label">Median Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(history?.medianDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Average Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(history?.averageDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Window Runs</div>
      <div class="admin-total-value">${Number(history?.windowRuns || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Duplicate Rate</div>
      <div class="admin-total-value">${(duplicateRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Output Yield</div>
      <div class="admin-total-value">${(outputYieldRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Source Failures</div>
      <div class="admin-total-value">${failed.toLocaleString()} / ${sourceCount.toLocaleString()} (${(failureRate * 100).toFixed(1)}%)</div>
    </div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Slowest sources</strong>: ${escapeHtml(slowestSummary)}</div>
  `;
}

export function renderAdminOpsTrends(trendsEl, runs) {
  if (!trendsEl) return;
  const canPatchInPlace = Boolean(trendsEl && trendsEl.dataset);
  const rows = Array.isArray(runs) ? runs : [];
  const fetchRuns = rows.filter(row => String(row?.type || "") === "fetch");
  const latest = fetchRuns[fetchRuns.length - 1];
  const prev = fetchRuns[fetchRuns.length - 2];
  if (!latest || !prev) {
    if (canPatchInPlace && trendsEl.dataset.opsTrendSig === "insufficient") return;
    if (canPatchInPlace) trendsEl.dataset.opsTrendSig = "insufficient";
    trendsEl.textContent = "Trends: not enough fetch history yet.";
    return;
  }
  const latestOutput = Number(latest?.summary?.outputCount || 0);
  const prevOutput = Number(prev?.summary?.outputCount || 0);
  const latestFailed = Number(latest?.summary?.failedSources || 0);
  const prevFailed = Number(prev?.summary?.failedSources || 0);
  const summaryText =
    `Trends: output Δ ${formatSignedInt(latestOutput - prevOutput)} (latest ${latestOutput.toLocaleString()}); failed sources Δ ${formatSignedInt(latestFailed - prevFailed)}.`;

  const successfulRuns = fetchRuns
    .filter(row => {
      const status = String(row?.status || row?.displayStatus || "ok").toLowerCase();
      const output = Number(row?.summary?.outputCount || 0);
      return status !== "error" && Number.isFinite(output) && output > 0;
    })
    .map(row => {
      const stamp = Date.parse(String(row?.finishedAt || row?.startedAt || ""));
      return {
        output: Number(row?.summary?.outputCount || 0),
        ts: Number.isFinite(stamp) ? stamp : 0
      };
    })
    .sort((a, b) => a.ts - b.ts)
    .slice(-20);

  if (!successfulRuns.length) {
    if (canPatchInPlace && trendsEl.dataset.opsTrendSig === "empty") return;
    if (canPatchInPlace) trendsEl.dataset.opsTrendSig = "empty";
    trendsEl.textContent = "Trends: no successful fetch history yet.";
    return;
  }
  const signature = successfulRuns.map(item => `${item.ts}:${item.output}`).join("|");
  if (canPatchInPlace && trendsEl.dataset.opsTrendSig === signature) return;
  if (canPatchInPlace) trendsEl.dataset.opsTrendSig = signature;

  const width = 640;
  const height = 170;
  const padLeft = 54;
  const padRight = 16;
  const padTop = 18;
  const padBottom = 34;
  const chartW = width - padLeft - padRight;
  const chartH = height - padTop - padBottom;
  const values = successfulRuns.map(item => item.output);
  const rawMinY = Math.min(...values);
  const rawMaxY = Math.max(...values);
  const range = Math.max(1, rawMaxY - rawMinY);
  const pad = Math.max(1, range * 0.18);
  const zoomMinY = Math.max(0, rawMinY - pad);
  const zoomMaxY = rawMaxY + pad;
  const spanY = Math.max(1, zoomMaxY - zoomMinY);

  const points = successfulRuns.map((item, idx) => {
    const x = padLeft + (successfulRuns.length <= 1 ? chartW / 2 : (idx * chartW) / (successfulRuns.length - 1));
    const y = padTop + chartH - ((item.output - zoomMinY) / spanY) * chartH;
    return { x, y, value: item.output, ts: item.ts };
  });

  const linePath = points.length <= 1
    ? `M ${points[0].x.toFixed(2)} ${points[0].y.toFixed(2)}`
    : points.slice(1).reduce((acc, point, idx) => {
      const prevPoint = points[idx];
      const dx = point.x - prevPoint.x;
      const c1x = prevPoint.x + (dx / 3);
      const c1y = prevPoint.y;
      const c2x = prevPoint.x + (2 * dx / 3);
      const c2y = point.y;
      return `${acc} C ${c1x.toFixed(2)} ${c1y.toFixed(2)} ${c2x.toFixed(2)} ${c2y.toFixed(2)} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`;
    }, `M ${points[0].x.toFixed(2)} ${points[0].y.toFixed(2)}`);

  const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${(padTop + chartH).toFixed(2)} L ${points[0].x.toFixed(2)} ${(padTop + chartH).toFixed(2)} Z`;
  const yTicks = [0, 0.5, 1].map(ratio => ({
    y: padTop + chartH - ratio * chartH,
    label: Math.round(zoomMinY + (spanY * ratio))
  }));
  const xLabel = item => (item.ts ? new Date(item.ts).toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "n/a");
  const first = points[0];
  const mid = points[Math.floor((points.length - 1) / 2)];
  const last = points[points.length - 1];
  const pointDots = points.map(point =>
    `<circle cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="2.0" class="admin-ops-trend-dot"><title>${point.value.toLocaleString()} jobs</title></circle>`
  ).join("");

  trendsEl.innerHTML = `
    <div class="admin-ops-trend-summary">${escapeHtml(summaryText)}</div>
    <svg class="admin-ops-trend-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Successful jobs fetched over time">
      <path class="admin-ops-trend-area" d="${areaPath}" />
      ${yTicks.map(tick => `<line class="admin-ops-trend-grid" x1="${padLeft}" x2="${width - padRight}" y1="${tick.y.toFixed(2)}" y2="${tick.y.toFixed(2)}" />`).join("")}
      ${yTicks.map(tick => `<text class="admin-ops-trend-y-label" x="${padLeft - 8}" y="${(tick.y + 4).toFixed(2)}" text-anchor="end">${tick.label.toLocaleString()}</text>`).join("")}
      <path class="admin-ops-trend-line" d="${linePath}" />
      ${pointDots}
      <text class="admin-ops-trend-x-label" x="${first.x.toFixed(2)}" y="${height - 10}" text-anchor="start">${escapeHtml(xLabel(first))}</text>
      <text class="admin-ops-trend-x-label" x="${mid.x.toFixed(2)}" y="${height - 10}" text-anchor="middle">${escapeHtml(xLabel(mid))}</text>
      <text class="admin-ops-trend-x-label" x="${last.x.toFixed(2)}" y="${height - 10}" text-anchor="end">${escapeHtml(xLabel(last))}</text>
    </svg>
  `;
}

export function renderAdminOpsHistory(historyEl, runsOrModel) {
  if (!historyEl) return;
  const model = Array.isArray(runsOrModel)
    ? {
      currentRows: [],
      visibleCompletedRows: runsOrModel,
      olderCompletedRows: []
    }
    : (runsOrModel || {});
  const currentRows = Array.isArray(model.currentRows) ? model.currentRows : [];
  const visibleCompletedRows = Array.isArray(model.visibleCompletedRows) ? model.visibleCompletedRows : [];
  const olderCompletedRows = Array.isArray(model.olderCompletedRows) ? model.olderCompletedRows : [];
  const canPatchInPlace = Boolean(
    historyEl
    && typeof historyEl.querySelector === "function"
    && typeof historyEl.querySelectorAll === "function"
    && historyEl.dataset
  );
  if (!currentRows.length && !visibleCompletedRows.length && !olderCompletedRows.length) {
    historyEl.innerHTML = '<div class="no-results">No run history yet.</div>';
    if (canPatchInPlace) {
      delete historyEl.dataset.opsStructureSig;
    }
    return;
  }

  const toRowView = (row, rowArea, index) => {
    const rawStatus = String(row?.displayStatus || row?.status || "unknown");
    const statusToken = rawStatus.toLowerCase();
    const summary = row?.summary || {};
    const type = String(row?.type || "unknown");
    const syncAction = String(summary?.action || "").trim().toLowerCase();
    const syncLabel = syncAction ? `Sync ${syncAction}` : "Sync";
    const syncCounts = [summary?.activeCount, summary?.pendingCount, summary?.rejectedCount]
      .map(value => Number(value || 0))
      .map(value => value.toLocaleString())
      .join("/");
    const key = [
      rowArea,
      String(row?.id || ""),
      type,
      String(row?.startedAt || ""),
      String(row?.finishedAt || ""),
      String(index)
    ].join("|");
    return {
      key,
      rowArea,
      typeText: type,
      statusText: rawStatus,
      statusClass: getRunStatusChipClass(rawStatus),
      statusTitle: buildRunStatusTooltip(row),
      isRunning: statusToken === "running" || statusToken === "started",
      durationText: formatDuration(Number(row?.elapsedMs ?? row?.durationMs ?? 0)),
      outputOrQueuedText: row?.type === "discovery"
        ? `Queued (new): ${Number(summary?.queuedCandidateCount || 0).toLocaleString()}`
        : row?.type === "sync"
          ? `${syncLabel} (${syncCounts})`
          : Number(summary?.outputCount || 0).toLocaleString(),
      failedText: (row?.type === "discovery"
        ? Number(summary?.failedProbeCount || 0)
        : row?.type === "sync"
          ? Number(String(summary?.error || "").trim().length > 0 ? 1 : 0)
          : Number(summary?.failedSources || 0)).toLocaleString(),
      finishedText: formatDateTime(row?.finishedAt || row?.startedAt || "")
    };
  };

  const currentViews = currentRows.map((row, index) => toRowView(row, "current", index));
  const visibleCompletedViews = visibleCompletedRows.map((row, index) => toRowView(row, "current", currentRows.length + index));
  const olderCompletedViews = olderCompletedRows.map((row, index) => toRowView(row, "completed_older", index));
  const primaryViews = [...currentViews, ...visibleCompletedViews];

  const structureSignature = JSON.stringify({
    current: primaryViews.map(row => row.key),
    completedOlder: olderCompletedViews.map(row => row.key)
  });

  const updateExistingRows = (views, rowArea) => {
    const rowMap = new Map(
      Array.from(historyEl.querySelectorAll(`.admin-ops-history-row[data-row-area="${rowArea}"]`))
        .map(rowEl => [String(rowEl.dataset.runKey || ""), rowEl])
    );
    views.forEach(view => {
      const rowEl = rowMap.get(view.key);
      if (!rowEl) return;
      rowEl.classList.toggle("admin-ops-history-row-running", view.isRunning);
      const cells = rowEl.querySelectorAll(".admin-cell");
      if (cells.length < 6) return;
      cells[0].textContent = view.typeText;
      const chip = cells[1].querySelector(".admin-status-chip");
      if (chip) {
        chip.className = `admin-status-chip ${view.statusClass}`;
        chip.textContent = view.statusText;
        if (view.statusTitle) {
          chip.setAttribute("title", view.statusTitle);
        } else {
          chip.removeAttribute("title");
        }
      }
      cells[2].textContent = view.durationText;
      cells[3].textContent = view.outputOrQueuedText;
      cells[4].textContent = view.failedText;
      cells[5].textContent = view.finishedText;
    });
  };

  if (canPatchInPlace && historyEl.dataset.opsStructureSig === structureSignature) {
    updateExistingRows(primaryViews, "current");
    updateExistingRows(olderCompletedViews, "completed_older");
    return;
  }

  const olderOpen = canPatchInPlace ? Boolean(historyEl.querySelector(".admin-ops-history-older")?.open) : false;
  if (canPatchInPlace) {
    historyEl.dataset.opsStructureSig = structureSignature;
  }

  const renderRows = views => views.map(view => `
      <div class="admin-user-row admin-source-row admin-ops-history-row${view.isRunning ? " admin-ops-history-row-running" : ""}" data-row-area="${view.rowArea}" data-run-key="${escapeHtml(view.key)}">
        <div class="admin-cell">${escapeHtml(view.typeText)}</div>
        <div class="admin-cell"><span class="admin-status-chip ${view.statusClass}"${view.statusTitle ? ` title="${escapeHtml(view.statusTitle)}"` : ""}>${escapeHtml(view.statusText)}</span></div>
        <div class="admin-cell">${escapeHtml(view.durationText)}</div>
        <div class="admin-cell">${escapeHtml(view.outputOrQueuedText)}</div>
        <div class="admin-cell">${escapeHtml(view.failedText)}</div>
        <div class="admin-cell">${escapeHtml(view.finishedText)}</div>
      </div>
    `).join("");

  historyEl.innerHTML = `
    <div class="admin-ops-current-runs">
      <div class="admin-ops-history-title">Current Runs</div>
      <div class="jobs-table-header">
        <div class="admin-row-header admin-ops-history-header">
          <div>Type</div>
          <div>Status</div>
          <div>Duration</div>
          <div>Output / Queued (new)</div>
          <div>Failed</div>
          <div>Finished</div>
        </div>
      </div>
      <div class="jobs-table-body">
        ${primaryViews.length ? renderRows(primaryViews) : '<div class="no-results">No active runs.</div>'}
      </div>
    </div>
    ${olderCompletedViews.length ? `
      <details class="admin-ops-history-older admin-ops-completed-runs">
        <summary>Older runs (${olderCompletedViews.length})</summary>
        <div class="jobs-table-body">
          ${renderRows(olderCompletedViews)}
        </div>
      </details>
    ` : ""}
  `;
  if (canPatchInPlace) {
    const detailsEl = historyEl.querySelector(".admin-ops-history-older");
    if (detailsEl) detailsEl.open = olderOpen;
  }
}
