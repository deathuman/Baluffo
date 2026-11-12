import { escapeHtml } from "../../shared/ui/index.js";

const TABLE_LABELS = Object.freeze({
  job_feed_state: "Current feed state",
  jobs: "Job listings",
  job_sources: "Saved source records",
  source_runs: "Fetch history",
  task_events: "Task history",
  task_runs: "Task runs",
  sync_runs: "Sync history",
  source_registry_rows: "Source registry",
  source_registry_tombstones: "Deleted source records",
  source_registry_state: "Source registry state"
});

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatCount(value) {
  return toNumber(value).toLocaleString("en-US");
}

function formatPercent(fraction, fallback = "—") {
  const number = Number(fraction);
  if (!Number.isFinite(number) || number < 0) return fallback;
  if (number === 0) return "0%";
  if (number < 0.001) return "<0.1%";
  return `${(number * 100).toFixed(1)}%`;
}

function formatShare(row, allocatedBytes) {
  const share = Number(row?.share);
  if (Number.isFinite(share)) return share;
  const bytes = toNumber(row?.bytes);
  return allocatedBytes > 0 ? bytes / allocatedBytes : null;
}

function formatTime(value) {
  const text = String(value || "").trim();
  if (!text) return "Not recorded";
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? text : date.toLocaleString();
}

function formatDuration(value) {
  const totalSeconds = Math.max(0, Math.floor(toNumber(value) / 1000));
  if (totalSeconds < 60) return `${totalSeconds}s`;
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  if (minutes < 60) return `${minutes}m ${seconds}s`;
  const hours = Math.floor(minutes / 60);
  return `${hours}h ${minutes % 60}m`;
}

function formatLockDuration(value) {
  const heldMs = Math.max(0, toNumber(value));
  if (heldMs > 0 && heldMs < 1000) return "<1s";
  return formatDuration(heldMs);
}

export function renderStorageWriteLockHtml(writeLock) {
  const label = '<span class="admin-storage-write-lock-label">Write lock</span>';
  if (!writeLock || typeof writeLock !== "object") {
    return `${label}<strong>Status unavailable</strong><span>Storage health did not report a lock holder.</span>`;
  }
  if (writeLock.held !== true) {
    return `${label}<strong>No write lock held</strong><span>Database writes can proceed.</span>`;
  }
  const holder = String(writeLock.threadName || "").trim() || "unknown thread";
  const operation = String(writeLock.operation || "").trim();
  const operationText = operation ? ` (${escapeHtml(operation)})` : "";
  return `${label}<strong>Held by ${escapeHtml(holder)}${operationText}</strong><span>for ${escapeHtml(formatLockDuration(writeLock.heldMs))}</span>`;
}

export function storageMaintenancePhaseLabel(phase) {
  const labels = {
    starting: "Preparing maintenance",
    "prune-source-runs": "Removing old fetch history",
    "prune-event-history": "Pruning task and sync history",
    "compact-archived-payloads": "Compacting archived payloads",
    "prune-job-generations": "Removing older saved versions",
    complete: "Complete",
    failed: "Failed",
    "held-active-task": "Waiting for active task",
    "not-run": "Not started"
  };
  return labels[String(phase || "")] || String(phase || "Not started");
}

export function renderStorageMaintenanceHtml(
  runtimeRetention,
  compaction,
  formatBytesFormatter
) {
  const status = runtimeRetention && typeof runtimeRetention === "object"
    ? runtimeRetention
    : null;
  if (!status) return "";
  const running = status.running === true || status.status === "running";
  if (!running && String(status.status || "") === "not-run") return "";

  const format = typeof formatBytesFormatter === "function" ? formatBytesFormatter : formatBytes;
  const phase = String(status.phase || status.status || "");
  const phaseLabel = storageMaintenancePhaseLabel(phase);
  const elapsed = toNumber(status.elapsedMs ?? status.durationMs);
  const totalRuns = toNumber(status.totalRunCount);
  const processedRuns = toNumber(status.processedRunCount);
  const deletedRuns = toNumber(status.deletedRunCount);
  const deletedRows = toNumber(status.deletedRowCount);
  const deletedEvents = toNumber(status.deletedEventCount);
  const deletedSyncRuns = toNumber(status.deletedSyncRunCount);
  const deletedGenerations = toNumber(status.deletedGenerationCount);
  const details = [];

  details.push(`<span class="admin-storage-maintenance-phase">${escapeHtml(phaseLabel)}</span>`);
  if (elapsed > 0) details.push(`<span>Elapsed ${escapeHtml(formatDuration(elapsed))}</span>`);
  if (totalRuns > 0) {
    details.push(
      `<span>${formatCount(processedRuns)} of ${formatCount(totalRuns)} runs</span>`
    );
  }
  if (deletedRuns > 0 || deletedRows > 0) {
    details.push(
      `<span>Removed ${formatCount(deletedRuns)} runs / ${formatCount(deletedRows)} rows</span>`
    );
  }
  if (deletedEvents > 0 || deletedSyncRuns > 0) {
    details.push(
      `<span>Task/sync rows ${formatCount(deletedEvents)} / ${formatCount(deletedSyncRuns)}</span>`
    );
  }
  if (deletedGenerations > 0) {
    details.push(`<span>Removed ${formatCount(deletedGenerations)} saved versions</span>`);
  }
  if (compaction && String(compaction.status || "") !== "not-run") {
    const reclaimed = toNumber(compaction.reclaimedBytes);
    const compactionText = reclaimed > 0
      ? `${String(compaction.status)} · ${format(reclaimed)} reclaimed`
      : String(compaction.status);
    details.push(`<span>Compaction: ${escapeHtml(compactionText)}</span>`);
  }
  if (status.error) {
    details.push(`<span class="admin-storage-maintenance-error">${escapeHtml(String(status.error))}</span>`);
  }

  const headline = running
    ? "Maintenance running"
    : status.status === "failed"
      ? "Maintenance failed"
      : status.status === "held-active-task"
        ? "Maintenance paused"
        : "Maintenance complete";
  return `<div class="admin-storage-maintenance-progress${running ? " is-running" : ""}" data-maintenance-phase="${escapeHtml(phase)}" data-maintenance-running="${running ? "true" : "false"}">
    <div class="admin-storage-maintenance-progress-head">
      <strong>${escapeHtml(headline)}</strong>
      <span>${escapeHtml(phaseLabel)}</span>
    </div>
    <div class="admin-storage-maintenance-progress-meta">${details.join("")}</div>
  </div>`;
}

function tableLabel(name) {
  return TABLE_LABELS[name] || name || "Unknown table";
}

function renderRawName(name, label) {
  return label === name || !name
    ? ""
    : ` <code class="admin-storage-table-code">${escapeHtml(name)}</code>`;
}

function renderStat(label, value, note = "") {
  return `<div class="admin-storage-stat">
    <span class="admin-storage-stat-label">${escapeHtml(label)}</span>
    <strong class="admin-storage-stat-value">${escapeHtml(value)}</strong>
    ${note ? `<span class="admin-storage-stat-note">${escapeHtml(note)}</span>` : ""}
  </div>`;
}

function renderFact(label, value) {
  return `<div class="admin-storage-fact">
    <dt>${escapeHtml(label)}</dt>
    <dd>${escapeHtml(String(value))}</dd>
  </div>`;
}

function renderShareBar(fraction) {
  const width = fraction === null ? 0 : Math.min(100, Math.max(0, fraction * 100));
  return `<span class="admin-storage-table-share-cell">
    <span class="admin-storage-share-track" aria-hidden="true"><span class="admin-storage-share-fill" style="width:${width}%"></span></span>
    <span>${escapeHtml(formatPercent(fraction))}</span>
  </span>`;
}

function renderTableRow(table, estimated, allocatedBytes, formatBytes) {
  const name = String(table?.name || "unknown");
  const label = tableLabel(name);
  const bytes = toNumber(table?.bytes);
  const rowCount = table?.rowCount;
  const rowCountText = rowCount === null || rowCount === undefined
    ? "—"
    : `${estimated ? "~" : ""}${formatCount(rowCount)}`;
  const partial = table?.partial
    ? ' <span class="admin-storage-table-partial">incomplete</span>'
    : "";
  return `<div class="admin-storage-table-row admin-storage-overview-row" role="row"
    data-table-name="${escapeHtml(name)}"
    data-table-bytes="${bytes}"
    data-table-share="${escapeHtml(String(table?.share ?? ""))}"
    data-table-row-count="${escapeHtml(rowCount === null || rowCount === undefined ? "" : String(rowCount))}"
    data-sampled-rows="${escapeHtml(String(table?.sampledRows ?? ""))}">
    <span class="admin-storage-table-label" role="cell">
      <span>${escapeHtml(label)}${renderRawName(name, label)}</span>
      ${partial}
    </span>
    <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatBytes(bytes))}</span>
    ${renderShareBar(formatShare(table, allocatedBytes))}
    <span class="admin-storage-table-cell admin-storage-table-rows" role="cell">${escapeHtml(rowCountText)}</span>
  </div>`;
}

function renderTechnicalDetails(tableSizes, formatBytes, tableCount) {
  const facts = [
    renderFact("Measurement", tableSizes.estimated ? "Sampled estimate" : "Exact allocation"),
    renderFact("Method", tableSizes.method || "Not reported"),
    renderFact("Allocated space", formatBytes(toNumber(tableSizes.allocatedBytes))),
    renderFact("Database file", formatBytes(toNumber(tableSizes.databaseBytes))),
    renderFact("Free space", formatBytes(toNumber(tableSizes.freelistBytes))),
    renderFact("Tables measured", `${formatCount(tableCount)}${tableSizes.truncated ? " (largest only)" : ""}`),
    renderFact("Snapshot complete", tableSizes.complete === false ? "No" : "Yes"),
    tableSizes.durationMs === null || tableSizes.durationMs === undefined
      ? ""
      : renderFact("Time taken", `${formatCount(tableSizes.durationMs)} ms`),
    tableSizes.budgetMs === null || tableSizes.budgetMs === undefined
      ? ""
      : renderFact("Time budget", `${formatCount(tableSizes.budgetMs)} ms`),
    renderFact("Captured", formatTime(tableSizes.capturedAt))
  ].filter(Boolean);

  return `<details class="admin-storage-technical">
    <summary>Technical details</summary>
    <div class="admin-storage-technical-body">
      <dl class="admin-storage-facts">${facts.join("")}</dl>
      <p class="admin-storage-card-note">These are the raw values from the storage-health API. Row counts marked ~ are estimates, and sampled tables use a bounded row-id sample rather than a full scan.</p>
    </div>
  </details>`;
}

function formatBytes(value) {
  return `${Number(value || 0).toLocaleString("en-US")} B`;
}

export function renderStorageTablesMessageHtml(message) {
  return `<div class="admin-storage-state"><p>${escapeHtml(message)}</p></div>`;
}

export function renderStorageTablesHtml(tableSizes, formatBytesFormatter) {
  const format = typeof formatBytesFormatter === "function" ? formatBytesFormatter : formatBytes;
  if (!tableSizes || typeof tableSizes !== "object") {
    return renderStorageTablesMessageHtml("Storage health did not include a table-size breakdown.");
  }
  if (tableSizes.status === "unavailable") {
    return renderStorageTablesMessageHtml(
      `Table sizes unavailable: ${String(tableSizes.error || "the storage health read did not run")}`
    );
  }

  const allocatedBytes = toNumber(tableSizes.allocatedBytes);
  const tables = Array.isArray(tableSizes.tables) ? tableSizes.tables : [];
  const partial = tableSizes.status === "partial"
    || tableSizes.complete === false
    || tableSizes.truncated === true;
  const snapshotLabel = partial
    ? "Partial snapshot"
    : tableSizes.estimated ? "Estimated snapshot" : "Measured snapshot";
  const statusClass = partial ? " is-warning" : " is-ok";
  const rows = tables.map(table =>
    renderTableRow(table, tableSizes.estimated, allocatedBytes, format)
  ).join("");
  const otherBytes = toNumber(tableSizes.otherBytes);
  const otherLabel = partial
    ? "Unattributed allocated space"
    : "Indexes, schema, and free pages";
  const otherShare = partial
    ? null
    : formatShare({ bytes: otherBytes, share: tableSizes.otherShare }, allocatedBytes);
  const otherRow = otherBytes > 0
    ? `<div class="admin-storage-table-row admin-storage-overview-row admin-storage-table-other" role="row"
        data-table-name="unattributed"
        data-table-bytes="${otherBytes}"
        data-table-share="${partial ? "" : escapeHtml(String(tableSizes.otherShare ?? ""))}">
        <span class="admin-storage-table-label" role="cell">${escapeHtml(otherLabel)}</span>
        <span class="admin-storage-table-cell" role="cell">${escapeHtml(format(otherBytes))}</span>
        ${renderShareBar(otherShare)}
        <span class="admin-storage-table-cell admin-storage-table-rows" role="cell">—</span>
      </div>`
    : "";

  const notices = [];
  if (partial) {
    notices.push(
      "This bounded table sample ran out of time. Database size, allocated space, and free space are exact; table rows are estimates, and unattributed space includes tables and indexes the sample did not measure."
    );
  } else if (tableSizes.estimated) {
    notices.push("These sizes are estimated from a small sample, not exact page counts.");
  }
  if (tables.length === 0) {
    notices.push("No table rows were measured in this snapshot.");
  }

  return `<section class="admin-storage-card admin-storage-overview" data-storage-api="storage-health" data-storage-method="${escapeHtml(String(tableSizes.method || ""))}" data-storage-complete="${tableSizes.complete === false ? "false" : "true"}">
    <div class="admin-storage-card-head">
      <div>
        <h3 class="admin-storage-card-title">Space at a glance</h3>
        <p class="admin-storage-card-copy">The local database holds the current job feed and the history needed to keep it up to date.</p>
      </div>
      <span class="admin-storage-pill${statusClass}">${escapeHtml(snapshotLabel)}</span>
    </div>
    <div class="admin-storage-stats">
      ${renderStat("Database size", format(toNumber(tableSizes.databaseBytes)))}
      ${renderStat("Allocated space", format(allocatedBytes))}
      ${renderStat("Free space", format(toNumber(tableSizes.freelistBytes)))}
      ${renderStat("Last checked", formatTime(tableSizes.capturedAt))}
    </div>
    ${notices.map(notice => `<p class="admin-storage-notice${partial ? " is-warning" : ""}">${escapeHtml(notice)}</p>`).join("")}
    <div class="admin-storage-table-block">
      <div class="admin-storage-table-block-head">
        <div>
          <h4 class="admin-storage-subtitle">${partial ? "Largest tables in this sample" : "Largest measured tables"}</h4>
          <p class="admin-storage-card-note">${partial ? "Measured rows are estimates; the unattributed row is the remaining allocated space." : "Sizes include table data, indexes, and other pages where measured."}</p>
        </div>
      </div>
      <div class="admin-storage-table" role="table" aria-label="Runtime database table sizes">
        <div class="admin-storage-table-row admin-storage-overview-row admin-storage-table-head" role="row">
          <span role="columnheader">Table</span>
          <span role="columnheader">Size</span>
          <span role="columnheader">Share</span>
          <span class="admin-storage-table-rows" role="columnheader">Rows</span>
        </div>
        ${rows}${otherRow}
      </div>
    </div>
    ${renderTechnicalDetails(tableSizes, format, tables.length)}
  </section>`;
}
