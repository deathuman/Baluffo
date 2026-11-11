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
    ? "Unmeasured tables, indexes, and free pages"
    : "Indexes, schema, and free pages";
  const otherRow = otherBytes > 0
    ? `<div class="admin-storage-table-row admin-storage-overview-row admin-storage-table-other" role="row"
        data-table-name="unattributed"
        data-table-bytes="${otherBytes}"
        data-table-share="${escapeHtml(String(tableSizes.otherShare ?? ""))}">
        <span class="admin-storage-table-label" role="cell">${escapeHtml(otherLabel)}</span>
        <span class="admin-storage-table-cell" role="cell">${escapeHtml(format(otherBytes))}</span>
        ${renderShareBar(formatShare({ bytes: otherBytes, share: tableSizes.otherShare }, allocatedBytes))}
        <span class="admin-storage-table-cell admin-storage-table-rows" role="cell">—</span>
      </div>`
    : "";

  const notices = [];
  if (partial) {
    notices.push(
      "This snapshot is incomplete. The check ran out of time before measuring every table, so the list may be missing smaller tables and the unattributed total may be larger than usual."
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
      ${renderStat("Free space", format(toNumber(tableSizes.freelistBytes)))}
      ${renderStat("Tables measured", formatCount(tables.length), tableSizes.truncated ? "largest only" : "")}
      ${renderStat("Last checked", formatTime(tableSizes.capturedAt))}
    </div>
    ${notices.map(notice => `<p class="admin-storage-notice${partial ? " is-warning" : ""}">${escapeHtml(notice)}</p>`).join("")}
    <div class="admin-storage-table-block">
      <div class="admin-storage-table-block-head">
        <div>
          <h4 class="admin-storage-subtitle">Largest measured tables</h4>
          <p class="admin-storage-card-note">Sizes include table data, indexes, and other pages where measured.</p>
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
