import { escapeHtml } from "../../shared/ui/index.js";

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatCount(value) {
  return toNumber(value).toLocaleString();
}

function formatTime(value) {
  const text = String(value || "").trim();
  if (!text) return "—";
  const date = new Date(text);
  return Number.isNaN(date.getTime()) ? text : date.toLocaleString();
}

function phaseLabel(phase) {
  const labels = {
    queued: "Queued",
    opening: "Opening database",
    counters: "Reading page counters",
    feed_state: "Reading current feed state",
    tables: "Measuring table allocation",
    job_sources: "Grouping job_sources by generation",
    jobs: "Grouping jobs by generation",
    finalizing: "Finalizing",
    cancelling: "Cancelling"
  };
  return labels[String(phase || "")] || String(phase || "Not run");
}

function generationLabel(generation) {
  return String(generation || "").trim() || "(empty generation)";
}

function renderMeta(diagnostic, formatBytes) {
  const parts = [
    `${escapeHtml(formatBytes(toNumber(diagnostic.databaseBytes)))} database`,
    `${escapeHtml(formatBytes(toNumber(diagnostic.walBytes)))} WAL`,
    diagnostic.capturedAt ? `captured ${escapeHtml(formatTime(diagnostic.capturedAt))}` : "",
    diagnostic.durationMs === null || diagnostic.durationMs === undefined
      ? ""
      : `${formatCount(diagnostic.durationMs)} ms`,
    diagnostic.runId ? `run ${escapeHtml(String(diagnostic.runId))}` : ""
  ].filter(Boolean);
  return `<div class="admin-storage-diagnostic-meta">${parts.map(part => `<span>${part}</span>`).join("")}</div>`;
}

function renderProgress(diagnostic, formatBytes) {
  const progress = diagnostic.progress || {};
  const operations = toNumber(progress.operationCount);
  const quick = diagnostic.mode === "quick";
  return `
    ${renderMeta(diagnostic, formatBytes)}
    <div class="admin-storage-diagnostic-progress">
      <strong>${escapeHtml(phaseLabel(diagnostic.phase))}</strong>
      <span>${formatCount(diagnostic.elapsedMs)} ms elapsed</span>
      ${operations > 0 ? `<span>${formatCount(operations)} SQLite operations</span>` : ""}
    </div>
    <p class="admin-storage-diagnostic-note">This read-only ${quick ? "quick generation check" : "scan"} is running in the bridge background. You can keep using Admin or cancel it.</p>
  `;
}

function renderQuickMeta(diagnostic, formatBytes) {
  const parts = [
    `${escapeHtml(formatBytes(toNumber(diagnostic.databaseBytes)))} database`,
    `${escapeHtml(formatBytes(toNumber(diagnostic.walBytes)))} WAL`,
    `${formatCount(diagnostic.pageSize)} B pages`,
    `${formatCount(diagnostic.pageCount)} pages`,
    `${escapeHtml(formatBytes(toNumber(diagnostic.freelistBytes)))} free`,
    diagnostic.capturedAt ? `captured ${escapeHtml(formatTime(diagnostic.capturedAt))}` : "",
    diagnostic.durationMs === null || diagnostic.durationMs === undefined
      ? ""
      : `${formatCount(diagnostic.durationMs)} ms`,
    diagnostic.runId ? `run ${escapeHtml(String(diagnostic.runId))}` : ""
  ].filter(Boolean);
  return `<div class="admin-storage-diagnostic-meta">${parts.map(part => `<span>${part}</span>`).join("")}</div>`;
}

function renderQuickFeedState(feedState) {
  if (!feedState || typeof feedState !== "object") return "";
  return `<div class="admin-storage-diagnostic-section">
    <h4>Current feed</h4>
    <div class="admin-storage-diagnostic-meta">
      <span>${escapeHtml(generationLabel(feedState.currentGeneration))}</span>
      <span>${formatCount(feedState.currentRowCount)} jobs</span>
      <span>${formatCount(feedState.currentSourceCount)} source rows</span>
      ${feedState.currentPublishedAt ? `<span>published ${escapeHtml(formatTime(feedState.currentPublishedAt))}</span>` : ""}
    </div>
  </div>`;
}

function renderQuickGenerations(title, section) {
  if (!section || typeof section !== "object") return "";
  const generations = Array.isArray(section.byGeneration) ? section.byGeneration : [];
  const currentOnly = section.scope === "current";
  const partial = section.status === "partial";
  const heading = currentOnly ? `${title} — current generation` : title;
  const notes = [
    currentOnly
      ? "Only the current generation is counted here. Older generations are left to the exact diagnostic."
      : "",
    partial ? String(section.error || "This generation count did not finish.") : ""
  ].filter(Boolean);
  const rows = generations.map(row => {
    const current = row?.isCurrent
      ? ' <span class="admin-storage-diagnostic-current">current</span>'
      : row?.isOrphan
        ? ' <span class="admin-storage-diagnostic-orphan">orphan</span>'
        : "";
    return `<div class="admin-storage-diagnostic-row admin-storage-diagnostic-quick-row" role="row">
      <span class="admin-storage-diagnostic-name" role="cell">${escapeHtml(generationLabel(row?.generation))}${current}</span>
      <span role="cell">${formatCount(row?.rowCount)}</span>
      <span role="cell">${row?.isOrphan ? "orphan" : row?.isCurrent ? "current" : "—"}</span>
    </div>`;
  }).join("");
  return `<div class="admin-storage-diagnostic-section">
    <h4>${escapeHtml(heading)}</h4>
    <div class="admin-storage-diagnostic-meta">
      <span>${formatCount(section.listedRowCount)} listed rows</span>
      <span>${formatCount(section.generationCount)} generations shown</span>
      ${section.truncated ? "<span>top generations only</span>" : ""}
    </div>
    ${notes.map(note => `<p class="admin-storage-diagnostic-note">${escapeHtml(note)}</p>`).join("")}
    <div class="admin-storage-diagnostic-grid" role="table" aria-label="${escapeHtml(title)}">
      <div class="admin-storage-diagnostic-row admin-storage-diagnostic-quick-row admin-storage-diagnostic-head" role="row">
        <span role="columnheader">Generation</span>
        <span role="columnheader">Rows</span>
        <span role="columnheader">State</span>
      </div>
      ${rows || '<div class="admin-storage-diagnostic-note">No rows were found.</div>'}
    </div>
  </div>`;
}

function renderQuickDiagnostic(diagnostic, formatBytes) {
  return `
    ${renderQuickMeta(diagnostic, formatBytes)}
    <p class="admin-storage-diagnostic-note">Quick check reads current feed counts and counts jobs by generation. It does not count older job_sources generations or measure table or index allocation.</p>
    ${Array.isArray(diagnostic.warnings) && diagnostic.warnings.length
      ? `<p class="admin-storage-diagnostic-warning">${diagnostic.warnings.map(warning => escapeHtml(String(warning))).join(" ")}</p>`
      : ""}
    ${renderQuickFeedState(diagnostic.feedState)}
    ${renderQuickGenerations("job_sources by feed generation", diagnostic.jobSourcesByGeneration)}
    ${renderQuickGenerations("jobs by feed generation", diagnostic.jobsByGeneration)}
  `;
}

function renderTableAllocation(tableSizes, formatBytes) {
  if (!tableSizes || tableSizes.status === "unavailable") {
    const reason = String(tableSizes?.error || "SQLite dbstat is unavailable in this runtime");
    return `<div class="admin-storage-diagnostic-section">
      <h4>Table allocation</h4>
      <div class="admin-storage-diagnostic-note">${escapeHtml(reason)}</div>
    </div>`;
  }
  const tables = Array.isArray(tableSizes.tables) ? tableSizes.tables : [];
  const rows = tables.map(table => {
    const bytes = toNumber(table?.bytes);
    const share = Number(table?.share);
    return `<div class="admin-storage-diagnostic-row" role="row">
      <span class="admin-storage-diagnostic-name" role="cell">${escapeHtml(String(table?.name || "unknown"))}</span>
      <span role="cell">${escapeHtml(formatBytes(toNumber(table?.tableBytes)))}</span>
      <span role="cell">${escapeHtml(formatBytes(toNumber(table?.indexBytes)))}</span>
      <span role="cell">${escapeHtml(formatBytes(bytes))}</span>
      <span role="cell">${escapeHtml(Number.isFinite(share) && share > 0 ? `${(share * 100).toFixed(1)}%` : "—")}</span>
    </div>`;
  }).join("");
  const otherBytes = toNumber(tableSizes.otherBytes);
  const otherRow = otherBytes > 0
    ? `<div class="admin-storage-diagnostic-row admin-storage-diagnostic-row-other" role="row">
      <span role="cell">Freelist and unassigned pages</span>
      <span role="cell">—</span>
      <span role="cell">—</span>
      <span role="cell">${escapeHtml(formatBytes(otherBytes))}</span>
      <span role="cell">—</span>
    </div>`
    : "";
  return `<div class="admin-storage-diagnostic-section">
    <h4>Table allocation</h4>
    <div class="admin-storage-diagnostic-meta">
      <span>${escapeHtml(formatBytes(toNumber(tableSizes.allocatedBytes)))} allocated</span>
      <span>${escapeHtml(formatBytes(toNumber(tableSizes.freelistBytes)))} free</span>
      <span>dbstat exact</span>
    </div>
    <div class="admin-storage-diagnostic-grid" role="table" aria-label="Exact runtime database table allocation">
      <div class="admin-storage-diagnostic-row admin-storage-diagnostic-head" role="row">
        <span role="columnheader">Table</span>
        <span role="columnheader">Table bytes</span>
        <span role="columnheader">Index bytes</span>
        <span role="columnheader">Total</span>
        <span role="columnheader">Share</span>
      </div>
      ${rows}${otherRow}
    </div>
  </div>`;
}

function renderJobSources(jobSources, formatBytes) {
  if (!jobSources || typeof jobSources !== "object") return "";
  const rows = (Array.isArray(jobSources.byGeneration) ? jobSources.byGeneration : []).map(row => {
    const current = row?.isCurrent
      ? ' <span class="admin-storage-diagnostic-current">current</span>'
      : "";
    return `<div class="admin-storage-diagnostic-row admin-storage-diagnostic-generation-row" role="row">
      <span class="admin-storage-diagnostic-name" role="cell">${escapeHtml(generationLabel(row?.generation))}${current}</span>
      <span role="cell">${formatCount(row?.rowCount)}</span>
      <span role="cell">${escapeHtml(formatBytes(toNumber(row?.payloadBytes)))}</span>
      <span role="cell">${escapeHtml(formatBytes(toNumber(row?.contentBytes)))}</span>
      <span role="cell">${escapeHtml(formatTime(row?.lastUpdatedAt))}</span>
    </div>`;
  }).join("");
  const tableBytes = jobSources.tableBytes;
  return `<div class="admin-storage-diagnostic-section">
    <h4>job_sources by feed generation</h4>
    <div class="admin-storage-diagnostic-meta">
      <span>${formatCount(jobSources.rowCount)} rows</span>
      <span>${formatCount(jobSources.generationCount)} generations</span>
      <span>${escapeHtml(formatBytes(toNumber(jobSources.payloadBytes)))} payload</span>
      <span>${escapeHtml(formatBytes(toNumber(jobSources.contentBytes)))} stored text</span>
      ${tableBytes === null || tableBytes === undefined
        ? "<span>table allocation unavailable</span>"
        : `<span>${escapeHtml(formatBytes(toNumber(tableBytes)))} allocated</span>`}
    </div>
    <div class="admin-storage-diagnostic-grid" role="table" aria-label="job_sources rows by feed generation">
      <div class="admin-storage-diagnostic-row admin-storage-diagnostic-generation-row admin-storage-diagnostic-head" role="row">
        <span role="columnheader">Generation</span>
        <span role="columnheader">Rows</span>
        <span role="columnheader">Payload</span>
        <span role="columnheader">Stored text</span>
        <span role="columnheader">Last updated</span>
      </div>
      ${rows || "<div class=\"admin-storage-diagnostic-note\">No job_sources rows were found.</div>"}
    </div>
    <p class="admin-storage-diagnostic-note">Payload is the stored JSON text. Stored text counts every text-affinity column in job_sources; these row and byte totals are exact SQLite content counts, not sampled estimates.</p>
  </div>`;
}

export function renderStorageDiagnosticHtml(diagnostic, formatBytes) {
  const format = typeof formatBytes === "function" ? formatBytes : value => String(value);
  if (!diagnostic || typeof diagnostic !== "object" || diagnostic.status === "idle") {
    return '<div class="admin-storage-tables-empty">Run Quick check for generation row counts, or Measure exact sizes for dbstat table allocation.</div>';
  }
  if (diagnostic.status === "failed") {
    return `<div class="admin-storage-tables-empty">Storage diagnostic failed: ${escapeHtml(String(diagnostic.error || "unknown error"))}</div>`;
  }
  if (diagnostic.status === "cancelled") {
    return `<div class="admin-storage-tables-empty">Storage diagnostic cancelled.</div>`;
  }
  if (diagnostic.status === "running" || diagnostic.status === "cancelling") {
    return renderProgress(diagnostic, format);
  }
  if (diagnostic.mode === "quick") {
    return renderQuickDiagnostic(diagnostic, format);
  }
  return `
    ${renderMeta(diagnostic, format)}
    ${Array.isArray(diagnostic.warnings) && diagnostic.warnings.length
      ? `<p class="admin-storage-diagnostic-warning">${diagnostic.warnings.map(warning => escapeHtml(String(warning))).join(" ")}</p>`
      : ""}
    ${renderTableAllocation(diagnostic.tableSizes, format)}
    ${renderJobSources(diagnostic.jobSources, format)}
  `;
}
