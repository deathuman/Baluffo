import { escapeHtml } from "../../shared/ui/index.js";

const TABLE_LABELS = Object.freeze({
  jobs: "Job listings",
  job_sources: "Saved source records",
  source_runs: "Fetch history",
  task_events: "Task history",
  task_runs: "Task runs",
  sync_runs: "Sync history",
  job_feed_state: "Current feed state"
});

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function hasNumber(value) {
  return value !== null && value !== undefined && value !== "" && Number.isFinite(Number(value));
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

function phaseLabel(phase) {
  const labels = {
    queued: "Queued",
    opening: "Opening database",
    counters: "Reading page counters",
    feed_state: "Reading current feed state",
    row_bytes: "Estimating current and older row bytes",
    tables: "Measuring table allocation",
    job_sources: "Counting saved source rows",
    jobs: "Reading the current jobs count",
    finalizing: "Finalizing",
    cancelling: "Cancelling"
  };
  return labels[String(phase || "")] || String(phase || "Not started");
}

function tableLabel(name) {
  return TABLE_LABELS[name] || name || "Unknown table";
}

function generationLabel(generation) {
  return String(generation || "").trim() || "Unknown version";
}

function renderRawName(name, label) {
  return name && name !== label
    ? ` <code class="admin-storage-table-code">${escapeHtml(name)}</code>`
    : "";
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

function renderNotice(text, tone = "") {
  return `<p class="admin-storage-notice${tone ? ` ${tone}` : ""}">${escapeHtml(text)}</p>`;
}

function renderPill(text, tone = "is-neutral") {
  return `<span class="admin-storage-pill ${tone}">${escapeHtml(text)}</span>`;
}

function renderSpaceBar(current, older) {
  const measured = current + older;
  const currentWidth = measured > 0 ? (current / measured) * 100 : 0;
  const olderWidth = measured > 0 ? (older / measured) * 100 : 0;
  return `<div class="admin-storage-space-bar" role="img" aria-label="Estimated stored row bytes by feed version">
    <span class="admin-storage-space-current" style="width:${currentWidth}%"></span>
    <span class="admin-storage-space-older" style="width:${olderWidth}%"></span>
  </div>`;
}

function renderQuickFeedState(feedState, totals, formatBytes) {
  const feed = feedState && typeof feedState === "object" ? feedState : {};
  const published = Boolean(feed.currentPublishedAt || feed.currentGeneration);
  const currentValue = totals.currentMeasured ? `~${formatBytes(totals.current)}` : "Not measured";
  const currentNote = !totals.currentMeasured
    ? "row-byte sample did not complete"
    : totals.currentComplete
      ? "job and source text, excluding indexes"
      : "measured tables only; some row text is unmeasured";
  return `<section class="admin-storage-card">
    <div class="admin-storage-card-head">
      <div>
        <h3 class="admin-storage-card-title">Current feed</h3>
        <p class="admin-storage-card-copy">What the jobs page is serving right now.</p>
      </div>
      ${published ? renderPill("Published feed", "is-ok") : renderPill("No published feed", "is-warning")}
    </div>
    <div class="admin-storage-stats">
      ${renderStat("Jobs", formatCount(feed.currentRowCount), "in the current feed")}
      ${renderStat("Saved source rows", formatCount(feed.currentSourceCount), "source records behind those jobs")}
      ${renderStat("Published", formatTime(feed.currentPublishedAt), "last feed update")}
      ${renderStat("Current stored rows", currentValue, currentNote)}
    </div>
  </section>`;
}

function probeTotals(probe) {
  const tables = Array.isArray(probe?.tables) ? probe.tables : [];
  const sumMeasured = key =>
    tables.reduce((total, table) => total + (hasNumber(table?.[key]) ? toNumber(table[key]) : 0), 0);
  const tableCurrent = sumMeasured("currentBytes");
  const tableOlder = sumMeasured("oldBytes");
  const tableTotal = sumMeasured("totalBytes");
  const current = Math.max(hasNumber(probe?.currentBytes) ? toNumber(probe.currentBytes) : 0, tableCurrent);
  const older = Math.max(hasNumber(probe?.oldBytes) ? toNumber(probe.oldBytes) : 0, tableOlder);
  const total = Math.max(
    hasNumber(probe?.totalBytes) ? toNumber(probe.totalBytes) : 0,
    tableTotal,
    current + older
  );
  const currentMeasured = tables.some(table => hasNumber(table?.currentBytes))
    || (tables.length === 0 && probe?.status === "ok" && hasNumber(probe?.currentBytes));
  const olderMeasured = tables.some(table => hasNumber(table?.oldBytes))
    || (tables.length === 0 && probe?.status === "ok" && hasNumber(probe?.oldBytes));
  const totalMeasured = tables.some(table => hasNumber(table?.totalBytes))
    || (tables.length === 0 && probe?.status === "ok" && hasNumber(probe?.totalBytes));
  const currentComplete = probe?.currentComplete === true;
  const totalComplete = probe?.totalComplete === true;
  return {
    current,
    older,
    total,
    currentMeasured,
    olderMeasured,
    totalMeasured,
    currentComplete,
    totalComplete
  };
}

function renderQuickSpace(probe, formatBytes) {
  if (!probe || typeof probe !== "object") return "";
  const totals = probeTotals(probe);
  const partial = probe.status !== "ok" || !totals.totalComplete;
  const measured = totals.current + totals.older;
  const hasMeasurement = totals.currentMeasured || totals.totalMeasured;
  const showShare = totals.totalComplete && totals.olderMeasured && measured > 0;
  const olderLabel = totals.olderMeasured
    ? totals.older > 0 ? `~${formatBytes(totals.older)}` : "None detected"
    : "Not measured";
  const totalLabel = totals.totalComplete ? `~${formatBytes(totals.total)}` : "Not measured";
  const currentShare = showShare ? totals.current / measured : null;
  return `<section class="admin-storage-card">
    <div class="admin-storage-card-head">
      <div>
        <h3 class="admin-storage-card-title">Space used by saved rows</h3>
        <p class="admin-storage-card-copy">Shows whether most stored job and source text belongs to the current feed or older saved versions.</p>
      </div>
      ${renderPill(partial ? "Partial estimate" : "Estimated", partial ? "is-warning" : "is-neutral")}
    </div>
    ${partial
      ? renderNotice("This estimate covers only the tables that completed. A current-versus-older share appears only when both sides were measured. Run Quick check again when the database is quiet for a fuller picture.", "is-warning")
      : ""}
    <div class="admin-storage-stats">
      ${renderStat("Current data", totals.currentMeasured ? `~${formatBytes(totals.current)}` : "Not measured", totals.currentComplete ? "current feed rows" : "measured tables only")}
      ${renderStat("Older saved versions", olderLabel, "older feed rows")}
      ${renderStat("Total sampled", totalLabel, totals.totalComplete ? "excludes indexes" : "older rows not measured")}
    </div>
    ${showShare
      ? `${renderSpaceBar(totals.current, totals.older)}
        <div class="admin-storage-space-legend">
          <span><i class="admin-storage-legend-swatch is-current"></i>Current · ${formatPercent(currentShare)}</span>
          <span><i class="admin-storage-legend-swatch is-older"></i>Older · ${formatPercent(totals.older / measured)}</span>
        </div>`
      : hasMeasurement
        ? renderNotice("A current-versus-older share is not available because not every table completed on both sides.", "is-warning")
        : renderNotice("No usable stored-row size was returned by this check. Try Quick check again, or run a full audit for exact allocation.", "is-warning")}
    <p class="admin-storage-card-note">Stored job and source text only. Indexes, schema, and free pages are reported by the full audit.</p>
  </section>`;
}

function renderGenerationTable(title, section, rawTitle = "") {
  if (!section || typeof section !== "object") return "";
  const generations = Array.isArray(section.byGeneration) ? section.byGeneration : [];
  const rows = generations.map(row => {
    const current = row?.isCurrent
      ? renderPill("Current", "is-ok")
      : row?.isOrphan ? renderPill("Orphan", "is-warning") : "";
    return `<div class="admin-storage-table-row admin-storage-generation-row" role="row"
      data-generation-id="${escapeHtml(String(row?.generation || ""))}"
      data-row-count="${escapeHtml(String(row?.rowCount ?? ""))}">
      <span class="admin-storage-table-label" role="cell"><code class="admin-storage-table-code" title="${escapeHtml(generationLabel(row?.generation))}">${escapeHtml(generationLabel(row?.generation))}</code></span>
      <span class="admin-storage-table-cell" role="cell">${formatCount(row?.rowCount)}</span>
      <span class="admin-storage-table-cell" role="cell">${current || "—"}</span>
    </div>`;
  }).join("");
  return `<div class="admin-storage-tech-section">
    <h4>${escapeHtml(title)}${rawTitle ? ` <code class="admin-storage-table-code">${escapeHtml(rawTitle)}</code>` : ""}</h4>
    <div class="admin-storage-tech-meta">
      <span>${formatCount(section.listedRowCount)} rows listed</span>
      <span>${formatCount(section.generationCount)} versions</span>
      ${section.truncated ? "<span>largest versions only</span>" : ""}
    </div>
    <div class="admin-storage-table" role="table" aria-label="${escapeHtml(title)}">
      <div class="admin-storage-table-row admin-storage-generation-row admin-storage-table-head" role="row">
        <span role="columnheader">Feed version</span>
        <span role="columnheader">Rows</span>
        <span role="columnheader">State</span>
      </div>
      ${rows || `<div class="admin-storage-tech-empty">No rows were found.</div>`}
    </div>
    ${section.status === "partial" ? renderNotice(String(section.error || "This generation count did not finish."), "is-warning") : ""}
  </div>`;
}

function renderProbeTable(probe, formatBytes) {
  if (!probe || typeof probe !== "object") return "";
  const tables = Array.isArray(probe.tables) ? probe.tables : [];
  if (tables.length === 0) return "";
  const rows = tables.map(table => `<div class="admin-storage-table-row admin-storage-probe-row" role="row"
    data-table-name="${escapeHtml(String(table?.table || "unknown"))}">
    <span class="admin-storage-table-label" role="cell">${escapeHtml(String(table?.table || "unknown"))}</span>
    <span class="admin-storage-table-cell" role="cell">${escapeHtml(table?.currentBytes == null ? "—" : formatBytes(toNumber(table.currentBytes)))}</span>
    <span class="admin-storage-table-cell" role="cell">${escapeHtml(table?.oldBytes == null ? "—" : formatBytes(toNumber(table.oldBytes)))}</span>
    <span class="admin-storage-table-cell" role="cell">${escapeHtml(table?.totalBytes == null ? "—" : formatBytes(toNumber(table.totalBytes)))}</span>
    <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatPercent(table?.currentShare))}</span>
  </div>`).join("");
  return `<div class="admin-storage-tech-section">
    <h4>Raw row-byte sample</h4>
    <div class="admin-storage-table" role="table" aria-label="Raw stored row bytes by table">
      <div class="admin-storage-table-row admin-storage-probe-row admin-storage-table-head" role="row">
        <span role="columnheader">Table</span>
        <span role="columnheader">Current</span>
        <span role="columnheader">Older</span>
        <span role="columnheader">Total</span>
        <span role="columnheader">Current share</span>
      </div>
      ${rows}
    </div>
  </div>`;
}

function renderQuickTechnical(diagnostic, formatBytes) {
  const feed = diagnostic.feedState || {};
  const probe = diagnostic.rowBytesProbe || {};
  const warnings = Array.isArray(diagnostic.warnings) ? diagnostic.warnings : [];
  const facts = [
    renderFact("Database file", formatBytes(toNumber(diagnostic.databaseBytes))),
    renderFact("WAL", formatBytes(toNumber(diagnostic.walBytes))),
    renderFact("Page size", `${formatCount(diagnostic.pageSize)} B`),
    renderFact("Pages", formatCount(diagnostic.pageCount)),
    renderFact("Free space", formatBytes(toNumber(diagnostic.freelistBytes))),
    renderFact("Captured", formatTime(diagnostic.capturedAt)),
    renderFact("Duration", `${formatCount(diagnostic.durationMs)} ms`),
    renderFact("Run ID", diagnostic.runId || "Not recorded"),
    renderFact("Probe method", probe.method || "Not reported"),
    renderFact("Probe status", probe.status || "Not reported"),
    renderFact("Feed version", feed.currentGeneration || "Not recorded"),
    renderFact("Feed published", formatTime(feed.currentPublishedAt))
  ];
  const sections = [
    renderGenerationTable("Saved source rows by feed version", diagnostic.jobSourcesByGeneration, "job_sources"),
    renderGenerationTable("Jobs by feed version", diagnostic.jobsByGeneration, "jobs"),
    renderProbeTable(probe, formatBytes)
  ];
  return renderTechnicalDetails("Technical details", facts, sections, warnings);
}

function renderWarnings(warnings, title = "Some measurements did not finish") {
  if (!Array.isArray(warnings) || warnings.length === 0) return "";
  return `<div class="admin-storage-notice is-warning"><strong>${escapeHtml(title)}.</strong> Open Technical details for the exact messages.</div>`;
}

function renderTechnicalDetails(summary, facts = [], sections = [], warnings = []) {
  const warningsHtml = warnings.length
    ? `<div class="admin-storage-tech-section"><h4>Warnings</h4><ul class="admin-storage-warning-list">${warnings.map(warning => `<li>${escapeHtml(String(warning))}</li>`).join("")}</ul></div>`
    : "";
  return `<details class="admin-storage-technical">
    <summary>${escapeHtml(summary)}</summary>
    <div class="admin-storage-technical-body">
      ${facts.length ? `<dl class="admin-storage-facts">${facts.join("")}</dl>` : ""}
      ${sections.join("")}${warningsHtml}
      <p class="admin-storage-card-note">Raw values from the storage diagnostic API are preserved here for support and API inspection.</p>
    </div>
  </details>`;
}

function renderQuickDiagnostic(diagnostic, formatBytes) {
  const probe = diagnostic.rowBytesProbe || {};
  const totals = probeTotals(probe);
  const warnings = Array.isArray(diagnostic.warnings) ? diagnostic.warnings : [];
  const partial = diagnostic.partial === true
    || diagnostic.exactContent === false
    || probe.status !== "ok"
    || !totals.totalComplete;
  return `
    ${renderQuickFeedState(diagnostic.feedState, totals, formatBytes)}
    ${renderQuickSpace(probe, formatBytes)}
    ${partial && warnings.length === 0
      ? renderNotice("This check finished with partial results. Some measurements did not complete before the time limit.", "is-warning")
      : ""}
    ${renderWarnings(warnings)}
    ${renderQuickTechnical(diagnostic, formatBytes)}
  `;
}

function renderShareBar(fraction) {
  const width = fraction === null ? 0 : Math.min(100, Math.max(0, fraction * 100));
  return `<span class="admin-storage-table-share-cell">
    <span class="admin-storage-share-track" aria-hidden="true"><span class="admin-storage-share-fill" style="width:${width}%"></span></span>
    <span>${escapeHtml(formatPercent(fraction))}</span>
  </span>`;
}

function renderTableAllocation(tableSizes, formatBytes) {
  if (!tableSizes || tableSizes.status === "unavailable") {
    return `<section class="admin-storage-card">
      <div class="admin-storage-card-head">
        <div><h3 class="admin-storage-card-title">Table allocation</h3><p class="admin-storage-card-copy">Exact table and index page usage is unavailable in this runtime.</p></div>
      </div>
      ${renderNotice(String(tableSizes?.error || "SQLite dbstat is not available."), "is-warning")}
    </section>`;
  }
  const tables = Array.isArray(tableSizes.tables) ? tableSizes.tables : [];
  const allocated = toNumber(tableSizes.allocatedBytes);
  const rows = tables.map(table => {
    const name = String(table?.name || "unknown");
    const label = tableLabel(name);
    const bytes = toNumber(table?.bytes);
    return `<div class="admin-storage-table-row admin-storage-allocation-row" role="row"
      data-table-name="${escapeHtml(name)}"
      data-table-bytes="${bytes}"
      data-table-share="${escapeHtml(String(table?.share ?? ""))}">
      <span class="admin-storage-table-label" role="cell">${escapeHtml(label)}${renderRawName(name, label)}</span>
      <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatBytes(toNumber(table?.tableBytes)))}</span>
      <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatBytes(toNumber(table?.indexBytes)))}</span>
      <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatBytes(bytes))}</span>
      ${renderShareBar(formatShare(table, allocated))}
    </div>`;
  }).join("");
  const other = toNumber(tableSizes.otherBytes);
  const otherRow = other > 0
    ? `<div class="admin-storage-table-row admin-storage-allocation-row admin-storage-table-other" role="row" data-table-name="unattributed" data-table-bytes="${other}">
        <span class="admin-storage-table-label" role="cell">Unassigned and free pages</span>
        <span class="admin-storage-table-cell" role="cell">—</span>
        <span class="admin-storage-table-cell" role="cell">—</span>
        <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatBytes(other))}</span>
        ${renderShareBar(formatShare({ bytes: other }, allocated))}
      </div>`
    : "";
  return `<section class="admin-storage-card">
    <div class="admin-storage-card-head">
      <div><h3 class="admin-storage-card-title">Table allocation</h3><p class="admin-storage-card-copy">Exact page usage from SQLite dbstat, including indexes.</p></div>
      ${renderPill("Exact", "is-ok")}
    </div>
    <div class="admin-storage-stats">
      ${renderStat("Allocated", formatBytes(allocated))}
      ${renderStat("Free pages", formatBytes(toNumber(tableSizes.freelistBytes)))}
    </div>
    <div class="admin-storage-table" role="table" aria-label="Exact runtime database table allocation">
      <div class="admin-storage-table-row admin-storage-allocation-row admin-storage-table-head" role="row">
        <span role="columnheader">Table</span>
        <span role="columnheader">Table data</span>
        <span role="columnheader">Indexes</span>
        <span role="columnheader">Total</span>
        <span role="columnheader">Share</span>
      </div>
      ${rows}${otherRow}
    </div>
  </section>`;
}

function renderJobSources(jobSources, formatBytes) {
  if (!jobSources || typeof jobSources !== "object") return "";
  const rows = (Array.isArray(jobSources.byGeneration) ? jobSources.byGeneration : []).map(row => {
    const current = row?.isCurrent ? renderPill("Current", "is-ok") : "";
    return `<div class="admin-storage-table-row admin-storage-generation-row admin-storage-source-generation-row" role="row"
      data-generation-id="${escapeHtml(String(row?.generation || ""))}"
      data-row-count="${escapeHtml(String(row?.rowCount ?? ""))}">
      <span class="admin-storage-table-label" role="cell"><code class="admin-storage-table-code" title="${escapeHtml(generationLabel(row?.generation))}">${escapeHtml(generationLabel(row?.generation))}</code>${current}</span>
      <span class="admin-storage-table-cell" role="cell">${formatCount(row?.rowCount)}</span>
      <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatBytes(toNumber(row?.payloadBytes)))}</span>
      <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatBytes(toNumber(row?.contentBytes)))}</span>
      <span class="admin-storage-table-cell" role="cell">${escapeHtml(formatTime(row?.lastUpdatedAt))}</span>
    </div>`;
  }).join("");
  return `<section class="admin-storage-card">
    <div class="admin-storage-card-head">
      <div><h3 class="admin-storage-card-title">Saved source rows by feed version</h3><p class="admin-storage-card-copy">Exact row and stored-text totals for each saved version of the source bundle.</p></div>
    </div>
    <div class="admin-storage-stats">
      ${renderStat("Rows", formatCount(jobSources.rowCount))}
      ${renderStat("Versions", formatCount(jobSources.generationCount))}
      ${renderStat("Stored JSON", formatBytes(toNumber(jobSources.payloadBytes)))}
      ${renderStat("Stored text", formatBytes(toNumber(jobSources.contentBytes)))}
    </div>
    <div class="admin-storage-table" role="table" aria-label="Saved source rows by feed version">
      <div class="admin-storage-table-row admin-storage-generation-row admin-storage-source-generation-row admin-storage-table-head" role="row">
        <span role="columnheader">Feed version</span>
        <span role="columnheader">Rows</span>
        <span role="columnheader">Stored JSON</span>
        <span role="columnheader">Stored text</span>
        <span role="columnheader">Updated</span>
      </div>
      ${rows || `<div class="admin-storage-tech-empty">No saved source rows were found.</div>`}
    </div>
    <p class="admin-storage-card-note">“Stored JSON” is the payload text. “Stored text” counts every text column; both are exact SQLite content counts, not samples.</p>
  </section>`;
}

function renderExactDiagnostic(diagnostic, formatBytes) {
  const warnings = Array.isArray(diagnostic.warnings) ? diagnostic.warnings : [];
  const facts = [
    renderFact("Database file", formatBytes(toNumber(diagnostic.databaseBytes))),
    renderFact("WAL", formatBytes(toNumber(diagnostic.walBytes))),
    renderFact("Captured", formatTime(diagnostic.capturedAt)),
    renderFact("Duration", `${formatCount(diagnostic.durationMs)} ms`),
    renderFact("Run ID", diagnostic.runId || "Not recorded"),
    renderFact("Allocation method", diagnostic.tableSizes?.status === "unavailable" ? "Unavailable" : "dbstat exact")
  ];
  return `
    ${warnings.length ? renderWarnings(warnings) : ""}
    ${renderTableAllocation(diagnostic.tableSizes, formatBytes)}
    ${renderJobSources(diagnostic.jobSources, formatBytes)}
    ${renderTechnicalDetails("Technical details", facts, [renderGenerationTable("Raw job_sources by feed generation", diagnostic.jobSources), renderGenerationTable("Raw jobs by feed generation", diagnostic.jobsByGeneration)], warnings)}
  `;
}

function renderProgress(diagnostic, formatBytes) {
  const progress = diagnostic.progress || {};
  const operations = toNumber(progress.operationCount);
  const quick = diagnostic.mode === "quick";
  return `<section class="admin-storage-card admin-storage-progress-card">
    <div class="admin-storage-card-head">
      <div>
        <h3 class="admin-storage-card-title">${quick ? "Quick check" : "Full storage audit"} in progress</h3>
        <p class="admin-storage-card-copy">${escapeHtml(phaseLabel(diagnostic.phase))}</p>
      </div>
      ${renderPill(quick ? "Checking" : "Auditing", "is-neutral")}
    </div>
    <div class="admin-storage-progress-meta">
      <span>${formatCount(diagnostic.elapsedMs)} ms elapsed</span>
      ${operations > 0 ? `<span>${formatCount(operations)} SQLite operations</span>` : ""}
      ${diagnostic.capturedAt ? `<span>started ${escapeHtml(formatTime(diagnostic.capturedAt))}</span>` : ""}
    </div>
    <p class="admin-storage-card-note">This read-only check runs in the bridge background. You can keep using Admin or cancel it.</p>
    ${renderTechnicalDetails("Technical details", [renderFact("Database file", formatBytes(toNumber(diagnostic.databaseBytes))), renderFact("Run ID", diagnostic.runId || "Not recorded")], [], [])}
  </section>`;
}

export function renderStorageDiagnosticHtml(diagnostic, formatBytes) {
  const format = typeof formatBytes === "function" ? formatBytes : value => `${Number(value || 0)} B`;
  if (!diagnostic || typeof diagnostic !== "object" || diagnostic.status === "idle") {
    return `<div class="admin-storage-state"><strong>No check has run yet.</strong><p>Run Quick check to see the current feed and how much space stored rows use. Use the full audit only when support needs exact allocation.</p></div>`;
  }
  if (diagnostic.status === "failed") {
    return `<div class="admin-storage-state"><strong>The check could not finish.</strong><p>${escapeHtml(String(diagnostic.error || "Unknown error."))}</p></div>`;
  }
  if (diagnostic.status === "cancelled") {
    return `<div class="admin-storage-state"><strong>The check was cancelled.</strong><p>Run Quick check again when you are ready.</p></div>`;
  }
  if (diagnostic.status === "running" || diagnostic.status === "cancelling") {
    return renderProgress(diagnostic, format);
  }
  if (diagnostic.mode === "quick") {
    return renderQuickDiagnostic(diagnostic, format);
  }
  return renderExactDiagnostic(diagnostic, format);
}
