import { escapeHtml } from "../../shared/ui/index.js";

const TABLE_LABELS = Object.freeze({
  job_feed_state: "Current feed state",
  job_feed_generation_stats: "Saved version row counts",
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

function formatRatio(fraction) {
  if (fraction === null || fraction === undefined) return "—";
  const number = Number(fraction);
  if (!Number.isFinite(number) || number < 0) return "—";
  if (number === 0) return "0%";
  if (number < 0.001) return "<0.1%";
  return `${(number * 100).toFixed(1)}%`;
}

function compactionSuppressionText(compaction, format) {
  const space = compaction && typeof compaction === "object" ? compaction.space : null;
  if (!space || typeof space !== "object") return "";
  if (space.largeAllocatedLowFreelist !== true || space.vacuumDueBySize !== false) return "";
  const free = format(toNumber(space.freelistBytes));
  if (space.vacuumBlockedBy === "freelist-bytes") {
    return `VACUUM is not due: ${free} free is below the ${format(toNumber(compaction.freelistThresholdBytes))} threshold.`;
  }
  if (space.vacuumBlockedBy !== "freelist-ratio") return "";
  const allocated = format(toNumber(space.allocatedBytes));
  return `VACUUM is suppressed: ${allocated} allocated, ${free} free (${formatRatio(space.observedFreelistRatio)} freelist), below the ${formatRatio(space.effectiveFreelistRatio)} large-database threshold.`;
}

function renderCompactionSuppressionHtml(text) {
  if (!text) return "";
  return `<div class="admin-storage-maintenance-progress is-idle" data-maintenance-phase="compaction-not-due" data-maintenance-running="false" data-compaction-suppressed="true">
    <div class="admin-storage-maintenance-progress-head">
      <strong>Compaction not due</strong>
      <span>VACUUM suppressed</span>
    </div>
    <div class="admin-storage-maintenance-progress-meta"><span>${escapeHtml(text)}</span></div>
  </div>`;
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

export function storageMaintenanceFollowUpScheduled(status) {
  if (!status || typeof status !== "object") return false;
  if (String(status.status || "") === "failed") return false;
  if (status.followUpScheduled === true) return true;
  if (status.completionState === "follow-up-scheduled") return true;
  return status.timerScheduled === true
    && status.done === false
    && String(status.status || "") === "ok";
}

export function storageMaintenanceRetryScheduled(status) {
  if (!status || typeof status !== "object") return false;
  if (status.retryScheduled === true) return true;
  if (status.completionState === "retry-scheduled") return true;
  return status.timerScheduled === true && String(status.status || "") === "failed";
}

export function storageMaintenancePhaseLabel(phase) {
  const labels = {
    starting: "Preparing maintenance",
    counting: "Counting saved-version rows",
    "prune-source-runs": "Removing old fetch history",
    "prune-event-history": "Pruning task and sync history",
    "compact-archived-payloads": "Compacting archived payloads",
    "prune-job-generations": "Removing older saved versions",
    cancelling: "Stopping after the current database step",
    cancelled: "Stopped",
    compaction: "Compacting database",
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
  const format = typeof formatBytesFormatter === "function" ? formatBytesFormatter : formatBytes;
  const status = runtimeRetention && typeof runtimeRetention === "object"
    ? runtimeRetention
    : null;
  const compactionRunning = Boolean(
    compaction
      && typeof compaction === "object"
      && (
        compaction.running === true
        || String(compaction.status || "") === "running"
      )
  );
  const suppressionText = compactionSuppressionText(compaction, format);
  if (!status) {
    if (compactionRunning) {
      return `<div class="admin-storage-maintenance-progress is-running" data-maintenance-phase="compaction" data-maintenance-running="true">
        <div class="admin-storage-maintenance-progress-head">
          <strong>Compaction running</strong>
          <span>Cannot be safely interrupted</span>
        </div>
        <div class="admin-storage-maintenance-progress-meta"><span>Wait for compaction to finish. The stop control only applies to feed cleanup.</span></div>
      </div>`;
    }
    return renderCompactionSuppressionHtml(suppressionText);
  }
  const statusName = String(status.status || "");
  const cancelling = statusName === "cancelling";
  const cancelled = statusName === "cancelled";
  const running = status.running === true || statusName === "running" || cancelling;
  if (!running && !compactionRunning && statusName === "not-run") {
    return renderCompactionSuppressionHtml(suppressionText);
  }
  const phase = String(
    (compactionRunning && !running ? "compaction" : status.phase)
      || (compactionRunning ? "compaction" : status.status)
      || ""
  );
  const followUpScheduled = storageMaintenanceFollowUpScheduled(status);
  const retryScheduled = storageMaintenanceRetryScheduled(status);
  const phaseLabel = followUpScheduled
    ? "Follow-up scheduled"
    : retryScheduled
      ? "Retry scheduled"
      : phase === "prune-job-generations" && status.stage === "counting"
        ? storageMaintenancePhaseLabel("counting")
        : storageMaintenancePhaseLabel(phase);
  const elapsed = toNumber(status.elapsedMs ?? status.durationMs);
  const totalRuns = toNumber(status.totalRunCount);
  const processedRuns = toNumber(status.processedRunCount);
  const deletedRuns = toNumber(status.deletedRunCount);
  const deletedRows = toNumber(status.deletedRowCount);
  const deletedEvents = toNumber(status.deletedEventCount);
  const deletedSyncRuns = toNumber(status.deletedSyncRunCount);
  const deletedGenerations = toNumber(status.deletedGenerationCount);
  const jobGenerations = status.jobGenerations && typeof status.jobGenerations === "object"
    ? status.jobGenerations
    : {};
  const totalGenerations = toNumber(
    status.totalGenerationCount ?? jobGenerations.totalGenerationCount
  );
  const processedGenerations = toNumber(
    status.processedGenerationCount
      ?? jobGenerations.processedGenerationCount
      ?? deletedGenerations
  );
  const totalGenerationRows = toNumber(
    status.totalGenerationRowCount ?? jobGenerations.totalGenerationRowCount
  );
  const deletedGenerationRows = toNumber(
    status.deletedGenerationRowCount ?? jobGenerations.deletedGenerationRowCount
  );
  const generationRowCountKnown = status.generationRowCountKnown === true
    || jobGenerations.generationRowCountKnown === true;
  const generationPercent = generationRowCountKnown && totalGenerationRows > 0
    ? Math.min(100, Math.round((deletedGenerationRows / totalGenerationRows) * 100))
    : null;
  const details = [];

  details.push(`<span class="admin-storage-maintenance-phase">${escapeHtml(phaseLabel)}</span>`);
  if (elapsed > 0) details.push(`<span>Elapsed ${escapeHtml(formatDuration(elapsed))}</span>`);
  if (followUpScheduled) {
    const followUpInS = Math.max(0, toNumber(status.followUpInS ?? status.nextRunInS));
    details.push(
      `<span>Follow-up scheduled${followUpInS > 0 ? ` in ${escapeHtml(formatDuration(followUpInS * 1000))}` : ""}</span>`
    );
  }
  if (retryScheduled) {
    const retryInS = Math.max(0, toNumber(status.retryInS ?? status.nextRunInS));
    details.push(
      `<span>Retry scheduled${retryInS > 0 ? ` in ${escapeHtml(formatDuration(retryInS * 1000))}` : ""}</span>`
    );
  }
  if (totalRuns > 0) {
    details.push(
      `<span>${formatCount(processedRuns)} of ${formatCount(totalRuns)} runs</span>`
    );
  }
  if (deletedRuns > 0) {
    details.push(
      `<span>Removed ${formatCount(deletedRuns)} runs / ${formatCount(deletedRows)} rows</span>`
    );
  }
  if (deletedEvents > 0 || deletedSyncRuns > 0) {
    details.push(
      `<span>Task/sync rows ${formatCount(deletedEvents)} / ${formatCount(deletedSyncRuns)}</span>`
    );
  }
  if (totalGenerations > 0) {
    details.push(
      `<span>Saved versions ${formatCount(processedGenerations)} of ${formatCount(totalGenerations)}</span>`
    );
  } else if (deletedGenerations > 0) {
    details.push(`<span>Removed ${formatCount(deletedGenerations)} saved versions</span>`);
  }
  if (generationRowCountKnown && totalGenerationRows > 0) {
    const percentText = generationPercent === null ? "" : ` (${generationPercent}%)`;
    details.push(
      `<span>${formatCount(deletedGenerationRows)} of ${formatCount(totalGenerationRows)} saved-version rows${percentText}</span>`
    );
  } else if (deletedGenerationRows > 0) {
    details.push(
      `<span>${formatCount(deletedGenerationRows)} saved-version rows removed</span>`
    );
  }
  if (compactionRunning) {
    details.push("<span>Compaction is running and cannot be safely interrupted.</span>");
  } else if (compaction && String(compaction.status || "") !== "not-run" && !suppressionText) {
    const reclaimed = toNumber(compaction.reclaimedBytes);
    const compactionText = reclaimed > 0
      ? `${String(compaction.status)} · ${format(reclaimed)} reclaimed`
      : String(compaction.status);
    details.push(`<span>Compaction: ${escapeHtml(compactionText)}</span>`);
  }
  if (!running && !compactionRunning && suppressionText) {
    details.push(`<span class="admin-storage-maintenance-suppression" data-compaction-suppressed="true">${escapeHtml(suppressionText)}</span>`);
  }
  if (status.error) {
    details.push(`<span class="admin-storage-maintenance-error">${escapeHtml(String(status.error))}</span>`);
  }
  if (cancelled) {
    details.push(
      "<span>Stopped after the current database step. Already removed data stays removed.</span>"
    );
  }

  const headline = cancelling
    ? compactionRunning
      ? "Feed cleanup stopping"
      : "Maintenance stopping"
    : running
      ? "Maintenance running"
      : followUpScheduled
        ? "Maintenance batch complete"
        : statusName === "failed"
          ? "Maintenance failed"
          : statusName === "held-active-task"
            ? "Maintenance paused"
            : cancelled
              ? compactionRunning
                ? "Feed cleanup stopped"
                : "Maintenance stopped"
              : compactionRunning
                ? "Compaction running"
                : "Maintenance complete";
  const active = running || compactionRunning;
  return `<div class="admin-storage-maintenance-progress${active ? " is-running" : ""}" data-maintenance-phase="${escapeHtml(phase)}" data-maintenance-running="${active ? "true" : "false"}" data-maintenance-follow-up="${followUpScheduled || retryScheduled ? "true" : "false"}" data-maintenance-completion-state="${escapeHtml(String(status.completionState || ""))}">
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

function storageRebuildPhaseLabel(phase) {
  const labels = {
    idle: "Not started",
    queued: "Queued",
    "stopping-retention": "Stopping feed cleanup",
    "checking-space": "Checking free space",
    copying: "Copying current feed and history",
    installing: "Installing the rebuilt database",
    cancelling: "Stopping before installation",
    complete: "Complete",
    cancelled: "Stopped",
    superseded: "Superseded by a write",
    interrupted: "Interrupted by a restart",
    failed: "Failed",
    "rolled-back": "Old database restored"
  };
  return labels[String(phase || "")] || String(phase || "Not started");
}

function shortHash(value) {
  const text = String(value || "").trim();
  return text ? text.slice(0, 12) : "—";
}

export function renderStorageRebuildHtml(rebuild, formatBytesFormatter) {
  const format = typeof formatBytesFormatter === "function" ? formatBytesFormatter : formatBytes;
  if (!rebuild || typeof rebuild !== "object") {
    return `<p class="admin-storage-card-note">No current-feed rebuild has run.</p>`;
  }
  const status = String(rebuild.status || "idle");
  const running = status === "running" || status === "cancelling";
  const phaseLabel = storageRebuildPhaseLabel(rebuild.phase || status);
  const details = [];
  const generation = String(rebuild.currentGeneration || "").trim();
  if (generation) {
    details.push(
      `<span>Current feed <code class="admin-storage-table-code">${escapeHtml(generation)}</code></span>`
    );
  }
  if (toNumber(rebuild.jobRowCount) > 0 || toNumber(rebuild.sourceRowCount) > 0) {
    details.push(
      `<span>${formatCount(rebuild.jobRowCount)} jobs / ${formatCount(rebuild.sourceRowCount)} source rows</span>`
    );
  }
  if (toNumber(rebuild.stagingBytes) > 0) {
    details.push(`<span>Rebuilt copy ${escapeHtml(format(toNumber(rebuild.stagingBytes)))}</span>`);
  }
  if (toNumber(rebuild.databaseBytes) > 0) {
    details.push(`<span>Live database ${escapeHtml(format(toNumber(rebuild.databaseBytes)))}</span>`);
  }
  if (toNumber(rebuild.reclaimedBytes) > 0) {
    details.push(`<span>${escapeHtml(format(toNumber(rebuild.reclaimedBytes)))} reclaimed</span>`);
  }
  if (toNumber(rebuild.backupBytes) > 0 && rebuild.backupExists !== false) {
    details.push(
      `<span>Rollback copy ${escapeHtml(format(toNumber(rebuild.backupBytes)))}</span>`
    );
  }
  const progress = rebuild.progress && typeof rebuild.progress === "object"
    ? rebuild.progress
    : {};
  if (toNumber(progress.tablesTotal) > 0) {
    details.push(
      `<span>${formatCount(progress.tablesDone)} of ${formatCount(progress.tablesTotal)} tables</span>`
    );
  }
  if (toNumber(rebuild.elapsedMs) > 0) {
    details.push(`<span>Elapsed ${escapeHtml(formatDuration(toNumber(rebuild.elapsedMs)))}</span>`);
  }
  const validation = rebuild.validation && typeof rebuild.validation === "object"
    ? rebuild.validation
    : {};
  const verified = status === "complete" && validation.rowHash;
  if (verified) {
    details.push(
      `<span>Verified row hash <code class="admin-storage-table-code">${escapeHtml(shortHash(validation.rowHash))}</code></span>`
    );
    details.push(
      `<span>Verified source hash <code class="admin-storage-table-code">${escapeHtml(shortHash(validation.sourceHash))}</code></span>`
    );
    details.push("<span>Quick check ok; no foreign-key violations</span>");
  }
  if (rebuild.error) {
    details.push(
      `<span class="admin-storage-maintenance-error">${escapeHtml(String(rebuild.error))}</span>`
    );
  }

  const tone = status === "failed" || status === "superseded" || status === "cancelled"
    ? " is-warning"
    : verified || status === "rolled-back"
      ? " is-ok"
      : "";
  const backupNote = rebuild.backupExists
    ? "The pre-rebuild database is retained on disk. Roll back to restore it, or delete it to free the space."
    : "No pre-rebuild database is retained.";
  return `<div class="admin-storage-maintenance-progress${running ? " is-running" : ""}" data-rebuild-status="${escapeHtml(status)}" data-rebuild-running="${running ? "true" : "false"}">
    <div class="admin-storage-maintenance-progress-head">
      <strong>${escapeHtml(phaseLabel)}</strong>
      <span>${escapeHtml(status === "idle" ? "No rebuild has run" : status)}</span>
    </div>
    <div class="admin-storage-maintenance-progress-meta">${details.join("")}</div>
    <p class="admin-storage-maintenance-preview-protection${tone}">${escapeHtml(backupNote)}</p>
  </div>`;
}

export function renderStorageMaintenancePreviewHtml(preview, formatBytesFormatter) {
  if (!preview || typeof preview !== "object") {
    return renderStorageTablesMessageHtml("Maintenance preview did not return a report.");
  }
  const format = typeof formatBytesFormatter === "function" ? formatBytesFormatter : formatBytes;
  if (preview.status === "failed") {
    return renderStorageTablesMessageHtml(
      `Maintenance preview unavailable: ${String(preview.error || "the read did not run")}`
    );
  }
  if (preview.status === "skipped") {
    return renderStorageTablesMessageHtml(
      `Maintenance preview unavailable: ${String(preview.reason || "the jobs feed is not SQLite-backed")}.`
    );
  }
  if (preview.status === "loading") {
    return `<section class="admin-storage-maintenance-preview" data-maintenance-preview="loading" aria-busy="true">
      <div class="admin-storage-maintenance-preview-head">
        <strong>Cleanup preview</strong>
        <span>Read-only · in progress</span>
      </div>
      <p class="admin-storage-maintenance-preview-protection">Preparing cleanup preview…</p>
      <p class="admin-storage-card-note">Reading cached or time-bounded generation counts and bounded payload samples. Nothing is deleted.</p>
    </section>`;
  }
  const candidates = Array.isArray(preview.candidates) ? preview.candidates : [];
  const currentGeneration = String(preview.currentGeneration || "");
  const currentProtected = preview.currentFeedProtected === true && Boolean(currentGeneration);
  const countsKnown = preview.countsKnown !== false;
  const candidateRows = toNumber(preview.candidateRowCount);
  const estimatedBytes = toNumber(preview.estimatedBytes);
  const freeDiskBytes = toNumber(preview.freeDiskBytes);
  const freelistBytes = toNumber(preview.freelistBytes);
  const candidateHtml = candidates.length
    ? `<ul class="admin-storage-maintenance-candidates">${candidates.map(candidate => {
      const generation = String(candidate?.generation || "unknown");
      const jobRows = toNumber(candidate?.jobRowCount);
      const sourceRows = toNumber(candidate?.sourceRowCount);
      const countsRowKnown = candidate?.countsKnown !== false;
      const countLabel = countsRowKnown ? "" : "At least ";
      const bytesPrefix = countsRowKnown ? "~" : "≥";
      return `<li class="admin-storage-maintenance-candidate" data-generation="${escapeHtml(generation)}">
        <code class="admin-storage-table-code">${escapeHtml(generation)}</code>
        <span>${countLabel}${formatCount(jobRows)} jobs + ${formatCount(sourceRows)} source rows</span>
        <strong>${bytesPrefix}${escapeHtml(format(toNumber(candidate?.estimatedBytes)))}</strong>
      </li>`;
    }).join("")}</ul>`
    : `<p class="admin-storage-card-note">No old saved versions are currently eligible for cleanup.</p>`;
  const capNote = preview.capReached === true
    ? ` One pass removes up to ${formatCount(preview.deleteCap)} versions; run the preview again after it finishes.`
    : "";
  const countNote = countsKnown
    ? ""
    : " Legacy counts that exceed the short counting budget fall back to bounded samples; rows marked at least and bytes marked ≥ are lower bounds.";
  const rowsPrefix = countsKnown ? "" : "≥ ";
  const bytesPrefix = countsKnown ? "~" : "≥";
  const freeSpaceNote = preview.freeSpaceKnown === false
    ? " Free disk space could not be read."
    : "";
  return `<section class="admin-storage-maintenance-preview" data-maintenance-preview="read-only" data-candidate-generations="${candidates.length}">
    <div class="admin-storage-maintenance-preview-head">
      <strong>Cleanup preview</strong>
      <span>Read-only · generated ${escapeHtml(formatTime(preview.generatedAt))}</span>
    </div>
    <div class="admin-storage-maintenance-preview-stats">
      <span><strong>${formatCount(candidates.length)}</strong> candidate versions</span>
      <span><strong>${rowsPrefix}${formatCount(candidateRows)}</strong> rows</span>
      <span><strong>${bytesPrefix}${escapeHtml(format(estimatedBytes))}</strong> stored payload</span>
      <span><strong>${escapeHtml(format(freeDiskBytes))}</strong> free disk</span>
      <span><strong>${escapeHtml(format(freelistBytes))}</strong> reusable SQLite pages</span>
    </div>
    <p class="admin-storage-maintenance-preview-protection${currentProtected ? " is-ok" : " is-warning"}">${currentProtected ? `Current feed <code>${escapeHtml(currentGeneration)}</code> is protected and never included.` : "No current feed generation is published; cleanup is held."}</p>
    ${candidateHtml}
    <p class="admin-storage-card-note">Payload bytes are a bounded estimate, not a promise of file shrinkage; compaction determines what is reclaimed.${capNote}${countNote}${freeSpaceNote}</p>
  </section>`;
}
