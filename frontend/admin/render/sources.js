import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js?v=6";

function formatCompactNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number.toLocaleString() : "0";
}

function renderCandidateReviewRows(rows, limit = 5) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return '<div class="no-results">No candidates in this lane.</div>';
  }
  return rows.slice(0, Math.max(1, limit)).map(row => {
    const name = escapeHtml(String(row?.name || row?.sourceIdentity || "Unnamed source"));
    const recommendation = escapeHtml(String(row?.promotionRecommendation || "review").replaceAll("_", " "));
    const provider = row?.providerFamily ? ` · ${escapeHtml(String(row.providerFamily))}` : "";
    const error = row?.lastProbeError ? ` · ${escapeHtml(String(row.lastProbeError))}` : "";
    return `
      <div class="admin-source-review-row">
        <strong>${name}</strong>
        <span>${escapeHtml(String(row?.adapter || "unknown"))}${provider}</span>
        <span>${formatCompactNumber(row?.jobsFound)} jobs · score ${formatCompactNumber(row?.rankScore)} · ${recommendation}${error}</span>
      </div>
    `;
  }).join("");
}

function renderProviderMigrationRows(rows, limit = 5) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return '<div class="no-results">No candidates in this lane.</div>';
  }
  return rows.slice(0, Math.max(1, limit)).map(row => {
    const name = escapeHtml(String(row?.name || row?.sourceIdentity || "Unnamed source"));
    const provider = row?.detectedProviderFamily
      ? ` &middot; ${escapeHtml(String(row.detectedProviderFamily))}`
      : "";
    const action = escapeHtml(String(row?.recommendedAction || "review").replaceAll("_", " "));
    const existing = row?.existingProviderSourceState
      ? ` &middot; ${escapeHtml(String(row.existingProviderSourceState))}`
      : "";
    return `
      <div class="admin-source-review-row">
        <strong>${name}</strong>
        <span>${escapeHtml(String(row?.currentAdapter || row?.adapter || "unknown"))}${provider}${existing}</span>
        <span>confidence ${formatCompactNumber(row?.migrationConfidence)} &middot; ${action}</span>
      </div>
    `;
  }).join("");
}

function renderReviewLaneDetails(title, rows, renderRows, { open = false, laneKey = "", limit = 5, expandableLanes = false } = {}) {
  const count = Array.isArray(rows) ? rows.length : 0;
  if (!count) {
    return `
      <details class="admin-source-review-lane-details"${open ? " open" : ""}>
        <summary>
          <span>${escapeHtml(title)}</span>
          <span class="muted">0 shown</span>
        </summary>
        <div class="admin-source-review-lane-body">
          ${renderRows(rows)}
        </div>
      </details>
    `;
  }
  // ponytail: per-lane +10 expansion kept in panel dataset; server caps lanes anyway
  const shown = Math.min(count, Math.max(1, limit));
  const moreButton = shown < count && laneKey && expandableLanes
    ? `
      <button
        type="button"
        class="btn back-btn admin-discovery-lane-more-btn"
        data-discovery-lane-key="${escapeHtml(laneKey)}"
      >Show 10 more (${(count - shown).toLocaleString()} left)</button>
    `
    : "";
  return `
    <details class="admin-source-review-lane-details"${open ? " open" : ""}>
      <summary>
        <span>${escapeHtml(title)}</span>
        <span class="muted">showing ${shown.toLocaleString()} of ${count.toLocaleString()}</span>
      </summary>
      <div class="admin-source-review-lane-body">
        ${renderRows(rows, limit)}
        ${moreButton}
      </div>
    </details>
  `;
}

export function renderDiscoveryCandidateReviewHtml(candidateReview, options = {}) {
  const review = candidateReview && typeof candidateReview === "object" && !Array.isArray(candidateReview)
    ? candidateReview
    : {};
  const total = Number(review.totalCandidates || 0);
  if (!total) {
    return options?.showEmpty
      ? '<div class="no-results">No discovery review evidence loaded yet.</div>'
      : "";
  }
  const lanes = [
    ["Top candidates", review.topCandidates],
    ["Provider-backed", review.providerBackedCandidates],
    ["Jobs found", review.candidatesWithJobs],
    ["Duplicates", review.duplicateCandidates],
    ["Hidden/deferred", review.hiddenOrDeferredCandidates],
    ["Needs browser probe", review.needsBrowserProbeCandidates],
    ["Likely reject/noise", review.likelyRejectCandidates]
  ];
  const laneLimits = options?.laneLimits && typeof options.laneLimits === "object" ? options.laneLimits : {};
  const laneLimit = key => Number(laneLimits[key] || 5);
  const counts = review.recommendationCounts && typeof review.recommendationCounts === "object"
    ? Object.entries(review.recommendationCounts)
      .map(([key, value]) => `${escapeHtml(String(key).replaceAll("_", " "))}: ${formatCompactNumber(value)}`)
      .join(" · ")
    : "";
  const migration = review.providerMigration && typeof review.providerMigration === "object" && !Array.isArray(review.providerMigration)
    ? review.providerMigration
    : {};
  const migrationTotal = Number(migration.totalCandidates || 0);
  const migrationLanes = [
    ["Staged provider candidates", migration.stagedProviderCandidates],
    ["Provider migration candidates", migration.providerMigrationCandidates],
    ["Already covered by provider", migration.alreadyCoveredByProvider],
    ["Add provider source candidates", migration.addProviderSourceCandidates],
    ["Unsupported provider candidates", migration.unsupportedProviderCandidates],
    ["Needs probe", migration.needsProbeCandidates],
    ["Keep static / insufficient evidence", migration.keepStaticOrInsufficientEvidence]
  ];
  return `
    <section class="admin-source-review-panel" aria-label="Discovery candidate review quality">
      <h4>Discovery Review Quality</h4>
      <p class="muted">Candidates ${formatCompactNumber(total)}${counts ? ` · ${counts}` : ""}</p>
      <div class="admin-source-review-disclosures">
        ${lanes.map(([title, rows], index) => renderReviewLaneDetails(
          title,
          rows,
          renderCandidateReviewRows,
          {
            open: index === 0,
            laneKey: `lane-${index}`,
            limit: laneLimit(`lane-${index}`),
            expandableLanes: options?.expandableLanes === true
          }
        )).join("")}
      </div>
      ${migrationTotal ? `
        <details class="admin-source-review-section-details">
          <summary>
            <span>Provider Migration Advisory</span>
            <span class="muted">${formatCompactNumber(migrationTotal)} candidates</span>
          </summary>
          <p class="muted">Read-only migration evidence for ${formatCompactNumber(migrationTotal)} candidates.</p>
          <div class="admin-source-review-disclosures">
            ${migrationLanes.map(([title, rows], index) => renderReviewLaneDetails(
              title,
              rows,
              renderProviderMigrationRows,
              {
                laneKey: `mig-${index}`,
                limit: laneLimit(`mig-${index}`),
                expandableLanes: options?.expandableLanes === true
              }
            )).join("")}
          </div>
        </details>
      ` : ""}
    </section>
  `;
}

export function renderSourcesTableHtml(
  rows,
  mode,
  formatSourceJobsFound,
  resolveSourceStatus,
  resolveSourceApprovalStatus,
  options = {}
) {
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
  const rowHeightPx = Math.max(1, Number(options?.rowHeightPx || 52));
  const virtual = Boolean(options?.virtual);
  const totalRows = rows.length;
  const startIndex = virtual
    ? Math.min(Math.max(0, Number(options?.startIndex || 0)), Math.max(0, totalRows - 1))
    : 0;
  const requestedEndIndex = virtual
    ? Number(options?.endIndex || totalRows)
    : totalRows;
  const endIndex = virtual
    ? Math.min(totalRows, Math.max(startIndex + 1, requestedEndIndex))
    : totalRows;
  const visibleRows = virtual ? rows.slice(startIndex, endIndex) : rows;
  const topSpacerHeight = virtual ? startIndex * rowHeightPx : 0;
  const bottomSpacerHeight = virtual ? Math.max(0, totalRows - endIndex) * rowHeightPx : 0;
  const selectedSourceKeys = options?.selectedSourceKeys instanceof Set
    ? options.selectedSourceKeys
    : new Set(Array.isArray(options?.selectedSourceKeys) ? options.selectedSourceKeys : []);
  const selectedSourceIds = options?.selectedSourceIds instanceof Set
    ? options.selectedSourceIds
    : new Set(Array.isArray(options?.selectedSourceIds) ? options.selectedSourceIds : []);

  function buildSourceStatusTitle(row, normalizedStatus, statusErrorDetail) {
    if (normalizedStatus === "error" && statusErrorDetail) {
      return tooltipAttrs(`Error: ${statusErrorDetail}`);
    }
    if (normalizedStatus === "warning") {
      const warningReason = String(
        row?.warningReason
        || row?._lastWarning
        || row?.lastWarning
        || row?.warning
        || ""
      ).trim();
      if (warningReason) {
        return tooltipAttrs(`Warning: ${warningReason}`);
      }
      if (statusErrorDetail) {
        return tooltipAttrs(`Warning: ${statusErrorDetail}`);
      }
      if (String(row?.lastProbedAt || "").trim()) {
        return tooltipAttrs("Warning: source was probed, but no confirmed healthy check result is available yet.");
      }
      return tooltipAttrs("Warning: source needs review, but no detailed warning reason was recorded.");
    }
    if (normalizedStatus === "not_run" || normalizedStatus === "n/a") {
      return tooltipAttrs("Not run yet: no source check or fetch result has been recorded.");
    }
    if (normalizedStatus !== "excluded") {
      return "";
    }
    const reason = String(
      row?.exclusionReason
      || row?._lastError
      || row?.error
      || row?.cacheDecision
      || ""
    ).trim();
    if (!reason) {
      return "";
    }
    return tooltipAttrs(`Excluded: ${reason}`);
  }

  return `
    <div class="jobs-table-header">
      <div class="admin-row-header admin-source-row-header">
        <div>${leadHeader}</div>
        <div>Name</div>
        <div>Adapter</div>
        <div>Studio</div>
        <div>Status</div>
        <div>Jobs</div>
        <div>Approval</div>
      </div>
    </div>
    <div class="jobs-table-body admin-source-table-body" data-source-mode="${escapeHtml(mode)}" data-total-rows="${totalRows}"${virtual ? ` data-virtualized="true" data-window-start="${startIndex}" data-window-end="${endIndex}"` : ""}>
      ${virtual && topSpacerHeight > 0 ? `<div class="admin-source-virtual-spacer" style="height: ${topSpacerHeight}px;"></div>` : ""}
      ${visibleRows.map((row, visibleIndex) => {
        const rowIndex = startIndex + visibleIndex;
        const sourceIdRaw = String(row.id || "").trim();
        const sourceId = escapeHtml(sourceIdRaw);
        const sourceUrlRaw = String(
          row.listing_url
          || row.api_url
          || row.feed_url
          || row.board_url
          || (Array.isArray(row.pages) ? (row.pages[0] || "") : "")
          || ""
        );
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
        const statusTitle = buildSourceStatusTitle(row, normalizedStatus, statusErrorDetail);
        const statusClass = normalizedStatus === "error"
          ? "critical"
          : normalizedStatus === "excluded"
            ? "warning"
            : normalizedStatus === "warning" || normalizedStatus === "not_run" || normalizedStatus === "n/a"
              ? "warning"
              : "healthy";
        const jobsFound = formatSourceJobsFound(row);
        const approvalStatus = typeof resolveSourceApprovalStatus === "function"
          ? resolveSourceApprovalStatus(row, mode)
          : null;
        const approvalLabel = escapeHtml(String(approvalStatus?.label || ""));
        const approvalTone = String(approvalStatus?.tone || "warning").toLowerCase();
        const approvalClass = approvalTone === "critical"
          ? "critical"
          : approvalTone === "healthy"
            ? "healthy"
            : "warning";
        const approvalTitleRaw = String(approvalStatus?.title || approvalStatus?.label || "").trim();
        const approvalTitle = tooltipAttrs(approvalTitleRaw);
        const sourceUrl = escapeHtml(sourceUrlRaw);
        const selectedKey = sourceIdRaw || `|${sourceUrlRaw}`;
        const checkedAttr = selectedSourceKeys.has(selectedKey) || (sourceIdRaw && selectedSourceIds.has(sourceIdRaw))
          ? " checked"
          : "";
        const sourceIdTitle = sourceIdRaw || "missing source id";
        const sourceIdAria = escapeHtml(`Source ID: ${sourceIdRaw || "missing source id"}`);
        const idIconHtml = `<span class="admin-source-id-inline"${tooltipAttrs(sourceIdTitle)} aria-label="${sourceIdAria}">i</span>`;
        const leadCell = isPending
          ? `<span class="admin-select-cell-inner"><input type="checkbox" class="pending-source-checkbox" data-ui="source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}" data-source-row-index="${rowIndex}"${checkedAttr}>${idIconHtml}</span>`
          : isRejected
            ? `<span class="admin-select-cell-inner"><input type="checkbox" class="rejected-source-checkbox" data-ui="source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}" data-source-row-index="${rowIndex}"${checkedAttr}>${idIconHtml}</span>`
            : isActive
              ? `<span class="admin-select-cell-inner"><input type="checkbox" class="active-source-checkbox" data-ui="source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}" data-source-row-index="${rowIndex}"${checkedAttr}>${idIconHtml}</span>`
              : `<span class="muted">N/A</span>`;
        return `
          <div class="admin-user-row admin-source-row" data-source-row-index="${rowIndex}">
            <div class="admin-cell" data-label="${leadHeader}">${leadCell}</div>
            <div class="admin-cell" data-label="Name">${name}</div>
            <div class="admin-cell" data-label="Adapter">${adapter}</div>
            <div class="admin-cell" data-label="Studio">${studio}</div>
            <div class="admin-cell" data-label="Status"><span class="admin-status-chip ${statusClass}"${statusTitle}>${status}</span></div>
            <div class="admin-cell" data-label="Jobs">${jobsFound}</div>
            <div class="admin-cell" data-label="Approval"><span class="admin-status-chip ${approvalClass}"${approvalTitle}>${approvalLabel}</span></div>
          </div>
        `;
      }).join("")}
      ${virtual && bottomSpacerHeight > 0 ? `<div class="admin-source-virtual-spacer" style="height: ${bottomSpacerHeight}px;"></div>` : ""}
    </div>
  `;
}
