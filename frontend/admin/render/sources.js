import { escapeHtml } from "../../shared/ui/index.js";

function formatCompactNumber(value) {
  const number = Number(value || 0);
  return Number.isFinite(number) ? number.toLocaleString() : "0";
}

function renderCandidateReviewRows(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    return '<div class="no-results">No candidates in this lane.</div>';
  }
  return rows.slice(0, 5).map(row => {
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

export function renderDiscoveryCandidateReviewHtml(candidateReview) {
  const review = candidateReview && typeof candidateReview === "object" && !Array.isArray(candidateReview)
    ? candidateReview
    : {};
  const total = Number(review.totalCandidates || 0);
  if (!total) {
    return "";
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
  const counts = review.recommendationCounts && typeof review.recommendationCounts === "object"
    ? Object.entries(review.recommendationCounts)
      .map(([key, value]) => `${escapeHtml(String(key).replaceAll("_", " "))}: ${formatCompactNumber(value)}`)
      .join(" · ")
    : "";
  return `
    <section class="admin-source-review-panel" aria-label="Discovery candidate review quality">
      <h4>Discovery Review Quality</h4>
      <p class="muted">Candidates ${formatCompactNumber(total)}${counts ? ` · ${counts}` : ""}</p>
      <div class="admin-source-review-grid">
        ${lanes.map(([title, rows]) => `
          <div class="admin-source-review-lane">
            <h5>${escapeHtml(title)}</h5>
            ${renderCandidateReviewRows(rows)}
          </div>
        `).join("")}
      </div>
    </section>
  `;
}

export function renderSourcesTableHtml(
  rows,
  mode,
  formatSourceJobsFound,
  resolveSourceStatus,
  resolveSourceApprovalStatus
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

  function buildSourceStatusTitle(row, normalizedStatus, statusErrorDetail) {
    if (normalizedStatus === "error" && statusErrorDetail) {
      return ` title="${escapeHtml(`Error: ${statusErrorDetail}`)}"`;
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
    return ` title="${escapeHtml(`Excluded: ${reason}`)}"`;
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
        const approvalTitle = approvalTitleRaw ? ` title="${escapeHtml(approvalTitleRaw)}"` : "";
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
          ? `<span class="admin-select-cell-inner"><input type="checkbox" class="pending-source-checkbox" data-ui="source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}">${idIconHtml}</span>`
          : isRejected
            ? `<span class="admin-select-cell-inner"><input type="checkbox" class="rejected-source-checkbox" data-ui="source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}">${idIconHtml}</span>`
            : isActive
              ? `<span class="admin-select-cell-inner"><input type="checkbox" class="active-source-checkbox" data-ui="source-checkbox" data-source-id="${sourceId}" data-source-url="${sourceUrl}">${idIconHtml}</span>`
              : `<span class="muted">N/A</span>`;
        return `
          <div class="admin-user-row admin-source-row">
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
    </div>
  `;
}
