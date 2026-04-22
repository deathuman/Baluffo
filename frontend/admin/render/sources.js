import { escapeHtml } from "../../shared/ui/index.js";

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
          </div>
        `;
      }).join("")}
    </div>
  `;
}
