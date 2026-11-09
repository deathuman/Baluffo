import { escapeHtml } from "../../shared/ui/index.js";

function toNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatCount(value) {
  return toNumber(value).toLocaleString();
}

function formatShare(share, bytes, allocatedBytes) {
  const fraction = Number(share);
  const resolved = Number.isFinite(fraction) && fraction > 0
    ? fraction
    : (allocatedBytes > 0 ? bytes / allocatedBytes : 0);
  return resolved > 0 ? `${(resolved * 100).toFixed(1)}%` : "—";
}

function formatCapturedAt(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const date = new Date(text);
  if (Number.isNaN(date.getTime())) return text;
  return date.toLocaleString();
}

export function renderStorageTablesMessageHtml(message) {
  return `<div class="admin-storage-tables-empty">${escapeHtml(message)}</div>`;
}

export function renderStorageTablesHtml(tableSizes, formatBytes) {
  const format = typeof formatBytes === "function" ? formatBytes : value => String(value);
  if (!tableSizes || typeof tableSizes !== "object") {
    return renderStorageTablesMessageHtml("Storage health did not include a table-size breakdown.");
  }
  if (tableSizes.status === "unavailable") {
    const reason = String(tableSizes.error || "the storage health read did not run");
    return renderStorageTablesMessageHtml(`Table sizes unavailable: ${reason}`);
  }

  const allocatedBytes = toNumber(tableSizes.allocatedBytes);
  const tables = Array.isArray(tableSizes.tables) ? tableSizes.tables : [];
  const rows = tables.map(table => {
    const bytes = toNumber(table?.bytes);
    const rowCount = table?.rowCount;
    const rowCountText = rowCount === null || rowCount === undefined
      ? "—"
      : `${tableSizes.estimated ? "~" : ""}${formatCount(rowCount)}`;
    const partial = table?.partial
      ? ' <span class="admin-storage-tables-partial">partial</span>'
      : "";
    return `<div class="admin-storage-tables-row" role="row">
      <span class="admin-storage-tables-name" role="cell">${escapeHtml(String(table?.name || "unknown"))}${partial}</span>
      <span role="cell">${escapeHtml(format(bytes))}</span>
      <span role="cell">${escapeHtml(formatShare(table?.share, bytes, allocatedBytes))}</span>
      <span class="admin-storage-tables-rows" role="cell">${escapeHtml(rowCountText)}</span>
    </div>`;
  }).join("");

  const otherBytes = toNumber(tableSizes.otherBytes);
  const otherRow = otherBytes > 0
    ? `<div class="admin-storage-tables-row admin-storage-tables-other" role="row">
      <span role="cell">Indexes, schema, and untracked pages</span>
      <span role="cell">${escapeHtml(format(otherBytes))}</span>
      <span role="cell">${escapeHtml(formatShare(0, otherBytes, allocatedBytes))}</span>
      <span class="admin-storage-tables-rows" role="cell">—</span>
    </div>`
    : "";

  const captured = formatCapturedAt(tableSizes.capturedAt);
  const metaParts = [
    `${escapeHtml(format(toNumber(tableSizes.databaseBytes)))} database`,
    `${escapeHtml(format(toNumber(tableSizes.freelistBytes)))} free`,
    tableSizes.estimated ? "estimated from rowid samples" : "exact dbstat measurement",
    tableSizes.durationMs === null || tableSizes.durationMs === undefined
      ? ""
      : `${formatCount(tableSizes.durationMs)} ms`,
    tableSizes.budgetMs === null || tableSizes.budgetMs === undefined
      ? ""
      : `${formatCount(tableSizes.budgetMs)} ms budget`,
    captured ? `captured ${escapeHtml(captured)}` : ""
  ].filter(Boolean);

  const notes = [];
  if (tableSizes.estimated) {
    notes.push(
      "Sizes are estimates: the rowid sample gives row counts and average row bytes, and exact dbstat totals are used only on small databases."
    );
  }
  if (tableSizes.complete === false) {
    notes.push("The time budget expired before every table was measured; completed tables are shown.");
  }
  if (tableSizes.truncated) {
    notes.push("Only the largest tables are shown; the rest are included in the pooled row.");
  }

  const tableHtml = rows
    ? `<div class="admin-storage-tables-grid" role="table" aria-label="Runtime database table sizes">
      <div class="admin-storage-tables-row admin-storage-tables-head" role="row">
        <span role="columnheader">Table</span>
        <span role="columnheader">Size</span>
        <span role="columnheader">Share</span>
        <span class="admin-storage-tables-rows" role="columnheader">Rows</span>
      </div>
      ${rows}${otherRow}
    </div>`
    : renderStorageTablesMessageHtml(
      tableSizes.complete === false
        ? "No tables were measured before the time budget expired."
        : "No tables were measured."
    );

  return `
    <div class="admin-storage-tables-meta">${metaParts.map(part => `<span>${part}</span>`).join("")}</div>
    ${tableHtml}
    ${notes.length ? `<p class="admin-storage-tables-note">${escapeHtml(notes.join(" "))}</p>` : ""}
  `;
}
