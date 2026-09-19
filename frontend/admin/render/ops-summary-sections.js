/**
 * Admin Ops summary rendering — generic ops section builders (artifacts, failures, performance, task lane).
 *
 * Split out of ``ops-summary.js``; that module stays the thin coordinator
 * owning the five public render entrypoints.
 *
 * @module ops-summary-sections
 */

import { escapeHtml } from "../../shared/ui/index.js";
import {
  formatDateTime,
  formatDuration,
  getRunStatusChipClass
} from "./ops-shared.js";

function formatDiagnosticsCopyButton(key) {
  return `
    <button
      class="btn clear-filters-btn admin-ops-diagnostics-copy-btn"
      type="button"
      data-ops-diagnostics-copy="${escapeHtml(key)}"
      data-tooltip="Copy bounded diagnostics for this section"
    >Copy diagnostics</button>
  `;
}

function formatOpsFetcherMetricSection(section) {
  return `
    <section class="admin-ops-metrics-section admin-ops-metrics-section-${escapeHtml(section.key)}">
      <div class="admin-ops-metrics-section-head">
        <div>
          <h4>${escapeHtml(section.title)}</h4>
          <p>${escapeHtml(section.description)}</p>
        </div>
        ${section.diagnostics ? formatDiagnosticsCopyButton(section.key) : ""}
      </div>
      <div class="admin-ops-metrics-section-body">
        ${section.html}
      </div>
    </section>
  `;
}

function formatArtifactSummary(summary) {
  const entries = summary && typeof summary === "object" && !Array.isArray(summary)
    ? Object.entries(summary)
    : [];
  if (!entries.length) return "no summary";
  return entries
    .slice(0, 8)
    .map(([key, value]) => {
      if (value === null || ["string", "number", "boolean"].includes(typeof value)) {
        return `${key} ${String(value)}`;
      }
      if (Array.isArray(value)) {
        return `${key} [${value.length.toLocaleString()}]`;
      }
      if (value && typeof value === "object") {
        return `${key} {${Object.keys(value).slice(0, 4).join(", ")}}`;
      }
      return `${key} ${String(value)}`;
    })
    .join(", ");
}

function formatDiscoveryAuditArtifacts(payload = {}) {
  const artifacts = Array.isArray(payload?.artifacts) ? payload.artifacts : [];
  if (!artifacts.length) {
    return `
      <div class="admin-ops-schedule-item admin-ops-full-row">
        <strong>Discovery audit artifacts</strong>: no artifact diagnostics loaded.
        <button type="button" class="btn clear-filters-btn" data-action="refresh-discovery-audit-artifacts">Refresh artifacts</button>
      </div>
    `;
  }
  const found = artifacts.filter(row => row?.exists).length;
  const warningCount = artifacts.reduce((total, row) => total + (Array.isArray(row?.warnings) ? row.warnings.length : 0), 0);
  const rowsHtml = artifacts.map(row => {
    const warnings = Array.isArray(row?.warnings) && row.warnings.length
      ? ` warnings: ${row.warnings.map(value => escapeHtml(value)).join(", ")}`
      : "";
    return `
      <tr>
        <td>${escapeHtml(row?.name || "artifact")}</td>
        <td>${row?.exists ? "present" : "missing"}</td>
        <td>${escapeHtml(row?.pathDisplay || row?.relativePath || "")}</td>
        <td>${Number(row?.sizeBytes || 0).toLocaleString()} B</td>
        <td>${escapeHtml(row?.modifiedAt || "")}</td>
        <td>${escapeHtml(formatArtifactSummary(row?.summary || {}))}${warnings}</td>
      </tr>
    `;
  }).join("");
  return `
    <div class="admin-ops-schedule-item admin-ops-full-row">
      <strong>Discovery audit artifacts</strong>: ${found.toLocaleString()}/${artifacts.length.toLocaleString()} present, ${warningCount.toLocaleString()} warnings.
      <button type="button" class="btn clear-filters-btn" data-action="refresh-discovery-audit-artifacts">Refresh artifacts</button>
    </div>
    <div class="admin-table-shell admin-ops-full-row">
      <table class="admin-table admin-ops-audit-artifacts-table">
        <thead>
          <tr><th>Name</th><th>Status</th><th>Path</th><th>Size</th><th>Modified</th><th>Summary</th></tr>
        </thead>
        <tbody>${rowsHtml}</tbody>
      </table>
    </div>
  `;
}

function formatTaskFailureAttempts(payload = {}) {
  const fetch = payload?.fetch && typeof payload.fetch === "object" ? payload.fetch : {};
  const discovery = payload?.discovery && typeof payload.discovery === "object" ? payload.discovery : {};
  const warnings = Array.isArray(payload?.warnings) ? payload.warnings : [];
  const fetchBuckets = Array.isArray(fetch?.failureBuckets) ? fetch.failureBuckets : [];
  const discoveryBuckets = Array.isArray(discovery?.highPriorityBuckets) ? discovery.highPriorityBuckets : [];
  const warningText = warnings.length
    ? ` warnings: ${warnings.slice(0, 4).map(value => escapeHtml(value)).join(", ")}`
    : "";
  const fetchBucketText = fetchBuckets.length
    ? fetchBuckets
      .slice(0, 4)
      .map(row => `${escapeHtml(row?.key || "unknown")} ${Number(row?.count || 0).toLocaleString()}`)
      .join(", ")
    : "none";
  const discoveryBucketRowsHtml = discoveryBuckets.length
    ? discoveryBuckets.map(row => `
      <tr>
        <td>${escapeHtml(row?.key || "unknown")}</td>
        <td>${Number(row?.count || 0).toLocaleString()}</td>
        <td>${escapeHtml(String(row?.classification || "diagnostic").replaceAll("_", " "))}</td>
      </tr>
    `).join("")
    : `
      <tr>
        <td colspan="3">No high-priority discovery buckets.</td>
      </tr>
    `;
  return `
    <div class="admin-ops-schedule-item admin-ops-full-row">
      <strong>Task failure attempts</strong>: fetch hard ${Number(fetch?.hardFailureCount || 0).toLocaleString()},
      partial ${Number(fetch?.partialWarningCount || 0).toLocaleString()},
      expected cache exclusions ${Number(fetch?.expectedExclusionCount || 0).toLocaleString()};
      discovery diagnostics ${Number(discovery?.actionableDiagnosticCount || 0).toLocaleString()},
      expected negatives ${Number(discovery?.expectedNegativeCount || 0).toLocaleString()},
      expected skips ${Number(discovery?.expectedSkipCount || 0).toLocaleString()}.${warningText}
      <button type="button" class="btn clear-filters-btn" data-action="refresh-task-failure-attempts">Refresh attempts</button>
    </div>
    <div class="admin-ops-schedule-item admin-ops-full-row">
      <strong>Fetch buckets</strong>: ${fetchBucketText}
    </div>
    <div class="admin-table-shell admin-ops-full-row">
      <table class="admin-table admin-ops-task-failure-attempts-table">
        <thead>
          <tr><th>Discovery Bucket</th><th>Count</th><th>Classification</th></tr>
        </thead>
        <tbody>${discoveryBucketRowsHtml}</tbody>
      </table>
    </div>
  `;
}

function formatPerformanceTimingRows(rows = []) {
  const timingRows = Array.isArray(rows) ? rows : [];
  if (!timingRows.length) {
    return `<tr><td colspan="6">No timing samples yet.</td></tr>`;
  }
  return timingRows.slice(0, 8).map(row => `
    <tr>
      <td>${escapeHtml(row?.label || "unknown")}</td>
      <td>${Number(row?.count || 0).toLocaleString()}</td>
      <td>${formatDuration(Number(row?.p50Ms || 0))}</td>
      <td>${formatDuration(Number(row?.p95Ms || 0))}</td>
      <td>${formatDuration(Number(row?.maxMs || 0))}</td>
      <td>${Number(row?.errorCount || 0).toLocaleString()}</td>
    </tr>
  `).join("");
}

function formatPerformanceProfile(payload = {}) {
  const routes = Array.isArray(payload?.routeTimings?.routes) ? payload.routeTimings.routes : [];
  const operations = Array.isArray(payload?.operationTimings?.operations)
    ? payload.operationTimings.operations
    : [];
  const generatedAt = String(payload?.generatedAt || "").trim();
  const runtime = payload?.runtime && typeof payload.runtime === "object" ? payload.runtime : {};
  const runtimeLabel = [runtime?.runtimeMode, runtime?.appVersion]
    .map(value => String(value || "").trim())
    .filter(Boolean)
    .join(" ");
  return `
    <div class="admin-ops-schedule-item admin-ops-full-row">
      <strong>Backend performance</strong>:
      ${routes.length.toLocaleString()} route groups,
      ${operations.length.toLocaleString()} operation groups
      ${runtimeLabel ? `for ${escapeHtml(runtimeLabel)}` : ""}.
      ${generatedAt ? `Snapshot ${escapeHtml(formatDateTime(generatedAt))}.` : ""}
      <button type="button" class="btn clear-filters-btn" data-action="refresh-performance-profile">Refresh performance</button>
    </div>
    <div class="admin-table-shell admin-ops-full-row">
      <table class="admin-table admin-ops-performance-table">
        <thead>
          <tr><th>Route</th><th>Count</th><th>P50</th><th>P95</th><th>Max</th><th>Errors</th></tr>
        </thead>
        <tbody>${formatPerformanceTimingRows(routes)}</tbody>
      </table>
    </div>
    <div class="admin-table-shell admin-ops-full-row">
      <table class="admin-table admin-ops-performance-table">
        <thead>
          <tr><th>Operation</th><th>Count</th><th>P50</th><th>P95</th><th>Max</th><th>Errors</th></tr>
        </thead>
        <tbody>${formatPerformanceTimingRows(operations)}</tbody>
      </table>
    </div>
  `;
}

function formatOpsTaskLane(rows, diagnostics = null) {
  const laneRows = Array.isArray(rows) ? rows : [];
  const body = laneRows.map(row => {
    const status = String(row.lifecycleStatus || row.status || "unknown").trim();
    return `
      <div class="admin-ops-task-lane-card admin-ops-task-lane-card-${escapeHtml(row.type)}">
        <div class="admin-ops-task-lane-card-head">
          <strong>${escapeHtml(row.label)}</strong>
          <span class="admin-status-chip ${getRunStatusChipClass(status)}">${escapeHtml(status.replaceAll("_", " "))}</span>
        </div>
        <div class="admin-ops-task-lane-meta">${escapeHtml(row.hasRun ? (row.isLive ? `Running ${formatDuration(Number(row.elapsedMs || 0))}` : `Last ${formatDuration(Number(row.elapsedMs || 0))}`) : "No run yet")}</div>
        <div class="admin-ops-task-lane-summary">${escapeHtml(row.summary)}</div>
      </div>
    `;
  }).join("");
  return `
    <section class="admin-ops-task-lane" aria-label="Operations task status">
      <div class="admin-ops-task-lane-head">
        <div>
          <h4>Task Status</h4>
          <p>Compact read-only status for discovery, fetch, and sync.</p>
        </div>
        ${diagnostics ? formatDiagnosticsCopyButton("taskStatus") : ""}
      </div>
      <div class="admin-ops-task-lane-grid">${body}</div>
    </section>
  `;
}

export {
  formatOpsFetcherMetricSection,
  formatDiscoveryAuditArtifacts,
  formatTaskFailureAttempts,
  formatPerformanceProfile,
  formatOpsTaskLane
};
