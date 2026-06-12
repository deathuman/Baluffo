/**
 * Admin Ops summary rendering — thin coordinator.
 *
 * All payload-specific formatters live in sibling leaf modules:
 * - ops-summary-dedup.js          dedup gate/audit/review/dedup-lists
 * - ops-summary-provider-static.js  provider/static disagreement rows
 * - ops-summary-source-policy.js    source-health/coverage/cleanup
 *
 * This file only owns the five public render entrypoints and a few
 * generic section builders.  No leaf module imports from here.
 */

import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js?v=6";
import {
  buildOpsFetcherDiagnosticsSections,
  buildOpsFetcherMetricSections,
  buildOpsTaskLaneRows
} from "../domain/ops-health-view-model.js?v=2";
import {
  FETCHER_FAILURE_BUCKET_LABELS,
  formatDuration,
  formatDateTime,
  getRunStatusChipClass,
  sanitizeSlowSourceName,
  stableOpsSignature
} from "./ops-shared.js";

import {
  formatDedupRiskReasonCounts,
  formatDedupOutlierReasonCounts,
  formatDedupIdentityShapeCounts,
  formatDedupReviewQueueCounts,
  formatDedupReviewQueueCauseCounts,
  formatDedupIdentityQualityCounts,
  formatDedupNonProviderIdentityProvenanceCounts,
  formatDedupGoogleSheetsBundleShapeCounts,
  formatDedupGoogleSheetsRoleBucketAuditCounts,
  formatDedupGoogleSheetsRoleBucketAuditSummary,
  formatDedupGoogleSheetsBucketIntentCounts,
  formatDedupGoogleSheetsWeakGroupingAuditCounts,
  formatDedupMergedRows,
  formatDedupOutlierRows,
  formatDedupRiskRows,
  formatDedupReviewQueueRows,
  formatDedupAuditGate,
  formatDedupAuditGateCard,
  formatDedupAuditGateExamples,
  formatDedupReviewStateSummary,
  formatCurrentRunMergeExamples,
  formatOpsMetricsDetails,
  buildDedupListsContent,
  wireDedupReviewActions
} from "./ops-summary-dedup.js";

import {
  formatProviderStaticDisagreementRows,
  formatProviderStaticTitleCompanyCollisionRows,
  formatProviderStaticDisagreementCounts,
  formatProviderStaticDisagreementGateCounts,
  formatProviderStaticDisagreementClassificationCounts,
  formatProviderStaticTitleCompanyCollisionCounts,
  formatProviderStaticTitleCompanyCollisionAuditCounts,
  visibleProviderStaticRows
} from "./ops-summary-provider-static.js";

import {
  formatSourceHealthRows,
  formatProviderCoverageRows,
  formatDynamicRedundantStaticRows,
  formatProviderStaticOverlapRows,
  formatStaticSuppressionPolicyRows,
  formatRedundantStaticProposalRows,
  formatConservativeCleanupReasonCounts,
  formatConservativeCleanupFreshnessSummary,
  formatConservativeCleanupProposalRows,
  formatConservativeCleanupBlockedRows,
  formatDedupSourceClasses
} from "./ops-summary-source-policy.js";

/** @typedef {import("../../shared/types.js").DedupIntCountMap} DedupIntCountMap */
/** @typedef {import("../../shared/types.js").DedupMergeExampleRow} DedupMergeExampleRow */
/** @typedef {import("../../shared/types.js").DedupReviewQueueRow} DedupReviewQueueRow */
/** @typedef {import("../../shared/types.js").GoogleSheetsRoleBucketAuditPayload} GoogleSheetsRoleBucketAuditPayload */
/** @typedef {import("../../shared/types.js").DedupAuditGateDetail} DedupAuditGateDetail */
/** @typedef {import("../../shared/types.js").DedupAuditGatePayload} DedupAuditGatePayload */
/** @typedef {import("../../shared/types.js").FetcherMetricsPayload} FetcherMetricsPayload */

function formatPipelineScheduleStatus(entry) {
  const interval = Number(entry?.intervalHours || 0);
  const next = formatDateTime(entry?.nextRunAt || "");
  const error = String(entry?.lastTriggerError || "").trim();
  if (!entry || Object.keys(entry).length === 0) return "unknown";
  if (!entry.enabled) return "disabled";
  if (error) return `needs attention: ${error}`;
  if (entry.pending) return "pending; waiting for idle";
  if (entry.due) return "due now";
  if (interval > 0 && next) return `every ${interval}h, next ${next}`;
  if (interval > 0) return `every ${interval}h`;
  return "enabled";
}

function formatRegistryCountBasis(summary) {
  const basis = String(summary?.countBasis || "").toLowerCase();
  if (summary?.summaryExact === true || basis === "normalized") {
    return "normalized counts";
  }
  if (summary?.summaryExact === false || basis === "storage") {
    return "storage snapshot counts";
  }
  return "registry counts";
}

function hasOwnField(object, key) {
  return Boolean(object && typeof object === "object" && Object.prototype.hasOwnProperty.call(object, key));
}

function formatPendingField(label = "Not loaded yet") {
  return `<span class="muted">${escapeHtml(label)}</span>`;
}

function formatOptionalNumber(object, key, { pending = "Not loaded yet" } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = Number(object?.[key]);
  if (!Number.isFinite(value)) return formatPendingField(pending);
  return value.toLocaleString();
}

function formatOptionalPercent(object, key, { pending = "Not loaded yet" } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = Number(object?.[key]);
  if (!Number.isFinite(value)) return formatPendingField(pending);
  return `${(value * 100).toFixed(1)}%`;
}

function formatOptionalDuration(object, key, { pending = "Not loaded yet" } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = Number(object?.[key]);
  if (!Number.isFinite(value)) return formatPendingField(pending);
  return escapeHtml(formatDuration(value));
}

function formatOptionalText(object, key, { pending = "Not loaded yet", formatter = value => String(value) } = {}) {
  if (!hasOwnField(object, key)) return formatPendingField(pending);
  const value = object?.[key];
  const text = formatter(value);
  return escapeHtml(text || pending);
}

function renderPipelineScheduleControls(pipeline) {
  const interval = Number(pipeline?.intervalHours || 24);
  const safeInterval = Number.isFinite(interval)
    ? Math.max(1, Math.min(168, Math.trunc(interval)))
    : 24;
  const enabled = Boolean(pipeline?.enabled);
  return `
    <div class="admin-ops-schedule-item admin-ops-pipeline-schedule admin-ops-full-row">
      <div class="admin-ops-pipeline-schedule-summary">
        <strong>Pipeline</strong>: ${escapeHtml(formatPipelineScheduleStatus(pipeline || {}))}
      </div>
      <div class="admin-ops-pipeline-schedule-controls" data-ui="admin-pipeline-schedule-controls">
        <label class="admin-ops-pipeline-schedule-toggle">
          <input type="checkbox" data-ui="admin-pipeline-schedule-enabled" ${enabled ? "checked" : ""}>
          <span>Enable</span>
        </label>
        <label class="admin-ops-pipeline-schedule-interval">
          <span>Every</span>
          <input type="number" min="1" max="168" step="1" value="${safeInterval}" data-ui="admin-pipeline-schedule-interval">
          <span>h</span>
        </label>
        <button type="button" class="btn clear-filters-btn" data-action="save-pipeline-schedule">Save</button>
      </div>
    </div>
  `;
}

// ── public exports ──────────────────────────────────────────────────

export function renderAdminOpsAlerts(alertsEl, alerts, handlers = {}) {
  if (!alertsEl) return;
  const canPatchInPlace = Boolean(alertsEl && alertsEl.dataset);
  const rows = Array.isArray(alerts) ? alerts : [];
  const signature = stableOpsSignature(rows.map(alert => ({
    id: String(alert?.id || ""),
    severity: String(alert?.severity || ""),
    message: String(alert?.message || ""),
    dismissible: alert?.dismissible !== false
  })));
  if (canPatchInPlace && alertsEl.dataset.opsAlertsSig === signature) return;
  if (canPatchInPlace) alertsEl.dataset.opsAlertsSig = signature;
  if (!rows.length) {
    alertsEl.innerHTML = "";
    return;
  }
  alertsEl.innerHTML = rows.map(alert => {
    const id = escapeHtml(String(alert?.id || ""));
    const severity = String(alert?.severity || "warning").toLowerCase();
    const cls = severity === "critical" ? "critical" : "warning";
    const dismissible = alert?.dismissible !== false;
    return `
      <div class="admin-alert-banner ${cls}" data-alert-id="${id}">
        <div class="admin-alert-message">${escapeHtml(String(alert?.message || id))}</div>
        ${dismissible
          ? `<button class="btn back-btn admin-alert-ack-btn" data-ui="admin-alert-ack-btn" data-alert-id="${id}" ${tooltipAttrs("Dismiss this operations alert.")}>Dismiss</button>`
          : ""}
      </div>
    `;
  }).join("");

  alertsEl.querySelectorAll(".admin-alert-ack-btn").forEach(btn => {
    btn.addEventListener("click", () => {
      if (typeof handlers.onAck === "function") {
        handlers.onAck(String(btn.dataset.alertId || ""));
      }
    });
  });
}

export function renderAdminOpsKpis(kpisEl, kpis, status) {
  if (!kpisEl) return;
  const canPatchInPlace = Boolean(kpisEl && kpisEl.dataset);
  const signature = stableOpsSignature({
    status: String(status || ""),
    sevenDayFetchSuccessRate: hasOwnField(kpis, "sevenDayFetchSuccessRate") ? kpis?.sevenDayFetchSuccessRate : "__pending__",
    failedSourceRatioLatest: hasOwnField(kpis, "failedSourceRatioLatest") ? kpis?.failedSourceRatioLatest : "__pending__",
    pendingApprovalsCount: hasOwnField(kpis, "pendingApprovalsCount") ? kpis?.pendingApprovalsCount : "__pending__",
    avgFetchDurationMs7d: hasOwnField(kpis, "avgFetchDurationMs7d") ? kpis?.avgFetchDurationMs7d : "__pending__",
    lastSuccessfulFetchAge: hasOwnField(kpis, "lastSuccessfulFetchAge") ? String(kpis?.lastSuccessfulFetchAge || "") : "__pending__",
    registrySync: kpis?.registrySync || {},
    providerCoverage: kpis?.providerCoverage || {},
    dedupReviewState: kpis?.dedupReviewState || {}
  });
  if (canPatchInPlace && kpisEl.dataset.opsKpisSig === signature) return;
  if (canPatchInPlace) kpisEl.dataset.opsKpisSig = signature;
  const registrySync = kpis?.registrySync && typeof kpis.registrySync === "object"
    ? kpis.registrySync
    : {};
  const providerCoverage = kpis?.providerCoverage && typeof kpis.providerCoverage === "object"
    ? kpis.providerCoverage
    : {};
  const dedupReviewState = kpis?.dedupReviewState && typeof kpis.dedupReviewState === "object"
    ? kpis.dedupReviewState
    : {};
  const statusClass = status === "critical" ? "critical" : status === "warning" ? "warning" : "healthy";
  const lastSyncAt = String(registrySync?.lastSyncAt || "");
  const lastSyncLabel = lastSyncAt ? formatDateTime(lastSyncAt) : (hasOwnField(registrySync, "lastSyncAt") ? "Never" : "Not loaded yet");
  const providerCoverageLoaded = hasOwnField(kpis, "providerCoverage");
  const dedupReviewStateLoaded = hasOwnField(kpis, "dedupReviewState");
  const providerCoverageSummary = providerCoverageLoaded
    ? `validated ${Number(providerCoverage?.statusCounts?.validated_provider || 0).toLocaleString()},
          probing ${Number((providerCoverage?.statusCounts?.probing || 0) + (providerCoverage?.statusCounts?.untested || 0)).toLocaleString()},
          failed/unstable ${Number((providerCoverage?.statusCounts?.failed_provider || 0) + (providerCoverage?.statusCounts?.unstable_provider || 0)).toLocaleString()},
          ready later ${Number((providerCoverage?.readyLaterProviders || []).length || 0).toLocaleString()}.
          Static sources are retained.`
    : "";
  const dedupReviewStateSummary = dedupReviewStateLoaded
    ? escapeHtml(formatDedupReviewStateSummary(dedupReviewState))
    : "";
  const providerCoverageHtml = providerCoverageLoaded
    ? `<div class="admin-ops-schedule-item admin-ops-full-row">
          <strong>Provider coverage</strong>:
          ${providerCoverageSummary}
        </div>`
    : "";
  const dedupReviewStateHtml = dedupReviewStateLoaded
    ? `<div class="admin-ops-schedule-item admin-ops-full-row">
          <strong>Dedup review-state</strong>: ${dedupReviewStateSummary}
        </div>`
    : "";
  const registryDiagnosticsHtml = `
    <details class="admin-ops-metrics-details admin-ops-registry-sync-details admin-ops-full-row">
      <summary>Registry and sync diagnostics</summary>
      <div class="admin-ops-metrics-details-body">
        <div class="admin-total-card">
          <div class="admin-total-label">Active Sources</div>
          <div class="admin-total-value">${formatOptionalNumber(registrySync, "activeCount")}</div>
        </div>
        <div class="admin-total-card">
          <div class="admin-total-label">Pending Review</div>
          <div class="admin-total-value">${formatOptionalNumber(registrySync, "pendingCount")}</div>
        </div>
        <div class="admin-ops-schedule-item admin-ops-full-row">
          <strong>Registry &amp; Sync</strong>:
          ${escapeHtml(formatRegistryCountBasis(registrySync))},
          hidden ${formatOptionalNumber(registrySync, "hiddenPendingCount")},
          deferred ${formatOptionalNumber(registrySync, "deferredPendingCount")},
          rejected local-only ${formatOptionalNumber(registrySync, "ignoredRejectedCount")},
          tombstones local-only ${formatOptionalNumber(registrySync, "ignoredTombstonedCount")}.
        </div>
        <div class="admin-ops-schedule-item admin-ops-full-row">
          <strong>Last sync</strong>:
          ${hasOwnField(registrySync, "lastSyncStatus") ? escapeHtml(String(registrySync?.lastSyncStatus || "never")) : formatPendingField()} @ ${escapeHtml(lastSyncLabel)};
          pull ${formatOptionalNumber(registrySync, "pulledCount")},
          push ${formatOptionalNumber(registrySync, "pushedCount")},
          conflicts ${formatOptionalNumber(registrySync, "conflictCount")},
          invalid rows ${formatOptionalNumber(registrySync, "invalidRowsCount")}.
        </div>
        ${providerCoverageHtml}
        ${dedupReviewStateHtml}
      </div>
    </details>
  `;
  kpisEl.innerHTML = `
    <div class="admin-total-card">
      <div class="admin-total-label">Ops Status</div>
      <div class="admin-total-value"><span class="admin-status-chip ${statusClass}">${escapeHtml(status)}</span></div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Last Successful Fetch</div>
      <div class="admin-total-value">${formatOptionalText(kpis, "lastSuccessfulFetchAge", { pending: "Loading latest fetch KPI..." })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Fetch Success (7d)</div>
      <div class="admin-total-value">${formatOptionalPercent(kpis, "sevenDayFetchSuccessRate", { pending: "Loading latest fetch KPI..." })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Avg Fetch Duration (7d)</div>
      <div class="admin-total-value">${formatOptionalDuration(kpis, "avgFetchDurationMs7d", { pending: "Loading latest fetch KPI..." })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Failed Source Ratio</div>
      <div class="admin-total-value">${formatOptionalPercent(kpis, "failedSourceRatioLatest", { pending: "Loading latest fetch KPI..." })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Pending Approvals</div>
      <div class="admin-total-value">${formatOptionalNumber(kpis, "pendingApprovalsCount")}</div>
    </div>
    ${registryDiagnosticsHtml}
  `;
}

export function renderAdminOpsSchedule(scheduleEl, schedule) {
  if (!scheduleEl) return;
  const canPatchInPlace = Boolean(scheduleEl && scheduleEl.dataset);
  const signature = stableOpsSignature({
    pipeline: schedule?.pipeline || {}
  });
  if (canPatchInPlace && scheduleEl.dataset.opsScheduleSig === signature) return;
  if (canPatchInPlace) scheduleEl.dataset.opsScheduleSig = signature;
  const pipeline = schedule?.pipeline || {};
  scheduleEl.innerHTML = `
    ${renderPipelineScheduleControls(pipeline)}
  `;
}

export function renderAdminOpsDedupLists(dedupEl, metrics, options = {}) {
  if (!dedupEl) return;
  const latest = metrics?.latestRun || {};
  const canPatchInPlace = Boolean(dedupEl && dedupEl.dataset);
  const signature = stableOpsSignature({
    dedupEvidence: latest?.dedupEvidence || {},
    dedupReviewStateSummary: latest?.dedupReviewStateSummary || {},
    dedupReviewStateReadWarning: String(latest?.dedupReviewStateReadWarning || "")
  });
  if (canPatchInPlace && dedupEl.dataset.opsDedupListsSig === signature) return;
  if (canPatchInPlace) dedupEl.dataset.opsDedupListsSig = signature;
  const content = buildDedupListsContent(metrics, options);
  dedupEl.innerHTML = content.html;
  wireDedupReviewActions(dedupEl, content.rowGroups, options?.onDedupReviewAction);
}

// ── generic section builders ────────────────────────────────────────

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

// ── main fetcher metrics panel ──────────────────────────────────────

/**
 * @param {HTMLElement|null|undefined} metricsEl
 * @param {FetcherMetricsPayload|null|undefined} metrics
 * @param {Object|null} [failureSummary]
 * @param {Object} [options]
 */
export function renderAdminOpsFetcherMetrics(metricsEl, metrics, failureSummary = null, options = {}) {
  if (!metricsEl) return;
  const hasMetricsPayload = metrics && typeof metrics === "object" && !Array.isArray(metrics);
  if (!hasMetricsPayload) {
    metricsEl.innerHTML = "";
    if (metricsEl.dataset) delete metricsEl.dataset.opsFetcherMetricsSig;
    return;
  }
  const latest = metrics?.latestRun || {};
  const history = metrics?.history || {};
  const summary = failureSummary && typeof failureSummary === "object"
    ? failureSummary
    : { topLevelFailedSources: 0, detailFailureCount: 0, buckets: [] };
  const canPatchInPlace = Boolean(metricsEl && metricsEl.dataset);
  const signature = stableOpsSignature({
    latestRun: {
      inputCount: Number(latest?.inputCount || 0),
      outputCount: Number(latest?.outputCount || 0),
      duplicateRate: Number(latest?.duplicateRate || 0),
      sourceFailureRate: Number(latest?.sourceFailureRate || 0),
      failedSources: Number(latest?.failedSources || 0),
      sourceCount: Number(latest?.sourceCount || 0),
      durationMs: Number(latest?.durationMs || 0),
      medianSourceDurationMs: Number(latest?.medianSourceDurationMs || 0),
      p95SourceDurationMs: Number(latest?.p95SourceDurationMs || 0)
    },
    history: {
      windowRuns: Number(history?.windowRuns || 0),
      medianDurationMs: Number(history?.medianDurationMs || 0),
      averageDurationMs: Number(history?.averageDurationMs || 0)
    },
    slowestSources: Array.isArray(latest?.slowestSources) ? latest.slowestSources : [],
    stageTop: Array.isArray(latest?.stageTop) ? latest.stageTop : [],
    sourceHealth: latest?.sourceHealth || {},
    dedupEvidence: latest?.dedupEvidence || {},
    dedupReviewStateSummary: latest?.dedupReviewStateSummary || {},
    dedupReviewStateReadWarning: String(latest?.dedupReviewStateReadWarning || ""),
    providerCoverage: latest?.providerCoverage || {},
    providerStaticOverlap: latest?.providerStaticOverlap || {},
    staticSuppressionPolicy: latest?.staticSuppressionPolicy || {},
    redundantStaticProposals: latest?.redundantStaticProposals || {},
    conservativeStaticCleanupProposals: latest?.conservativeStaticCleanupProposals || {},
    sourcePolicyRecommendationExport: latest?.sourcePolicyRecommendationExport || {},
    frontendPerfCounters: metrics?.frontendPerfCounters || {},
    discoveryAuditArtifacts: metrics?.discoveryAuditArtifacts || {},
    taskFailureAttempts: metrics?.taskFailureAttempts || {},
    performanceProfile: metrics?.performanceProfile || {},
    runModel: options?.runModel || {},
    includeDebugDiagnostics: options?.includeDebugDiagnostics !== false,
    debugDiagnosticsLoading: Boolean(options?.debugDiagnosticsLoading),
    failureSummary: summary
  });
  if (canPatchInPlace && metricsEl.dataset.opsFetcherMetricsSig === signature) return;
  if (canPatchInPlace) metricsEl.dataset.opsFetcherMetricsSig = signature;

  const failed = Number(latest?.failedSources || 0);
  const sourceCount = Math.max(0, Number(latest?.sourceCount || 0));
  const duplicateRate = Math.max(0, Number(latest?.duplicateRate || 0));
  const outputYieldRate = Math.max(0, Number(latest?.outputYieldRate || 0));
  const failureRate = Math.max(0, Number(latest?.sourceFailureRate || 0));
  const slowest = Array.isArray(latest?.slowestSources) ? latest.slowestSources : [];
  const stageTop = Array.isArray(latest?.stageTop) ? latest.stageTop : [];
  const highCostLowYield = Array.isArray(latest?.highCostLowYieldSources) ? latest.highCostLowYieldSources : [];
  const sourceHealth = latest?.sourceHealth && typeof latest.sourceHealth === "object" ? latest.sourceHealth : {};
  const dedupEvidence = latest?.dedupEvidence && typeof latest.dedupEvidence === "object" ? latest.dedupEvidence : {};
  const dedupReviewStateSummary = latest?.dedupReviewStateSummary && typeof latest.dedupReviewStateSummary === "object"
    ? latest.dedupReviewStateSummary
    : {};
  const dedupReviewStateReadWarning = String(latest?.dedupReviewStateReadWarning || "");
  const providerCoverage = latest?.providerCoverage && typeof latest.providerCoverage === "object" ? latest.providerCoverage : {};
  const providerStaticOverlap = latest?.providerStaticOverlap && typeof latest.providerStaticOverlap === "object" ? latest.providerStaticOverlap : {};
  const staticSuppressionPolicy = latest?.staticSuppressionPolicy && typeof latest.staticSuppressionPolicy === "object" ? latest.staticSuppressionPolicy : {};
  const redundantStaticProposals = latest?.redundantStaticProposals && typeof latest.redundantStaticProposals === "object" ? latest.redundantStaticProposals : {};
  const conservativeStaticCleanupProposals = latest?.conservativeStaticCleanupProposals && typeof latest.conservativeStaticCleanupProposals === "object" ? latest.conservativeStaticCleanupProposals : {};
  const sourcePolicyRecommendationExport = latest?.sourcePolicyRecommendationExport && typeof latest.sourcePolicyRecommendationExport === "object" ? latest.sourcePolicyRecommendationExport : {};
  const frontendPerfCounters = metrics?.frontendPerfCounters && typeof metrics.frontendPerfCounters === "object"
    ? metrics.frontendPerfCounters
    : {};
  const performanceProfile = metrics?.performanceProfile && typeof metrics.performanceProfile === "object"
    ? metrics.performanceProfile
    : {};
  const frontendPerfCounterRows = Object.entries(frontendPerfCounters)
    .filter(([, value]) => value && typeof value === "object")
    .sort((left, right) => Number(right[1]?.p95Ms || 0) - Number(left[1]?.p95Ms || 0))
    .slice(0, 8);
  const frontendPerfSummary = frontendPerfCounterRows.length
    ? frontendPerfCounterRows.map(([key, row]) => (
      `${escapeHtml(key)}: p95 ${formatDuration(Number(row?.p95Ms || 0))}, p50 ${formatDuration(Number(row?.p50Ms || 0))}, count ${Number(row?.count || 0).toLocaleString()}`
    )).join("; ")
    : "No frontend fetch/render counter samples yet.";
  const slowestSummary = slowest.length
    ? slowest
      .slice(0, 3)
      .map(row => `${sanitizeSlowSourceName(row?.name)} (${formatDuration(Number(row?.durationMs || 0))})`)
      .filter(Boolean)
      .join(" | ")
    : "No source timing data yet.";
  const slowestStageSummary = stageTop.length
    ? stageTop
      .slice(0, 3)
      .map(row => `${String(row?.stage || "unknown")} (${formatDuration(Number(row?.durationMs || 0))})`)
      .join(" | ")
    : "No stage timing data yet.";
  const highCostSummary = highCostLowYield.length
    ? highCostLowYield
      .slice(0, 3)
      .map(row => `${sanitizeSlowSourceName(row?.name)} (${formatDuration(Number(row?.durationMs || 0))}, kept ${Number(row?.keptCount || 0)})`)
      .join(" | ")
    : "No high-cost low-yield sources.";
  const bucketRows = Array.isArray(summary?.buckets) ? summary.buckets : [];
  const attentionSummary = formatSourceHealthRows(
    sourceHealth?.sourcesNeedingAttention,
    "No sources need attention.",
    { includeDuration: true }
  );
  const zeroReviewSummary = formatSourceHealthRows(
    sourceHealth?.zeroKeptNeedsReview,
    "No zero-kept sources need review.",
    { includeDuration: true }
  );
  const browserSummary = formatSourceHealthRows(
    sourceHealth?.browserFallbackRecommended,
    "No browser fallback recommendations.",
    { includeDuration: true }
  );
  const productiveSummary = formatSourceHealthRows(
    sourceHealth?.topProductiveSources,
    "No productive source ranking yet."
  );
  const dynamicRedundantSummary = formatDynamicRedundantStaticRows(
    sourceHealth?.dynamicRedundantStatic,
    "No runtime-only static suppression."
  );
  const validatedProviderSummary = formatProviderCoverageRows(
    providerCoverage?.validatedProviders,
    "No validated staged providers yet."
  );
  const failedProviderSummary = formatProviderCoverageRows(
    providerCoverage?.unstableOrFailedProviders,
    "No unstable or failed staged providers."
  );
  const reviewProviderSummary = formatProviderCoverageRows(
    providerCoverage?.needsReviewProviders,
    "No provider coverage rows need review."
  );
  const readyLaterProviderSummary = formatProviderCoverageRows(
    providerCoverage?.readyLaterProviders,
    "No providers are replacement-ready for a later slice."
  );
  const overlapAuditSummary = formatProviderStaticOverlapRows(
    providerStaticOverlap?.pairs,
    "No provider/static overlap audit pairs."
  );
  const suppressedPolicySummary = formatStaticSuppressionPolicyRows(
    staticSuppressionPolicy?.suppressedPairs,
    "No policy-suppressed pairs."
  );
  const pausedPolicySummary = formatStaticSuppressionPolicyRows(
    staticSuppressionPolicy?.pausedPairs,
    "No policy-paused pairs."
  );
  const warningPolicySummary = formatStaticSuppressionPolicyRows(
    staticSuppressionPolicy?.warningPairs,
    "No warning-suppressed pairs."
  );
  const proposalRows = Array.isArray(redundantStaticProposals?.proposals) ? redundantStaticProposals.proposals : [];
  const safeRedundantProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "safe_redundant_static"),
    "No safe redundant static proposals."
  );
  const keepStaticProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "keep_static"),
    "No keep-static proposals."
  );
  const moreHistoryProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "needs_more_history"),
    "No more-history proposals."
  );
  const reviewProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "needs_review" || row?.proposal === "provider_unstable"),
    "No review/provider-unstable proposals."
  );
  const staticOnlyProposalSummary = formatRedundantStaticProposalRows(
    proposalRows.filter(row => row?.proposal === "static_only_jobs_detected"),
    "No static-only proposals."
  );
  const cleanupProposalReadySummary = formatConservativeCleanupProposalRows(
    conservativeStaticCleanupProposals?.proposalReadyExamples || conservativeStaticCleanupProposals?.proposals,
    "No proposal-ready cleanup pairs yet."
  );
  const cleanupBlockedSummary = formatConservativeCleanupBlockedRows(
    conservativeStaticCleanupProposals?.blockedExamples || conservativeStaticCleanupProposals?.blockedCandidates,
    "No blocked cleanup candidates."
  );
  const cleanupBlockedReasonSummary = formatConservativeCleanupReasonCounts(
    conservativeStaticCleanupProposals?.blockedReasonCounts
  );
  const cleanupFreshnessSummary = formatConservativeCleanupFreshnessSummary(
    conservativeStaticCleanupProposals
  );
  const mergeReasonCounts = dedupEvidence?.mergeReasonCounts && typeof dedupEvidence.mergeReasonCounts === "object"
    ? dedupEvidence.mergeReasonCounts
    : {};
  const sourceBundleComposition = dedupEvidence?.sourceBundleComposition && typeof dedupEvidence.sourceBundleComposition === "object"
    ? dedupEvidence.sourceBundleComposition
    : {};
  const riskReasonCounts = dedupEvidence?.riskReasonCounts && typeof dedupEvidence.riskReasonCounts === "object"
    ? dedupEvidence.riskReasonCounts
    : {};
  const outlierReasonCounts = dedupEvidence?.outlierReasonCounts && typeof dedupEvidence.outlierReasonCounts === "object"
    ? dedupEvidence.outlierReasonCounts
    : {};
  const identityShapeCounts = dedupEvidence?.identityShapeCounts && typeof dedupEvidence.identityShapeCounts === "object"
    ? dedupEvidence.identityShapeCounts
    : {};
  const identityQualityCounts = dedupEvidence?.identityQualityCounts && typeof dedupEvidence.identityQualityCounts === "object"
    ? dedupEvidence.identityQualityCounts
    : {};
  const nonProviderIdentityProvenanceCounts = dedupEvidence?.nonProviderIdentityProvenanceCounts && typeof dedupEvidence.nonProviderIdentityProvenanceCounts === "object"
    ? dedupEvidence.nonProviderIdentityProvenanceCounts
    : {};
  const googleSheetsBundleShapeCounts = dedupEvidence?.googleSheetsBundleShapeCounts && typeof dedupEvidence.googleSheetsBundleShapeCounts === "object"
    ? dedupEvidence.googleSheetsBundleShapeCounts
    : {};
  const googleSheetsRoleBucketAuditCounts = dedupEvidence?.googleSheetsRoleBucketAuditCounts && typeof dedupEvidence.googleSheetsRoleBucketAuditCounts === "object"
    ? dedupEvidence.googleSheetsRoleBucketAuditCounts
    : {};
  const googleSheetsRoleBucketAudit = dedupEvidence?.googleSheetsRoleBucketAudit && typeof dedupEvidence.googleSheetsRoleBucketAudit === "object"
    ? dedupEvidence.googleSheetsRoleBucketAudit
    : {};
  const googleSheetsBucketIntentCounts = dedupEvidence?.googleSheetsBucketIntentCounts && typeof dedupEvidence.googleSheetsBucketIntentCounts === "object"
    ? dedupEvidence.googleSheetsBucketIntentCounts
    : {};
  const googleSheetsWeakGroupingAuditCounts = dedupEvidence?.googleSheetsWeakGroupingAuditCounts && typeof dedupEvidence.googleSheetsWeakGroupingAuditCounts === "object"
    ? dedupEvidence.googleSheetsWeakGroupingAuditCounts
    : {};
  const reviewQueueCounts = dedupEvidence?.reviewQueueCounts && typeof dedupEvidence.reviewQueueCounts === "object"
    ? dedupEvidence.reviewQueueCounts
    : {};
  const reviewQueueCauseCounts = dedupEvidence?.reviewQueueCauseCounts && typeof dedupEvidence.reviewQueueCauseCounts === "object"
    ? dedupEvidence.reviewQueueCauseCounts
    : {};
  const dedupAuditGate = dedupEvidence?.dedupAuditGate && typeof dedupEvidence.dedupAuditGate === "object"
    ? dedupEvidence.dedupAuditGate
    : {};
  const providerStaticDisagreementCounts = dedupEvidence?.providerStaticDisagreementCounts && typeof dedupEvidence.providerStaticDisagreementCounts === "object"
    ? dedupEvidence.providerStaticDisagreementCounts
    : {};
  const providerStaticDisagreementGateCounts = dedupEvidence?.providerStaticDisagreementGateCounts && typeof dedupEvidence.providerStaticDisagreementGateCounts === "object"
    ? dedupEvidence.providerStaticDisagreementGateCounts
    : {};
  const providerStaticDisagreementClassificationCounts = dedupEvidence?.providerStaticDisagreementClassificationCounts && typeof dedupEvidence.providerStaticDisagreementClassificationCounts === "object"
    ? dedupEvidence.providerStaticDisagreementClassificationCounts
    : {};
  const providerStaticTitleCompanyCollisionCounts = dedupEvidence?.providerStaticTitleCompanyCollisionCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionCounts
    : {};
  const providerStaticTitleCompanyCollisionAuditCounts = dedupEvidence?.providerStaticTitleCompanyCollisionAuditCounts && typeof dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts === "object"
    ? dedupEvidence.providerStaticTitleCompanyCollisionAuditCounts
    : {};
  const topMergedSummary = formatDedupMergedRows(
    dedupEvidence?.topMergedJobs,
    "No merged canonical jobs in the latest fetch report."
  );
  const topOutlierSummary = formatDedupOutlierRows(
    dedupEvidence?.topSourceBundleOutliers,
    "No carried source-bundle collision outliers in the latest fetch report."
  );
  const riskyMergeSummary = formatDedupRiskRows(
    dedupEvidence?.riskyMergeExamples,
    "No risky merge examples in the latest fetch report."
  );
  const reviewQueueSummary = formatDedupReviewQueueRows(
    dedupEvidence?.reviewQueue,
    "No dedup review queue examples in the latest fetch report."
  );
  const providerStaticDisagreementRows = Array.isArray(dedupEvidence?.providerStaticDisagreementExamples)
    ? dedupEvidence.providerStaticDisagreementExamples
    : [];
  const providerStaticTitleCompanyCollisionRows = Array.isArray(dedupEvidence?.providerStaticTitleCompanyCollisionExamples)
    ? dedupEvidence.providerStaticTitleCompanyCollisionExamples
    : [];
  const providerStaticDisagreementSummary = formatProviderStaticDisagreementRows(
    providerStaticDisagreementRows,
    "No provider/static disagreement examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStatic" }
  );
  const providerStaticTitleCompanyCollisionSummary = formatProviderStaticTitleCompanyCollisionRows(
    providerStaticTitleCompanyCollisionRows,
    "No provider/static title/company collision examples in the latest fetch report.",
    { onReviewAction: options?.onDedupReviewAction, tableKey: "providerStaticTitleCompany" }
  );
  const bucketSummaryHtml = bucketRows.length
    ? bucketRows.map(bucket => `
      <div class="admin-ops-schedule-item admin-ops-full-row">
        <strong>${escapeHtml(FETCHER_FAILURE_BUCKET_LABELS[bucket.key] || bucket.key)}</strong>
        : ${Number(bucket.count || 0).toLocaleString()}
        ${bucket.examples?.length ? ` (${escapeHtml(bucket.examples.join(" | "))})` : ""}
      </div>
    `).join("")
    : `
      <div class="admin-ops-schedule-item admin-ops-full-row">
        <strong>Failure buckets</strong>: No classified failures in the latest fetch report.
      </div>
    `;

  const runtimeSecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Slowest sources</strong>: ${escapeHtml(slowestSummary)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Slowest stages</strong>: ${escapeHtml(slowestStageSummary)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>High-cost low-yield</strong>: ${escapeHtml(highCostSummary)}</div>
  `;
  const runtimeSectionHtml = `
    <div class="admin-total-card">
      <div class="admin-total-label">Latest Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(latest?.durationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Median Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(history?.medianDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Average Runtime</div>
      <div class="admin-total-value">${formatDuration(Number(history?.averageDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Window Runs</div>
      <div class="admin-total-value">${Number(history?.windowRuns || 0).toLocaleString()}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Duplicate Rate</div>
      <div class="admin-total-value">${(duplicateRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Output Yield</div>
      <div class="admin-total-value">${(outputYieldRate * 100).toFixed(1)}%</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Median Source Time</div>
      <div class="admin-total-value">${formatDuration(Number(latest?.medianSourceDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">P95 Source Time</div>
      <div class="admin-total-value">${formatDuration(Number(latest?.p95SourceDurationMs || 0))}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Source Failures</div>
      <div class="admin-total-value">${failed.toLocaleString()} / ${sourceCount.toLocaleString()} (${(failureRate * 100).toFixed(1)}%)</div>
    </div>
    ${formatOpsMetricsDetails("Runtime diagnostics", runtimeSecondaryHtml, "admin-ops-runtime-details")}
  `;

  const failureBucketDetailsHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Failure buckets</strong></div>
    ${bucketSummaryHtml}
  `;
  const failuresSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top-level failed sources</strong>: ${Number(summary?.topLevelFailedSources || 0).toLocaleString()}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Grouped detail failures</strong>: ${Number(summary?.detailFailureCount || 0).toLocaleString()}</div>
    ${formatOpsMetricsDetails("Failure bucket details", failureBucketDetailsHtml, "admin-ops-failures-details")}
  `;

  const dedupSecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup current-run merge examples</strong>: ${formatCurrentRunMergeExamples(dedupEvidence?.currentRunMergeExamples, "No current-run merge examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static disagreements</strong>: ${escapeHtml(formatProviderStaticDisagreementCounts(providerStaticDisagreementCounts))}. Gate: ${escapeHtml(formatProviderStaticDisagreementGateCounts(providerStaticDisagreementGateCounts))}. Classifications: ${escapeHtml(formatProviderStaticDisagreementClassificationCounts(providerStaticDisagreementClassificationCounts))}. ${providerStaticDisagreementSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup provider/static title-company collisions</strong>: ${escapeHtml(formatProviderStaticTitleCompanyCollisionCounts(providerStaticTitleCompanyCollisionCounts))}. Audit: ${escapeHtml(formatProviderStaticTitleCompanyCollisionAuditCounts(providerStaticTitleCompanyCollisionAuditCounts))}. ${providerStaticTitleCompanyCollisionSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup carried bundle examples</strong>: ${formatDedupAuditGateExamples(dedupEvidence?.carriedBundleExamples, "No carried bundle examples.")}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup source composition</strong>: ${escapeHtml(formatDedupSourceClasses(sourceBundleComposition))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup risk reasons</strong>: ${escapeHtml(formatDedupRiskReasonCounts(riskReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup outlier reasons</strong>: ${escapeHtml(formatDedupOutlierReasonCounts(outlierReasonCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity shapes</strong>: ${escapeHtml(formatDedupIdentityShapeCounts(identityShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup identity quality</strong>: ${escapeHtml(formatDedupIdentityQualityCounts(identityQualityCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup non-provider provenance</strong>: ${escapeHtml(formatDedupNonProviderIdentityProvenanceCounts(nonProviderIdentityProvenanceCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bundle shapes</strong>: ${escapeHtml(formatDedupGoogleSheetsBundleShapeCounts(googleSheetsBundleShapeCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit</strong>: ${escapeHtml(formatDedupGoogleSheetsRoleBucketAuditCounts(googleSheetsRoleBucketAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets role-bucket audit summary</strong>: ${formatDedupGoogleSheetsRoleBucketAuditSummary(googleSheetsRoleBucketAudit)}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets bucket intent</strong>: ${escapeHtml(formatDedupGoogleSheetsBucketIntentCounts(googleSheetsBucketIntentCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup Google Sheets weak grouping audit</strong>: ${escapeHtml(formatDedupGoogleSheetsWeakGroupingAuditCounts(googleSheetsWeakGroupingAuditCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup action queue</strong>: ${escapeHtml(formatDedupReviewQueueCounts(reviewQueueCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup diagnostic causes</strong>: ${escapeHtml(formatDedupReviewQueueCauseCounts(reviewQueueCauseCounts))}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top merged jobs</strong>: ${topMergedSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top source-bundle outliers</strong>: ${topOutlierSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review examples</strong>: ${reviewQueueSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Risky merge examples</strong>: ${riskyMergeSummary}</div>
  `;
  const dedupSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup evidence</strong>: read-only diagnostics. Current-run merges by reason: primary URL ${Number(mergeReasonCounts?.primaryUrl || 0).toLocaleString()}, secondary key ${Number(mergeReasonCounts?.secondaryKey || 0).toLocaleString()}, known mirror pair ${Number(mergeReasonCounts?.knownMirrorPair || 0).toLocaleString()}, social key ${Number(mergeReasonCounts?.socialKey || 0).toLocaleString()}, sparse identity ${Number(mergeReasonCounts?.sparseIdentity || 0).toLocaleString()}, unknown ${Number(mergeReasonCounts?.unknown || 0).toLocaleString()}. Carried source-bundle collision rows: ${Number(dedupEvidence?.sourceBundleCollisionCount || 0).toLocaleString()}.</div>
    ${formatDedupAuditGateCard(dedupAuditGate)}
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Dedup review-state</strong>: ${escapeHtml(formatDedupReviewStateSummary(dedupReviewStateSummary, dedupReviewStateReadWarning, dedupAuditGate))}</div>
    ${formatOpsMetricsDetails("Dedup supporting diagnostics", dedupSecondaryHtml, "admin-ops-dedup-details")}
  `;

  const sourceHealthSecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Zero kept / needs review</strong>: ${zeroReviewSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Browser fallback recommended</strong>: ${browserSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Top productive sources</strong>: ${productiveSummary}</div>
  `;
  const sourceHealthSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Sources needing attention</strong>: ${attentionSummary}</div>
    ${formatOpsMetricsDetails("Source health supporting diagnostics", sourceHealthSecondaryHtml, "admin-ops-source-health-details")}
  `;

  const sourcePolicySecondaryHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Runtime-suppressed static sources</strong>: ${dynamicRedundantSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Validated staged providers</strong>: ${validatedProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Provider coverage needs review</strong>: ${reviewProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Unstable / failed providers</strong>: ${failedProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Ready later (no static mutation)</strong>: ${readyLaterProviderSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Provider/static overlap audit</strong>: safe ${Number(providerStaticOverlap?.safePairCount || 0).toLocaleString()}, needs review ${Number(providerStaticOverlap?.needsReviewPairCount || 0).toLocaleString()}, insufficient history ${Number(providerStaticOverlap?.insufficientHistoryPairCount || 0).toLocaleString()}. ${overlapAuditSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Static suppression policy</strong>: suppressed ${Number(staticSuppressionPolicy?.suppressedCount || 0).toLocaleString()}, paused ${Number(staticSuppressionPolicy?.pausedCount || 0).toLocaleString()}, warnings ${Number(staticSuppressionPolicy?.warningCount || 0).toLocaleString()}. Suppressed: ${suppressedPolicySummary} Paused: ${pausedPolicySummary} Warnings: ${warningPolicySummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Redundant static proposals</strong>: safe ${Number(redundantStaticProposals?.safeRedundantCount || 0).toLocaleString()}, keep static ${Number(redundantStaticProposals?.keepStaticCount || 0).toLocaleString()}, more history ${Number(redundantStaticProposals?.needsMoreHistoryCount || 0).toLocaleString()}, review/unstable ${Number((redundantStaticProposals?.needsReviewCount || 0) + (redundantStaticProposals?.providerUnstableCount || 0)).toLocaleString()}, static-only ${Number(redundantStaticProposals?.staticOnlyDetectedCount || 0).toLocaleString()}. Safe: ${safeRedundantProposalSummary} Keep: ${keepStaticProposalSummary} History: ${moreHistoryProposalSummary} Review: ${reviewProposalSummary} Static-only: ${staticOnlyProposalSummary}</div>
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Conservative static cleanup proposals</strong>: total candidates ${Number(conservativeStaticCleanupProposals?.totalCandidateCount || 0).toLocaleString()}, proposal-ready ${Number(conservativeStaticCleanupProposals?.proposalCount || 0).toLocaleString()}, stale ${Number(conservativeStaticCleanupProposals?.staleCount || 0).toLocaleString()}, blocked ${Number(conservativeStaticCleanupProposals?.blockedCount || 0).toLocaleString()}. Freshness: ${escapeHtml(cleanupFreshnessSummary)}. Blockers: ${escapeHtml(cleanupBlockedReasonSummary)} Ready: ${cleanupProposalReadySummary} Blocked: ${cleanupBlockedSummary}</div>
  `;
  const sourcePolicySectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Source-policy review</strong>: local review pairs ${Number(sourcePolicyRecommendationExport?.reviewStatePairCount || 0).toLocaleString()}, force-paused ${Number(sourcePolicyRecommendationExport?.manualForcePausedCount || 0).toLocaleString()}. Use the Source Policy Review queue for local, reversible actions.</div>
    ${formatOpsMetricsDetails("Source policy supporting diagnostics", sourcePolicySecondaryHtml, "admin-ops-source-policy-details")}
  `;
  const frontendPerfSectionHtml = `
    <div class="admin-ops-schedule-item admin-ops-full-row"><strong>Frontend fetch/render counters</strong>: ${frontendPerfSummary}</div>
  `;
  const performanceProfileSectionHtml = formatPerformanceProfile(performanceProfile);
  const auditArtifactsSectionHtml = formatDiscoveryAuditArtifacts(metrics?.discoveryAuditArtifacts || {});
  const taskFailureAttemptsSectionHtml = formatTaskFailureAttempts(metrics?.taskFailureAttempts || {});

  const taskLaneRows = buildOpsTaskLaneRows(options?.runModel || {});
  const diagnosticsByKey = buildOpsFetcherDiagnosticsSections({
    latest,
    history,
    failureSummary: summary,
    taskLaneRows,
    auditArtifacts: metrics?.discoveryAuditArtifacts || {},
    taskFailureAttempts: metrics?.taskFailureAttempts || {},
    performanceProfile
  });
  const taskLaneHtml = formatOpsTaskLane(taskLaneRows, diagnosticsByKey.taskStatus);
  const sectionHtmlByKey = {
    runtime: runtimeSectionHtml,
    failures: failuresSectionHtml,
    taskFailures: taskFailureAttemptsSectionHtml,
    frontendPerf: frontendPerfSectionHtml,
    performance: performanceProfileSectionHtml,
    sourceHealth: sourceHealthSectionHtml,
    sourcePolicy: sourcePolicySectionHtml,
    auditArtifacts: auditArtifactsSectionHtml
  };
  if (options?.includeDedupSection === true) {
    sectionHtmlByKey.dedup = dedupSectionHtml;
  }
  const includeDebugDiagnostics = options?.includeDebugDiagnostics !== false;
  const debugDiagnosticsHtml = `
    <details class="admin-ops-metrics-details admin-ops-debug-diagnostics admin-ops-full-row">
      <summary>Debug diagnostics</summary>
      <div class="admin-ops-metrics-details-body">
        <div class="admin-ops-schedule-item admin-ops-full-row">
          Frontend counters, route timing profiles, audit artifacts, source-policy support data, task-failure attempts, and dedup support diagnostics are not loaded by default.
          <button type="button" class="btn clear-filters-btn" data-action="load-debug-diagnostics"${options?.debugDiagnosticsLoading ? " disabled" : ""}>${options?.debugDiagnosticsLoading ? "Loading debug diagnostics..." : "Load debug diagnostics"}</button>
        </div>
      </div>
    </details>
  `;
  const sectionHtml = includeDebugDiagnostics
    ? `${taskLaneHtml}${buildOpsFetcherMetricSections(
      sectionHtmlByKey,
      diagnosticsByKey
    ).map(formatOpsFetcherMetricSection).join("")}`
    : `${taskLaneHtml}${debugDiagnosticsHtml}`;
  if (!sectionHtml.trim()) {
    metricsEl.innerHTML = "";
    return;
  }
  metricsEl.innerHTML = `
    <h4 class="admin-section-title">Fetcher Diagnostics</h4>
    <details class="admin-ops-metrics-details admin-ops-fetcher-diagnostics admin-ops-full-row">
      <summary>Fetcher diagnostics</summary>
      <div class="admin-ops-metrics-details-body admin-ops-fetcher-diagnostics-body">
        ${sectionHtml}
      </div>
    </details>
  `;

  if (typeof options?.onDedupReviewAction === "function") {
    const rowGroups = {
      providerStatic: visibleProviderStaticRows(providerStaticDisagreementRows),
      providerStaticTitleCompany: visibleProviderStaticRows(providerStaticTitleCompanyCollisionRows)
    };
    metricsEl.querySelectorAll("[data-dedup-review-action]").forEach(button => {
      button.addEventListener("click", () => {
        const action = String(button.getAttribute("data-dedup-review-action") || "");
        const tableKey = String(button.getAttribute("data-dedup-review-table") || "");
        const rowIndex = Number(button.getAttribute("data-dedup-review-row") || -1);
        const row = Array.isArray(rowGroups?.[tableKey]) ? rowGroups[tableKey][rowIndex] : null;
        if (!row || !action) return;
        options.onDedupReviewAction(row, action);
      });
    });
  }
  if (typeof options?.onCopySectionDiagnostics === "function") {
    metricsEl.querySelectorAll("[data-ops-diagnostics-copy]").forEach(button => {
      button.addEventListener("click", () => {
        const key = String(button.getAttribute("data-ops-diagnostics-copy") || "");
        const section = diagnosticsByKey[key];
        if (!section) return;
        options.onCopySectionDiagnostics(section);
      });
    });
  }
  if (typeof options?.onLoadDebugDiagnostics === "function") {
    metricsEl.querySelectorAll('[data-action="load-debug-diagnostics"]').forEach(button => {
      button.addEventListener("click", () => {
        options.onLoadDebugDiagnostics();
      });
    });
  }
  if (typeof options?.onRefreshAuditArtifacts === "function") {
    metricsEl.querySelectorAll('[data-action="refresh-discovery-audit-artifacts"]').forEach(button => {
      button.addEventListener("click", () => {
        options.onRefreshAuditArtifacts();
      });
    });
  }
  if (typeof options?.onRefreshTaskFailureAttempts === "function") {
    metricsEl.querySelectorAll('[data-action="refresh-task-failure-attempts"]').forEach(button => {
      button.addEventListener("click", () => {
        options.onRefreshTaskFailureAttempts();
      });
    });
  }
  if (typeof options?.onRefreshPerformanceProfile === "function") {
    metricsEl.querySelectorAll('[data-action="refresh-performance-profile"]').forEach(button => {
      button.addEventListener("click", () => {
        options.onRefreshPerformanceProfile();
      });
    });
  }
}
