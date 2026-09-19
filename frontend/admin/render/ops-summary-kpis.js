/**
 * Admin Ops summary rendering — ops alert banners, KPI cards, and registry/sync diagnostics.
 *
 * Split out of ``ops-summary.js``; that module stays the thin coordinator
 * owning the five public render entrypoints.
 *
 * @module ops-summary-kpis
 */

import {
  formatDateTime,
  stableOpsSignature
} from "./ops-shared.js";
import {
  escapeHtml,
  tooltipAttrs
} from "../../shared/ui/index.js";
import { formatDedupReviewStateSummary } from "./ops-summary-dedup.js";
import { hasOwnField, formatOptionalDuration, formatOptionalNumber, formatOptionalPercent, formatOptionalText, formatPendingField } from "./ops-summary-fields.js";

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

function renderAdminOpsAlerts(alertsEl, alerts, handlers = {}) {
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

function renderAdminOpsKpis(kpisEl, kpis, status, options = {}) {
  if (!kpisEl) return;
  const fetchKpiPendingLabel = String(options?.fetchKpiPendingLabel || "Loading latest fetch KPI...");
  const fetchKpiPendingLabels = options?.fetchKpiPendingLabels && typeof options.fetchKpiPendingLabels === "object"
    ? options.fetchKpiPendingLabels
    : {};
  const pendingLabelFor = key => String(fetchKpiPendingLabels[key] || fetchKpiPendingLabel);
  const canPatchInPlace = Boolean(kpisEl && kpisEl.dataset);
  const signature = stableOpsSignature({
    status: String(status || ""),
    fetchKpiPendingLabel,
    fetchKpiPendingLabels,
    sevenDayFetchSuccessRate: hasOwnField(kpis, "sevenDayFetchSuccessRate") ? kpis?.sevenDayFetchSuccessRate : "__pending__",
    failedSourceRatioLatest: hasOwnField(kpis, "failedSourceRatioLatest") ? kpis?.failedSourceRatioLatest : "__pending__",
    pendingSourcesCount: hasOwnField(kpis, "pendingSourcesCount") ? kpis?.pendingSourcesCount : (hasOwnField(kpis, "pendingApprovalsCount") ? kpis?.pendingApprovalsCount : "__pending__"),
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
  const pendingSourcesKpis = hasOwnField(kpis, "pendingSourcesCount")
    ? kpis
    : { ...(kpis || {}), pendingSourcesCount: kpis?.pendingApprovalsCount };
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
      <div class="admin-total-value">${formatOptionalText(kpis, "lastSuccessfulFetchAge", { pending: pendingLabelFor("lastSuccessfulFetchAge") })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Fetch Success (7d)</div>
      <div class="admin-total-value">${formatOptionalPercent(kpis, "sevenDayFetchSuccessRate", { pending: pendingLabelFor("sevenDayFetchSuccessRate") })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Avg Fetch Duration (7d)</div>
      <div class="admin-total-value">${formatOptionalDuration(kpis, "avgFetchDurationMs7d", { pending: pendingLabelFor("avgFetchDurationMs7d") })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Failed Source Ratio</div>
      <div class="admin-total-value">${formatOptionalPercent(kpis, "failedSourceRatioLatest", { pending: pendingLabelFor("failedSourceRatioLatest") })}</div>
    </div>
    <div class="admin-total-card">
      <div class="admin-total-label">Pending Sources</div>
      <div class="admin-total-value">${formatOptionalNumber(pendingSourcesKpis, "pendingSourcesCount")}</div>
    </div>
    ${registryDiagnosticsHtml}
  `;
}

export { renderAdminOpsAlerts, renderAdminOpsKpis };
