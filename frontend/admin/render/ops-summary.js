/**
 * Admin Ops summary rendering — thin coordinator.
 *
 * Owns the five public render entrypoints and nothing else.  Every payload
 * formatter, section builder, and derivation lives in a sibling leaf:
 * - ops-summary-fields.js            pending-aware scalar field formatters
 * - ops-summary-schedule.js          pipeline schedule status/controls/entry
 * - ops-summary-kpis.js              alert banners, KPI cards, sync diagnostics
 * - ops-summary-sections.js          generic section builders
 * - ops-summary-fetcher-metrics.js   fetcher-metrics payload derivation
 * - ops-summary-fetcher-sections.js  fetcher-metrics section HTML assembly
 * - ops-summary-fetcher-wiring.js    fetcher-metrics DOM event wiring
 * - ops-summary-dedup.js             dedup gate/audit/review/dedup-lists
 * - ops-summary-provider-static.js   provider/static disagreement rows
 * - ops-summary-source-policy.js     source-health/coverage/cleanup
 *
 * No leaf module imports from here.
 */

import { buildOpsFetcherMetricsViewModel } from "./ops-summary-fetcher-metrics.js";
import { buildOpsFetcherSections } from "./ops-summary-fetcher-sections.js";
import { wireOpsFetcherMetricsInteractions } from "./ops-summary-fetcher-wiring.js";
import { stableOpsSignature } from "./ops-shared.js";
import {
  buildDedupListsContent,
  wireDedupReviewActions
} from "./ops-summary-dedup.js";

export {
  renderAdminOpsAlerts,
  renderAdminOpsKpis
} from "./ops-summary-kpis.js";
export { renderAdminOpsSchedule } from "./ops-summary-schedule.js";

/** @typedef {import("../../shared/types.js").FetcherMetricsPayload} FetcherMetricsPayload */

/**
 * @param {HTMLElement|null|undefined} dedupEl
 * @param {Object|null|undefined} metrics
 * @param {Object} [options]
 */
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

  const input = { latest, history, metrics, options, summary };
  const viewModel = buildOpsFetcherMetricsViewModel(input);
  const { sectionHtml, diagnosticsByKey } = buildOpsFetcherSections(input, viewModel);
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

  if (typeof metricsEl.querySelectorAll !== "function") return;
  wireOpsFetcherMetricsInteractions(metricsEl, options, {
    diagnosticsByKey,
    providerStaticDisagreementRows: viewModel.providerStaticDisagreementRows,
    providerStaticTitleCompanyCollisionRows: viewModel.providerStaticTitleCompanyCollisionRows
  });
}
