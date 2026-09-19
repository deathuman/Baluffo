/**
 * Admin Ops summary rendering — fetcher-metrics payload derivation.
 *
 * Split out of ``ops-summary.js``; that module stays the thin coordinator
 * owning the five public render entrypoints.
 *
 * @module ops-summary-fetcher-metrics
 */

import { escapeHtml } from "../../shared/ui/index.js";
import {
  FETCHER_FAILURE_BUCKET_LABELS,
  formatDuration,
  sanitizeSlowSourceName
} from "./ops-shared.js";
import {
  formatDedupMergedRows,
  formatDedupOutlierRows,
  formatDedupReviewQueueRows,
  formatDedupRiskRows
} from "./ops-summary-dedup.js";
import {
  formatProviderStaticDisagreementRows,
  formatProviderStaticTitleCompanyCollisionRows
} from "./ops-summary-provider-static.js";
import {
  formatConservativeCleanupBlockedRows,
  formatConservativeCleanupFreshnessSummary,
  formatConservativeCleanupProposalRows,
  formatConservativeCleanupReasonCounts,
  formatDynamicRedundantStaticRows,
  formatProviderCoverageRows,
  formatProviderStaticOverlapRows,
  formatRedundantStaticProposalRows,
  formatSourceHealthRows,
  formatStaticSuppressionPolicyRows
} from "./ops-summary-source-policy.js";

/**
 * Derive every scalar, summary string, and row list the fetcher-metrics
 * sections render, returning one flat view model.
 *
 * @param {{ latest: Object, metrics: Object, options: Object, summary: Object }} input
 * @returns {Object}
 */
function buildOpsFetcherMetricsViewModel({ latest, metrics, options, summary }) {
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

  return {
    failed,
    sourceCount,
    duplicateRate,
    outputYieldRate,
    failureRate,
    slowest,
    stageTop,
    highCostLowYield,
    sourceHealth,
    dedupEvidence,
    dedupReviewStateSummary,
    dedupReviewStateReadWarning,
    providerCoverage,
    providerStaticOverlap,
    staticSuppressionPolicy,
    redundantStaticProposals,
    conservativeStaticCleanupProposals,
    sourcePolicyRecommendationExport,
    frontendPerfCounters,
    performanceProfile,
    frontendPerfCounterRows,
    frontendPerfSummary,
    slowestSummary,
    slowestStageSummary,
    highCostSummary,
    bucketRows,
    attentionSummary,
    zeroReviewSummary,
    browserSummary,
    productiveSummary,
    dynamicRedundantSummary,
    validatedProviderSummary,
    failedProviderSummary,
    reviewProviderSummary,
    readyLaterProviderSummary,
    overlapAuditSummary,
    suppressedPolicySummary,
    pausedPolicySummary,
    warningPolicySummary,
    proposalRows,
    safeRedundantProposalSummary,
    keepStaticProposalSummary,
    moreHistoryProposalSummary,
    reviewProposalSummary,
    staticOnlyProposalSummary,
    cleanupProposalReadySummary,
    cleanupBlockedSummary,
    cleanupBlockedReasonSummary,
    cleanupFreshnessSummary,
    mergeReasonCounts,
    sourceBundleComposition,
    riskReasonCounts,
    outlierReasonCounts,
    identityShapeCounts,
    identityQualityCounts,
    nonProviderIdentityProvenanceCounts,
    googleSheetsBundleShapeCounts,
    googleSheetsRoleBucketAuditCounts,
    googleSheetsRoleBucketAudit,
    googleSheetsBucketIntentCounts,
    googleSheetsWeakGroupingAuditCounts,
    reviewQueueCounts,
    reviewQueueCauseCounts,
    dedupAuditGate,
    providerStaticDisagreementCounts,
    providerStaticDisagreementGateCounts,
    providerStaticDisagreementClassificationCounts,
    providerStaticTitleCompanyCollisionCounts,
    providerStaticTitleCompanyCollisionAuditCounts,
    topMergedSummary,
    topOutlierSummary,
    riskyMergeSummary,
    reviewQueueSummary,
    providerStaticDisagreementRows,
    providerStaticTitleCompanyCollisionRows,
    providerStaticDisagreementSummary,
    providerStaticTitleCompanyCollisionSummary,
    bucketSummaryHtml
  };
}

export { buildOpsFetcherMetricsViewModel };
