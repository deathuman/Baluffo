/**
 * Admin Ops summary rendering — fetcher-metrics per-section HTML assembly.
 *
 * Split out of ``ops-summary.js``; that module stays the thin coordinator
 * owning the five public render entrypoints.
 *
 * @module ops-summary-fetcher-sections
 */

import { escapeHtml } from "../../shared/ui/index.js";
import {
  buildOpsFetcherDiagnosticsSections,
  buildOpsFetcherMetricSections,
  buildOpsTaskLaneRows
} from "../domain/ops-health-view-model.js";
import { formatDuration } from "./ops-shared.js";
import {
  formatCurrentRunMergeExamples,
  formatDedupAuditGateCard,
  formatDedupAuditGateExamples,
  formatDedupGoogleSheetsBucketIntentCounts,
  formatDedupGoogleSheetsBundleShapeCounts,
  formatDedupGoogleSheetsRoleBucketAuditCounts,
  formatDedupGoogleSheetsRoleBucketAuditSummary,
  formatDedupGoogleSheetsWeakGroupingAuditCounts,
  formatDedupIdentityQualityCounts,
  formatDedupIdentityShapeCounts,
  formatDedupNonProviderIdentityProvenanceCounts,
  formatDedupOutlierReasonCounts,
  formatDedupReviewQueueCauseCounts,
  formatDedupReviewQueueCounts,
  formatDedupReviewStateSummary,
  formatDedupRiskReasonCounts,
  formatOpsMetricsDetails
} from "./ops-summary-dedup.js";
import {
  formatProviderStaticDisagreementClassificationCounts,
  formatProviderStaticDisagreementCounts,
  formatProviderStaticDisagreementGateCounts,
  formatProviderStaticTitleCompanyCollisionAuditCounts,
  formatProviderStaticTitleCompanyCollisionCounts
} from "./ops-summary-provider-static.js";
import { formatDedupSourceClasses } from "./ops-summary-source-policy.js";
import {
  formatDiscoveryAuditArtifacts,
  formatOpsFetcherMetricSection,
  formatOpsTaskLane,
  formatPerformanceProfile,
  formatTaskFailureAttempts
} from "./ops-summary-sections.js";

/**
 * Assemble the ordered fetcher-metrics section HTML and the section-keyed
 * diagnostics from the derivation view model.
 *
 * @param {{ latest: Object, history: Object, metrics: Object, options: Object, summary: Object }} input
 * @param {Object} viewModel result of ``buildOpsFetcherMetricsViewModel``
 * @returns {{ sectionHtml: string, diagnosticsByKey: Object }}
 */
function buildOpsFetcherSections({ latest, history, metrics, options, summary }, viewModel) {
  const {
    failed,
    sourceCount,
    duplicateRate,
    outputYieldRate,
    failureRate,
    dedupEvidence,
    dedupReviewStateSummary,
    dedupReviewStateReadWarning,
    providerStaticOverlap,
    staticSuppressionPolicy,
    redundantStaticProposals,
    conservativeStaticCleanupProposals,
    sourcePolicyRecommendationExport,
    performanceProfile,
    frontendPerfSummary,
    slowestSummary,
    slowestStageSummary,
    highCostSummary,
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
    providerStaticDisagreementSummary,
    providerStaticTitleCompanyCollisionSummary,
    bucketSummaryHtml
  } = viewModel;

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

  return { sectionHtml, diagnosticsByKey };
}

export { buildOpsFetcherSections };
