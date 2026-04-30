import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render/ops-summary.js";

function makeEl() {
  return { innerHTML: "", dataset: {} };
}

test("admin render: fetcher metrics render source-health triage", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      sourceCount: 3,
      sourceHealth: {
        sourcesNeedingAttention: [
          {
            name: "stormind",
            status: "ok",
            keptCount: 0,
            durationMs: 25000,
            failureBucket: "needs_review"
          }
        ],
        zeroKeptNeedsReview: [
          {
            name: "stormind",
            status: "ok",
            keptCount: 0,
            durationMs: 25000,
            failureBucket: "needs_review"
          }
        ],
        browserFallbackRecommended: [
          {
            name: "disney",
            status: "error",
            keptCount: 0,
            durationMs: 31000,
            failureBucket: "timeout"
          }
        ],
        topProductiveSources: [
          { name: "greenhouse_boards", status: "ok", keptCount: 120, durationMs: 1000 }
        ],
        dynamicRedundantStatic: [
          {
            name: "static_source::covered",
            status: "excluded",
            exclusionReason: "dynamic_redundant_provider",
            coveredByProviderSourceId: "Studio Greenhouse",
            coveredByProviderAdapter: "greenhouse",
            providerCoverageConsecutiveSuccesses: 2
          }
        ]
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Sources needing attention/i);
  assert.match(metricsEl.innerHTML, /Zero kept \/ needs review/i);
  assert.match(metricsEl.innerHTML, /Browser fallback recommended/i);
  assert.match(metricsEl.innerHTML, /Top productive sources/i);
  assert.match(metricsEl.innerHTML, /Runtime-suppressed static sources/i);
  assert.match(metricsEl.innerHTML, /Studio Greenhouse/i);
  assert.match(metricsEl.innerHTML, /greenhouse_boards/i);
});

test("admin render: fetcher metrics render provider coverage lanes", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      sourceCount: 1,
      providerCoverage: {
        validatedProviders: [
          {
            name: "Studio Greenhouse",
            providerCoverageStatus: "validated_provider",
            providerReplacementReadiness: "candidate",
            providerCoverageLatestKeptCount: 8,
            providerCoverageConsecutiveSuccesses: 1
          }
        ],
        needsReviewProviders: [
          {
            name: "Zero Jobs Provider",
            providerCoverageStatus: "needs_review",
            providerReplacementReadiness: "none"
          }
        ],
        unstableOrFailedProviders: [
          {
            name: "Broken Provider",
            providerCoverageStatus: "failed_provider",
            providerReplacementReadiness: "none"
          }
        ],
        readyLaterProviders: [
          {
            name: "Ready Later Provider",
            providerCoverageStatus: "validated_provider",
            providerReplacementReadiness: "ready_later",
            providerCoverageConsecutiveSuccesses: 2
          }
        ]
      },
      providerStaticOverlap: {
        safePairCount: 1,
        needsReviewPairCount: 1,
        insufficientHistoryPairCount: 0,
        pairs: [
          {
            staticSourceName: "static_source::covered",
            providerSourceName: "Studio Greenhouse",
            auditStatus: "safe",
            overlapCount: 2,
            staticOnlyCount: 0
          },
          {
            staticSourceName: "static_source::static-only",
            providerSourceName: "Broken Provider",
            auditStatus: "needs_review",
            overlapCount: 0,
            staticOnlyCount: 1
          }
        ]
      },
      staticSuppressionPolicy: {
        suppressedCount: 1,
        pausedCount: 1,
        warningCount: 1,
        suppressedPairs: [
          {
            staticSourceName: "static_source::covered",
            providerSourceName: "Studio Greenhouse",
            decision: "suppressed",
            reason: "prior_audit_safe",
            lastAuditStatus: "safe"
          }
        ],
        pausedPairs: [
          {
            staticSourceName: "static_source::static-only",
            providerSourceName: "Broken Provider",
            decision: "paused",
            reason: "prior_static_only_jobs_detected",
            lastAuditStatus: "needs_review"
          }
        ],
        warningPairs: [
          {
            staticSourceName: "static_source::warning",
            providerSourceName: "Studio Greenhouse",
            decision: "warning",
            reason: "prior_insufficient_history",
            lastAuditStatus: "insufficient_history"
          }
        ]
      },
      redundantStaticProposals: {
        totalProposalCount: 3,
        safeRedundantCount: 1,
        keepStaticCount: 0,
        needsMoreHistoryCount: 1,
        needsReviewCount: 0,
        providerUnstableCount: 0,
        staticOnlyDetectedCount: 1,
        proposals: [
          {
            staticSourceName: "static_source::covered",
            providerSourceName: "Studio Greenhouse",
            proposal: "safe_redundant_static",
            recommendedAction: "keep_runtime_suppression",
            lastAuditStatus: "safe"
          },
          {
            staticSourceName: "static_source::warning",
            providerSourceName: "Studio Greenhouse",
            proposal: "needs_more_history",
            recommendedAction: "collect_more_history",
            lastAuditStatus: "insufficient_history"
          },
          {
            staticSourceName: "static_source::static-only",
            providerSourceName: "Broken Provider",
            proposal: "static_only_jobs_detected",
            recommendedAction: "pause_suppression",
            lastAuditStatus: "needs_review"
          }
        ]
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Validated staged providers/i);
  assert.match(metricsEl.innerHTML, /Provider coverage needs review/i);
  assert.match(metricsEl.innerHTML, /Unstable \/ failed providers/i);
  assert.match(metricsEl.innerHTML, /Ready later \(no static mutation\)/i);
  assert.match(metricsEl.innerHTML, /Studio Greenhouse/i);
  assert.match(metricsEl.innerHTML, /Provider\/static overlap audit/i);
  assert.match(metricsEl.innerHTML, /static-only 1/i);
  assert.match(metricsEl.innerHTML, /Static suppression policy/i);
  assert.match(metricsEl.innerHTML, /prior static only jobs detected/i);
  assert.match(metricsEl.innerHTML, /prior insufficient history/i);
  assert.match(metricsEl.innerHTML, /Redundant static proposals/i);
  assert.match(metricsEl.innerHTML, /safe redundant static/i);
  assert.match(metricsEl.innerHTML, /collect more history/i);
  assert.match(metricsEl.innerHTML, /static only jobs detected/i);
});
