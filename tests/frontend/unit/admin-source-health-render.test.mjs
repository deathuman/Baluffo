import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsFetcherMetrics, renderAdminOpsKpis } from "../../../frontend/admin/render/ops-summary.js";

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
            lastAuditStatus: "safe",
            reviewState: "acknowledged",
            manualSuppressionOverride: "force_pause"
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
      },
      sourcePolicyRecommendationExport: {
        reviewStatePairCount: 2,
        manualForcePausedCount: 1
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
  assert.match(metricsEl.innerHTML, /Source-policy review/i);
  assert.match(metricsEl.innerHTML, /force-paused 1/i);
  assert.match(metricsEl.innerHTML, /local, reversible/i);
  assert.doesNotMatch(metricsEl.innerHTML, /Force pause/i);
  assert.doesNotMatch(metricsEl.innerHTML, /Clear override/i);
});

test("admin render: fetcher metrics render dedup review-state summary", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      sourceCount: 1,
      dedupReviewStateReadWarning: "malformed_dedup_review_state_artifact",
      dedupReviewStateSummary: {
        artifactPath: "data/dedup-review-state.json",
        status: "warning",
        readWarning: "malformed_dedup_review_state_artifact",
        reviewedPairCount: 2,
        reviewedSafeCount: 1,
        confirmedBlockingCount: 1,
        unresolvedBlockingCount: 1
      },
      dedupEvidence: {
        providerStaticDisagreementGateCounts: {
          blocked: 1,
          warning: 1,
          currentRunBlocked: 0,
          carriedBlocked: 1,
          carriedWarning: 1,
          autoSafeWarning: 0,
          locationPollutionWarning: 0,
          reviewedSafeWarning: 1,
          confirmedBlocking: 1
        },
        providerStaticDisagreementExamples: [
          {
            title: "Executive Assistant",
            company: "Animoca Brands",
            sourceBundleCount: 2,
            bundleEvidenceOrigin: "carried_from_existing_output",
            identityQuality: "provider_id_strong",
            providerSources: ["lever_sources"],
            staticSources: ["static_source::static:listing_url:https://careers.animocabrands.com/jobs"],
            providerUrls: ["https://jobs.lever.co/animocabrands/abc"],
            staticUrls: ["https://careers.animocabrands.com/companies/animoca-brands/jobs/1"],
            dedupReviewStatus: "reviewed_safe",
            dedupReviewUpdatedAt: "2026-05-02T10:00:00Z",
            dedupReviewUpdatedBy: "admin",
            disagreementGateDisposition: "warning",
            disagreementGateEvidence: [
              "manual_review_reviewed_safe"
            ],
            disagreementClassification: "same_job_different_urls",
            disagreementClassificationEvidence: [
              "origin:carried_from_existing_output",
              "provider_hosts:1",
              "static_hosts:1",
              "both_sides_have_ids_and_urls"
            ],
            disagreementEvidence: [
              "bundle_origin:carried_from_existing_output",
              "provider_urls:1",
              "static_urls:1",
              "shared_primary_url:false"
            ]
          }
        ],
        providerStaticTitleCompanyCollisionExamples: []
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Dedup review-state/i);
  assert.match(metricsEl.innerHTML, /path data\/dedup-review-state\.json/i);
  assert.match(metricsEl.innerHTML, /status warning/i);
  assert.match(metricsEl.innerHTML, /reviewed pairs 2/i);
  assert.match(metricsEl.innerHTML, /reviewed safe 1/i);
  assert.match(metricsEl.innerHTML, /confirmed blocking 1/i);
  assert.match(metricsEl.innerHTML, /unresolved blocking 1/i);
  assert.match(metricsEl.innerHTML, /warning malformed dedup review state artifact/i);
  assert.match(metricsEl.innerHTML, /review reviewed safe by admin at 2026-05-02T10:00:00Z/i);
  assert.doesNotMatch(metricsEl.innerHTML, /merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
});

test("admin render: ops health KPIs render dedup review-state summary", () => {
  const kpisEl = makeEl();
  renderAdminOpsKpis(kpisEl, {
    dedupReviewState: {
      artifactPath: "data/dedup-review-state.json",
      status: "warning",
      readWarning: "malformed_dedup_review_state_artifact",
      reviewedPairCount: 2,
      reviewedSafeCount: 1,
      confirmedBlockingCount: 1,
      unresolvedBlockingCount: 1
    },
    providerCoverage: {},
    registrySync: {},
    socialExperiment: {}
  }, "warning");

  assert.match(kpisEl.innerHTML, /Dedup review-state/i);
  assert.match(kpisEl.innerHTML, /path data\/dedup-review-state\.json/i);
  assert.match(kpisEl.innerHTML, /status warning/i);
  assert.match(kpisEl.innerHTML, /reviewed pairs 2/i);
  assert.match(kpisEl.innerHTML, /confirmed blocking 1/i);
  assert.match(kpisEl.innerHTML, /warning malformed dedup review state artifact/i);
});

test("admin render: fetcher metrics render conservative cleanup proposal closure", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      conservativeStaticCleanupProposals: {
        totalCandidateCount: 2,
        proposalCount: 1,
        staleCount: 0,
        blockedCount: 1,
        proposalGeneratedAt: "2026-05-01T00:00:00Z",
        proposalReportRunId: "fetch-123",
        proposalFreshnessStatus: "fresh",
        proposalFreshnessAgeSeconds: 0,
        proposalStaleThresholdSeconds: 86400,
        proposalReadinessHash: "abc123",
        blockedReasonCounts: {
          static_only_evidence_present: 1,
          source_sync_not_clean: 1
        },
        proposalReadyExamples: [
          {
            staticSourceName: "Static Alpha",
            providerSourceName: "Provider Alpha",
            recommendedAction: "move_static_to_hidden_pending",
            cleanRunEvidenceCount: 3,
            suppressionEvidenceStatus: "observed_dynamic_suppression",
            proposalReadiness: "actionable",
            proposalReadinessReason: "proposal evidence is fresh and actionable",
            proposalFreshnessStatus: "fresh",
            proposalFreshnessAgeSeconds: 0
          }
        ],
        blockedExamples: [
          {
            staticSourceName: "Static Beta",
            providerSourceName: "Provider Beta",
            blockers: ["static_only_evidence_present", "source_sync_not_clean"],
            proposalReadiness: "blocked",
            proposalReadinessReason: "static_only_evidence_present, source_sync_not_clean",
            proposalFreshnessStatus: "fresh",
            proposalFreshnessAgeSeconds: 0
          }
        ]
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Conservative static cleanup proposals/i);
  assert.match(metricsEl.innerHTML, /total candidates 2/i);
  assert.match(metricsEl.innerHTML, /proposal-ready 1/i);
  assert.match(metricsEl.innerHTML, /stale 0/i);
  assert.match(metricsEl.innerHTML, /blocked 1/i);
  assert.match(metricsEl.innerHTML, /status fresh/i);
  assert.match(metricsEl.innerHTML, /generated 2026-05-01T00:00:00Z/i);
  assert.match(metricsEl.innerHTML, /run fetch-123/i);
  assert.match(metricsEl.innerHTML, /stale after/i);
  assert.match(metricsEl.innerHTML, /static only evidence present 1/i);
  assert.match(metricsEl.innerHTML, /source sync not clean 1/i);
  assert.match(metricsEl.innerHTML, /Static Alpha/i);
  assert.match(metricsEl.innerHTML, /Static Beta/i);
  assert.doesNotMatch(metricsEl.innerHTML, /Mark reviewed safe/i);
  assert.doesNotMatch(metricsEl.innerHTML, /Force pause/i);
});
