import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminSourcePolicyReview } from "../../../frontend/admin/render/source-policy-review.js";

function makeMigrationLinkCandidate(overrides = {}) {
  return {
    providerSourceId: "greenhouse:slug:studio",
    providerSourceName: "Studio Greenhouse",
    providerAdapter: "greenhouse",
    providerIdField: "slug",
    providerIdValue: "studio",
    selectedStaticSourceId: "static:listing_url:https://studio.example/jobs",
    selectedStaticSourceName: "static_source::studio",
    selectedStaticUrl: "https://studio.example/jobs",
    staticTitle: "Senior Game Engineer",
    staticCompany: "Studio",
    staticCity: "Paris",
    staticCountry: "France",
    currentProviderLinkState: {
      providerBucket: "provider",
      migrationSourceIdentity: "greenhouse:slug:studio",
      providerSourceId: "greenhouse:slug:studio"
    },
    currentStaticLinkState: {
      providerBucket: "static",
      migrationSourceIdentity: "static:listing_url:https://studio.example/jobs",
      providerSourceId: "static:listing_url:https://studio.example/jobs"
    },
    currentRecommendation: "stable_safe_redundant",
    currentRecommendedAction: "keep_runtime_suppression",
    confidence: 0.83,
    confidenceTier: "high",
    apiEligible: true,
    apiValidationStatus: "eligible",
    blockers: [],
    evidenceReasons: ["stable_safe_redundant", "provider_static_redundant"],
    whyNotHighConfidence: "",
    ignoredAlternatives: ["alternative_candidate"],
    recommendedApiPayload: {
      providerSourceId: "greenhouse:slug:studio",
      providerSourceName: "Studio Greenhouse",
      providerAdapter: "greenhouse",
      providerIdField: "slug",
      providerIdValue: "studio",
      staticSourceId: "static:listing_url:https://studio.example/jobs",
      staticSourceName: "static_source::studio",
      staticUrl: "https://studio.example/jobs",
      staticTitle: "Senior Game Engineer",
      staticCompany: "Studio",
      staticCity: "Paris",
      staticCountry: "France",
      recommendedAction: "backfill_migration_identity_candidate",
      confidence: 0.83,
      apiEligible: true,
      apiValidationStatus: "eligible",
      blockers: [],
      evidenceReasons: ["stable_safe_redundant", "provider_static_redundant"],
      whyNotHighConfidence: "",
      ignoredAlternatives: ["alternative_candidate"]
    },
    sourceStateEvidence: undefined,
    ...overrides
  };
}

function makeEl(buttonsBySelector = {}) {
  return {
    innerHTML: "",
    querySelectorAll(selector) {
      return buttonsBySelector[selector] ?? [];
    }
  };
}

test("admin source policy review renders blocked migration link candidates read-only", () => {
  const reviewEl = makeEl();
  renderAdminSourcePolicyReview(reviewEl, {
    providerCoverageLinkBackfill: {
      disambiguationBlockerCounts: {
        no_source_state_history: 1,
        source_state_not_ok: 1
      },
      blockedCandidates: [
        makeMigrationLinkCandidate({
          confidence: 0.72,
          confidenceTier: "blocked",
          apiEligible: false,
          blockers: ["ambiguous_static_match"],
          evidenceReasons: ["redundant_static_rule_exact_match"],
          disambiguationBlockers: ["no_source_state_history"],
          ignoredAlternatives: [],
          recommendedApiPayload: undefined,
          sourceStateEvidence: {
            lastKeptCount: 4,
            lastStatus: "ok",
            lastSuccessfulAt: "2026-01-01T00:00:00Z",
            lastFetchedAt: "2026-01-02T00:00:00Z",
            providerCoverageStatus: "validated_provider",
            providerCoverageConsecutiveSuccesses: 2,
            providerCoverageLatestKeptCount: 4,
            evidenceScore: 7
          }
        }),
        makeMigrationLinkCandidate({
          confidence: 0.68,
          confidenceTier: "blocked",
          apiEligible: false,
          blockers: ["ambiguous_static_match"],
          evidenceReasons: ["redundant_static_rule_exact_match"],
          disambiguationBlockers: ["source_state_not_ok"],
          ignoredAlternatives: [],
          recommendedApiPayload: undefined,
          sourceStateEvidence: {
            lastKeptCount: 1,
            lastStatus: "error",
            lastSuccessfulAt: "2026-01-03T00:00:00Z",
            lastFetchedAt: "2026-01-04T00:00:00Z",
            providerCoverageStatus: "needs_review",
            providerCoverageConsecutiveSuccesses: 1,
            providerCoverageLatestKeptCount: 1,
            evidenceScore: 1
          }
        })
      ],
      disambiguationBlockedExamples: [
        makeMigrationLinkCandidate({
          disambiguationBlockers: ["no_source_state_history"]
        }),
        makeMigrationLinkCandidate({
          disambiguationBlockers: ["source_state_not_ok"]
        })
      ]
    }
  });

  assert.match(reviewEl.innerHTML, /Blocked Migration Link Candidates/);
  assert.match(reviewEl.innerHTML, /Blocked candidate\. Review the blocker evidence before any link is applied\./);
  assert.match(reviewEl.innerHTML, /ambiguous static match/);
  assert.match(reviewEl.innerHTML, /redundant static rule exact match/);
  assert.match(reviewEl.innerHTML, /Disambiguation/);
  assert.match(reviewEl.innerHTML, /no source state history/);
  assert.match(reviewEl.innerHTML, /source state not ok/);
  assert.match(reviewEl.innerHTML, /Last successful/);
  assert.match(reviewEl.innerHTML, /Last fetched/);
  assert.match(reviewEl.innerHTML, /Coverage status/);
  assert.match(reviewEl.innerHTML, /Coverage successes/);
  assert.match(reviewEl.innerHTML, /Coverage latest kept/);
  assert.match(reviewEl.innerHTML, /Disambiguation blockers:/);
  assert.match(reviewEl.innerHTML, /Read-only blocked candidate\./);
  assert.doesNotMatch(reviewEl.innerHTML, />Apply link</);
});
