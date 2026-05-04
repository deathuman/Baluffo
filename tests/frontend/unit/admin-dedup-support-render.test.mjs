import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsDedupLists } from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("admin render: fetcher metrics render dedup review-state summary", () => {
  const metricsEl = makeEl();
  renderAdminOpsDedupLists(metricsEl, {
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
            disagreementGateEvidence: ["manual_review_reviewed_safe"],
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

test("admin render: fetcher metrics tolerate missing dedup evidence", () => {
  const metricsEl = makeEl();
  renderAdminOpsDedupLists(metricsEl, {
    latestRun: { durationMs: 120000, sourceCount: 3, outputCount: 2 },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Dedup evidence/i);
  assert.match(metricsEl.innerHTML, /Dedup Audit Gate/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-audit-gate-card/i);
  assert.match(metricsEl.innerHTML, /status unknown/i);
  assert.match(metricsEl.innerHTML, /Blockers/i);
  assert.match(metricsEl.innerHTML, /Warnings/i);
  assert.match(metricsEl.innerHTML, /No gate examples/i);
  assert.match(metricsEl.innerHTML, /No carried bundle examples/i);
  assert.match(metricsEl.innerHTML, /No merged canonical jobs/i);
  assert.match(metricsEl.innerHTML, /No carried source-bundle collision outliers/i);
  assert.match(metricsEl.innerHTML, /No risky merge examples/i);
  assert.match(metricsEl.innerHTML, /Dedup outlier reasons/i);
  assert.match(metricsEl.innerHTML, /Dedup identity shapes/i);
  assert.match(metricsEl.innerHTML, /No dedup review queue examples/i);
});
