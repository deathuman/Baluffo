import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("admin render: provider/static disagreement examples are read-only", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 120000,
      sourceCount: 3,
      outputCount: 2,
      dedupEvidence: {
        dedupAuditGate: {
          status: "blocked",
          lifecycleUxReady: false,
          currentRunMergedCount: 0,
          sourceBundleCollisionCount: 1,
          currentRunSourceBundleCollisionCount: 0,
          carriedSourceBundleCollisionCount: 1,
          highRiskReviewQueueCount: 1,
          currentRunHighRiskReviewQueueCount: 0,
          carriedHighRiskReviewQueueCount: 1,
          providerStaticDisagreementCount: 1,
          providerStaticDisagreementCurrentRunCount: 0,
          providerStaticDisagreementCarriedCount: 1,
          googleSheetsGenericRoleGuardActive: true,
          blockers: ["provider_static_disagreement_needs_review"],
          warnings: [],
          examples: [
            {
              title: "Executive Assistant",
              company: "Animoca Brands",
              recommendedReviewAction: "review_provider_static_disagreement",
              suspectedCause: "provider_static_disagreement",
              sourceBundleCount: 2,
              identityQuality: "provider_id_strong",
              bundleEvidenceOrigin: "carried_from_existing_output"
            }
          ]
        },
        providerStaticDisagreementCounts: {
          total: 1,
          currentRun: 0,
          carried: 1
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
            disagreementEvidence: [
              "bundle_origin:carried_from_existing_output",
              "provider_urls:1",
              "static_urls:1",
              "shared_primary_url:false"
            ]
          }
        ]
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Dedup provider\/static disagreements/i);
  assert.match(metricsEl.innerHTML, /total 1, current 0, carried 1/i);
  assert.match(metricsEl.innerHTML, /provider\/static current 0/i);
  assert.match(metricsEl.innerHTML, /provider\/static carried 1/i);
  assert.match(metricsEl.innerHTML, /Executive Assistant/i);
  assert.match(metricsEl.innerHTML, /Animoca Brands/i);
  assert.match(metricsEl.innerHTML, /provider lever_sources/i);
  assert.match(
    metricsEl.innerHTML,
    /static static_source::static:listing_url:https:\/\/careers\.animocabrands\.com\/jobs/i
  );
  assert.match(metricsEl.innerHTML, /shared primary url:false/i);
  assert.doesNotMatch(metricsEl.innerHTML, /merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
});

test("admin render: missing provider/static disagreement examples render safely", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 120000,
      sourceCount: 3,
      outputCount: 2,
      dedupEvidence: {}
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Dedup provider\/static disagreements/i);
  assert.match(metricsEl.innerHTML, /total 0, current 0, carried 0/i);
  assert.match(metricsEl.innerHTML, /No provider\/static disagreement examples/i);
});
