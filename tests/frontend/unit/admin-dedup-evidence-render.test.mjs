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

test("admin render: fetcher metrics render read-only dedup evidence", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 120000,
      sourceCount: 3,
      outputCount: 2,
      mergedCount: 1,
      dedupEvidence: {
        mergedCount: 1,
        collisionSamplesCount: 1,
        sourceBundleCollisionCount: 2,
        riskyMergeExampleCount: 1,
        mergeReasonCounts: {
          primaryUrl: 1,
          secondaryKey: 0,
          socialKey: 0,
          sparseIdentity: 0,
          unknown: 0
        },
        sourceBundleComposition: {
          provider: 1,
          static: 1,
          social: 0,
          other: 0
        },
        riskReasonCounts: {
          same_title_company_different_location: 1,
          provider_static_duplicate_disagreement: 1,
          missing_provider_ids: 0,
          weak_title_company_only_evidence: 0
        },
        outlierReasonCounts: {
          multi_location_strong_identity: 1,
          location_divergence_without_strong_identity: 2,
          provider_static_disagreement: 1,
          large_other_source_bundle: 1,
          sparse_title_company_bundle: 0,
          unknown: 0
        },
        identityShapeCounts: {
          shared_job_detail_url: 1,
          shared_listing_or_category_url: 2,
          many_unique_urls_same_title: 3,
          provider_id_backed: 4,
          missing_url_and_ids: 0,
          mixed_or_unknown_identity: 0
        },
        topMergedJobs: [
          {
            title: "Senior Engineer",
            company: "Studio One",
            sourceBundleCount: 2,
            sourceClasses: { provider: 1, static: 1, social: 0, other: 0 }
          }
        ],
        topSourceBundleOutliers: [
          {
            title: "Accounting",
            company: "Kforce Inc",
            sourceBundleCount: 255,
            sourceClasses: { provider: 0, static: 0, social: 0, other: 255 },
            outlierReason: "large_other_source_bundle",
            distinctLocationCount: 30,
            uniqueJobLinkCount: 2,
            sharedPrimaryUrl: false,
            providerSourceJobIdCount: 0,
            hasStrongIdentity: false,
            dominantSourceClass: "other",
            identityShape: "shared_listing_or_category_url",
            sharedUrlHost: "kforce.example",
            sharedUrlPath: "/jobs",
            uniqueUrlHostCount: 1,
            uniqueUrlPathPrefixCount: 1,
            titleShape: "category_like",
            identityCaveats: [
              "shared_url_looks_like_listing_or_category",
              "category_like_title",
              "other_source_class_dominant"
            ]
          }
        ],
        riskyMergeExamples: [
          {
            title: "Designer",
            company: "Studio Two",
            riskReasons: ["same_title_company_different_location"]
          }
        ]
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Current Run Merges/i);
  assert.match(metricsEl.innerHTML, /Bundle Collisions/i);
  assert.match(metricsEl.innerHTML, /Risky Merges/i);
  assert.match(metricsEl.innerHTML, /Dedup evidence/i);
  assert.match(metricsEl.innerHTML, /primary URL 1/i);
  assert.match(metricsEl.innerHTML, /Carried source-bundle collision rows: 2/i);
  assert.match(metricsEl.innerHTML, /provider 1, static 1/i);
  assert.match(metricsEl.innerHTML, /Dedup risk reasons/i);
  assert.match(metricsEl.innerHTML, /location 1, provider\/static 1/i);
  assert.match(metricsEl.innerHTML, /Dedup outlier reasons/i);
  assert.match(metricsEl.innerHTML, /multi-location strong 1/i);
  assert.match(metricsEl.innerHTML, /large other 1/i);
  assert.match(metricsEl.innerHTML, /Dedup identity shapes/i);
  assert.match(metricsEl.innerHTML, /detail URL 1/i);
  assert.match(metricsEl.innerHTML, /listing\/category URL 2/i);
  assert.match(metricsEl.innerHTML, /many URLs 3/i);
  assert.match(metricsEl.innerHTML, /provider ID 4/i);
  assert.match(metricsEl.innerHTML, /Top source-bundle outliers/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-evidence-table/i);
  assert.match(metricsEl.innerHTML, /Accounting/i);
  assert.match(metricsEl.innerHTML, /Kforce Inc/i);
  assert.match(metricsEl.innerHTML, /large other source bundle/i);
  assert.match(metricsEl.innerHTML, /30 locations/i);
  assert.match(metricsEl.innerHTML, /2 links/i);
  assert.match(metricsEl.innerHTML, /0 provider IDs/i);
  assert.match(metricsEl.innerHTML, /other dominant/i);
  assert.match(metricsEl.innerHTML, /weak identity/i);
  assert.match(metricsEl.innerHTML, /shared listing or category url/i);
  assert.match(metricsEl.innerHTML, /title category like/i);
  assert.match(metricsEl.innerHTML, /shared kforce\.example\/jobs/i);
  assert.match(metricsEl.innerHTML, /caveats shared url looks like listing or category/i);
  assert.match(metricsEl.innerHTML, /Senior Engineer/i);
  assert.match(metricsEl.innerHTML, /Studio One/i);
  assert.match(metricsEl.innerHTML, /Designer/i);
  assert.match(metricsEl.innerHTML, /Studio Two/i);
  assert.doesNotMatch(metricsEl.innerHTML, /merge-btn|unmerge-btn|cleanup|lifecycle/i);
});

test("admin render: fetcher metrics tolerate missing dedup evidence", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: { durationMs: 120000, sourceCount: 3, outputCount: 2 },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Dedup evidence/i);
  assert.match(metricsEl.innerHTML, /No merged canonical jobs/i);
  assert.match(metricsEl.innerHTML, /No carried source-bundle collision outliers/i);
  assert.match(metricsEl.innerHTML, /No risky merge examples/i);
  assert.match(metricsEl.innerHTML, /Dedup outlier reasons/i);
  assert.match(metricsEl.innerHTML, /Dedup identity shapes/i);
});
