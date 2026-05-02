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
            sourceClasses: { provider: 0, static: 0, social: 0, other: 255 }
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
  assert.match(metricsEl.innerHTML, /Top source-bundle outliers/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-evidence-table/i);
  assert.match(metricsEl.innerHTML, /Accounting/i);
  assert.match(metricsEl.innerHTML, /Kforce Inc/i);
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
});
