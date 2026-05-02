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
        topMergedJobs: [
          {
            title: "Senior Engineer",
            company: "Studio One",
            sourceBundleCount: 2,
            sourceClasses: { provider: 1, static: 1, social: 0, other: 0 }
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

  assert.match(metricsEl.innerHTML, /Dedup Merged/i);
  assert.match(metricsEl.innerHTML, /Dedup Collisions/i);
  assert.match(metricsEl.innerHTML, /Risky Merges/i);
  assert.match(metricsEl.innerHTML, /Dedup evidence/i);
  assert.match(metricsEl.innerHTML, /primary URL 1/i);
  assert.match(metricsEl.innerHTML, /provider 1, static 1/i);
  assert.match(metricsEl.innerHTML, /admin-dedup-evidence-table/i);
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
  assert.match(metricsEl.innerHTML, /No risky merge examples/i);
});
