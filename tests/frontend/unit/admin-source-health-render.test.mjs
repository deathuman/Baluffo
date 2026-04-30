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
        ]
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Sources needing attention/i);
  assert.match(metricsEl.innerHTML, /Zero kept \/ needs review/i);
  assert.match(metricsEl.innerHTML, /Browser fallback recommended/i);
  assert.match(metricsEl.innerHTML, /Top productive sources/i);
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
      }
    },
    history: {}
  });

  assert.match(metricsEl.innerHTML, /Validated staged providers/i);
  assert.match(metricsEl.innerHTML, /Provider coverage needs review/i);
  assert.match(metricsEl.innerHTML, /Unstable \/ failed providers/i);
  assert.match(metricsEl.innerHTML, /Ready later \(no static mutation\)/i);
  assert.match(metricsEl.innerHTML, /Studio Greenhouse/i);
});
