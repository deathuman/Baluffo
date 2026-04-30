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
