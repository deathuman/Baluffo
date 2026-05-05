import test from "node:test";
import assert from "node:assert/strict";
import {
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics
} from "../../../frontend/admin/render.js";

function makeEl(buttonsBySelector = {}) {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: selector => buttonsBySelector[selector] || []
  };
}

function makeAttrButton(attrs) {
  return {
    getAttribute(name) {
      return attrs[name] || "";
    },
    addEventListener(_event, handler) {
      this.click = handler;
    }
  };
}

test("admin render: health diagnostics stay compact and dedup lists render separately", () => {
  const metricsEl = makeEl();
  const dedupEl = makeEl();
  const metrics = {
    latestRun: {
      dedupEvidence: {
        mergeReasonCounts: { secondaryKey: 2 },
        currentRunMergeExamples: [{ title: "Designer", company: "Studio", mergeReason: "secondaryKey" }]
      },
      sourceHealth: {
        zeroKeptNeedsReview: [{ name: "Source A", status: "ok", keptCount: 0 }]
      },
      providerCoverage: {
        needsReviewProviders: [{ name: "Provider A", providerCoverageStatus: "probing" }]
      }
    },
    history: {}
  };
  renderAdminOpsFetcherMetrics(metricsEl, {
    ...metrics
  });
  renderAdminOpsDedupLists(dedupEl, metrics);

  assert.doesNotMatch(metricsEl.innerHTML, /Dedup supporting diagnostics/i);
  assert.doesNotMatch(metricsEl.innerHTML, /Dedup Audit Gate/i);
  assert.match(dedupEl.innerHTML, /<details class="admin-ops-metrics-details admin-ops-dedup-details">/i);
  assert.match(dedupEl.innerHTML, /Dedup supporting diagnostics/i);
  assert.match(dedupEl.innerHTML, /Dedup Audit Gate/i);
  assert.match(metricsEl.innerHTML, /<details class="admin-ops-metrics-details admin-ops-source-health-details">/i);
  assert.match(metricsEl.innerHTML, /<details class="admin-ops-metrics-details admin-ops-source-policy-details">/i);
  assert.match(metricsEl.innerHTML, /Frontend fetch\/render counters/i);
  assert.match(metricsEl.innerHTML, /No frontend fetch\/render counter samples yet/i);
  assert.doesNotMatch(metricsEl.innerHTML, /merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
  assert.doesNotMatch(dedupEl.innerHTML, /merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
});

test("admin render: frontend perf counters appear in ops diagnostics", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    frontendPerfCounters: {
      frontend_fetch_bridge_get_ops_health: {
        count: 3,
        p50Ms: 12,
        p95Ms: 48
      }
    }
  });

  assert.match(metricsEl.innerHTML, /Frontend fetch\/render counters/i);
  assert.match(metricsEl.innerHTML, /frontend_fetch_bridge_get_ops_health/i);
  assert.match(metricsEl.innerHTML, /p95 48ms/i);
  assert.match(metricsEl.innerHTML, /count 3/i);
});

test("admin render: dedup review action wiring survives disclosure", () => {
  const reviewButton = makeAttrButton({
    "data-dedup-review-action": "reviewed_safe",
    "data-dedup-review-table": "providerStatic",
    "data-dedup-review-row": "0"
  });
  const metricsEl = makeEl({
    "[data-dedup-review-action]": [reviewButton]
  });
  const calls = [];
  renderAdminOpsDedupLists(metricsEl, {
    latestRun: {
      dedupEvidence: {
        providerStaticDisagreementExamples: [
          {
            title: "Designer",
            company: "Studio",
            dedupKey: "designer|studio",
            bundleEvidenceOrigin: "current_run",
            disagreementClassification: "same_job_different_urls",
            providerSources: ["provider"],
            staticSources: ["static"]
          }
        ]
      }
    },
    history: {}
  }, {
    onDedupReviewAction: (row, action) => calls.push({ row, action })
  });

  reviewButton.click();

  assert.equal(calls.length, 1);
  assert.equal(calls[0].action, "reviewed_safe");
  assert.equal(calls[0].row.title, "Designer");
});
