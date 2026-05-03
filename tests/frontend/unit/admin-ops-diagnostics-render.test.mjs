import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";

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

test("admin render: dense diagnostics render inside native disclosure blocks", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
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
  });

  assert.match(metricsEl.innerHTML, /<details class="admin-ops-metrics-details admin-ops-dedup-details">/i);
  assert.match(metricsEl.innerHTML, /Dedup supporting diagnostics/i);
  assert.match(metricsEl.innerHTML, /Dedup Audit Gate/i);
  assert.match(metricsEl.innerHTML, /<details class="admin-ops-metrics-details admin-ops-source-health-details">/i);
  assert.match(metricsEl.innerHTML, /<details class="admin-ops-metrics-details admin-ops-source-policy-details">/i);
  assert.doesNotMatch(metricsEl.innerHTML, /merge-btn|unmerge-btn|cleanup-btn|lifecycle-btn/i);
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
  renderAdminOpsFetcherMetrics(metricsEl, {
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
  }, null, {
    onDedupReviewAction: (row, action) => calls.push({ row, action })
  });

  reviewButton.click();

  assert.equal(calls.length, 1);
  assert.equal(calls[0].action, "reviewed_safe");
  assert.equal(calls[0].row.title, "Designer");
});
