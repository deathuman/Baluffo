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

test("admin render: discovery audit artifact diagnostics are bounded and actionable", () => {
  const copyButton = makeAttrButton({ "data-ops-diagnostics-copy": "auditArtifacts" });
  const refreshButton = makeAttrButton({});
  const metricsEl = makeEl({
    "[data-ops-diagnostics-copy]": [copyButton],
    '[data-action="refresh-discovery-audit-artifacts"]': [refreshButton]
  });
  const copied = [];
  let refreshed = 0;

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    discoveryAuditArtifacts: {
      ok: true,
      artifacts: [
        {
          name: "sheet-directory",
          exists: true,
          pathDisplay: "/data/sheet-directory-discovery-audit.json",
          sizeBytes: 1234,
          modifiedAt: "2026-06-02T20:00:00Z",
          sha256: "abc123",
          topLevelKeys: ["status", "summary"],
          summary: { status: "ok", candidatesCount: 1 },
          warnings: []
        },
        {
          name: "web-search",
          exists: false,
          pathDisplay: "/data/web-search-discovery-audit.json",
          sizeBytes: 0,
          warnings: ["missing"]
        }
      ]
    }
  }, null, {
    onCopySectionDiagnostics: section => copied.push(section),
    onRefreshAuditArtifacts: () => { refreshed += 1; }
  });

  assert.match(metricsEl.innerHTML, /Audit Artifacts/i);
  assert.match(metricsEl.innerHTML, /Discovery audit artifacts/i);
  assert.match(metricsEl.innerHTML, /1\/2 present/i);
  assert.match(metricsEl.innerHTML, /sheet-directory-discovery-audit\.json/i);
  assert.match(metricsEl.innerHTML, /Refresh artifacts/i);
  assert.doesNotMatch(metricsEl.innerHTML, /do-not-expose|rawSourceRows/i);

  copyButton.click();
  refreshButton.click();

  assert.equal(copied.length, 1);
  assert.equal(copied[0].key, "auditArtifacts");
  assert.equal(copied[0].artifacts.length, 2);
  assert.equal(copied[0].artifacts[0].summary.candidatesCount, 1);
  assert.equal(refreshed, 1);
});

test("admin render: task failure-attempt diagnostics are bounded and copyable", () => {
  const copyButton = makeAttrButton({ "data-ops-diagnostics-copy": "taskFailures" });
  const refreshButton = makeAttrButton({});
  const metricsEl = makeEl({
    "[data-ops-diagnostics-copy]": [copyButton],
    '[data-action="refresh-task-failure-attempts"]': [refreshButton]
  });
  const copied = [];
  let refreshed = 0;

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    taskFailureAttempts: {
      ok: true,
      generatedAt: "2026-06-04T07:00:00Z",
      fetch: {
        runId: "fetch_latest",
        hardFailureCount: 0,
        partialWarningCount: 1,
        expectedExclusionCount: 2127,
        failedSources: 0,
        excludedSources: 2127,
        failureBuckets: [{ key: "partial_warning", count: 1 }],
        partialWarnings: [
          {
            name: "scrapy_static_sources",
            status: "ok",
            error: "https://hidden.invalid/raw should not render"
          }
        ]
      },
      discovery: {
        runId: "discovery_latest",
        failureRecordCount: 729,
        expectedSkipCount: 415,
        actionableDiagnosticCount: 314,
        highPriorityBuckets: [
          { key: "dedupe_skipped", count: 405, classification: "expected_skip" },
          { key: "gamedevmap_recovery_fetch", count: 158, classification: "actionable_diagnostic" }
        ]
      },
      warnings: []
    }
  }, null, {
    onCopySectionDiagnostics: section => copied.push(section),
    onRefreshTaskFailureAttempts: () => { refreshed += 1; }
  });

  assert.match(metricsEl.innerHTML, /Task Failure Attempts/i);
  assert.match(metricsEl.innerHTML, /fetch hard 0/i);
  assert.match(metricsEl.innerHTML, /partial 1/i);
  assert.match(metricsEl.innerHTML, /expected cache exclusions 2[,.]127/i);
  assert.match(metricsEl.innerHTML, /gamedevmap_recovery_fetch/i);
  assert.match(metricsEl.innerHTML, /actionable diagnostic/i);
  assert.doesNotMatch(metricsEl.innerHTML, /hidden\.invalid|raw should not render/i);

  copyButton.click();
  refreshButton.click();

  assert.equal(copied.length, 1);
  assert.equal(copied[0].key, "taskFailures");
  assert.equal(copied[0].fetch.partialWarningCount, 1);
  assert.equal(copied[0].discovery.highPriorityBuckets.length, 2);
  assert.equal(refreshed, 1);
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
