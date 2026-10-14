import test from "node:test";
import assert from "node:assert/strict";
import {
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsKpis
} from "../../../frontend/admin/render.js";
import {
  createButtonMapEl as makeEl,
  createAttrButton as makeAttrButton
} from "./helpers/dom-test-helpers.mjs";

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

test("admin render: debug diagnostics can be hidden behind explicit action", () => {
  const debugButton = makeAttrButton({});
  const metricsEl = makeEl({
    '[data-action="load-debug-diagnostics"]': [debugButton]
  });
  let loaded = 0;

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    frontendPerfCounters: {
      frontend_fetch_bridge_get_ops_health: { count: 3, p50Ms: 12, p95Ms: 48 }
    },
    performanceProfile: {
      routeTimings: { routes: [{ label: "GET /ops/dashboard-health", p95Ms: 3100 }] }
    }
  }, null, {
    includeDebugDiagnostics: false,
    onLoadDebugDiagnostics: () => {
      loaded += 1;
    }
  });

  assert.match(metricsEl.innerHTML, /Debug diagnostics/i);
  assert.match(metricsEl.innerHTML, /Load debug diagnostics/i);
  assert.doesNotMatch(metricsEl.innerHTML, /Frontend fetch\/render counters/i);
  assert.doesNotMatch(metricsEl.innerHTML, /Backend performance/i);
  debugButton.click();
  assert.equal(loaded, 1);
});

test("admin render: backend performance diagnostics are bounded and refreshable", () => {
  const copyButton = makeAttrButton({ "data-ops-diagnostics-copy": "performance" });
  const refreshButton = makeAttrButton({});
  const metricsEl = makeEl({
    "[data-ops-diagnostics-copy]": [copyButton],
    '[data-action="refresh-performance-profile"]': [refreshButton]
  });
  const copied = [];
  let refreshed = 0;

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    performanceProfile: {
      ok: true,
      generatedAt: "2026-06-05T00:00:00+00:00",
      runtime: { runtimeMode: "container", appVersion: "0.2.43" },
      routeTimings: {
        routes: [
          {
            label: "GET /ops/dashboard-health",
            count: 3,
            p50Ms: 850,
            p95Ms: 3100,
            maxMs: 3100,
            errorCount: 0
          }
        ]
      },
      operationTimings: {
        operations: [
          {
            label: "ops.dashboard.history",
            count: 3,
            p50Ms: 20,
            p95Ms: 80,
            maxMs: 80,
            errorCount: 0
          }
        ]
      }
    }
  }, null, {
    onCopySectionDiagnostics: section => copied.push(section),
    onRefreshPerformanceProfile: () => { refreshed += 1; }
  });

  assert.match(metricsEl.innerHTML, /Backend Performance/i);
  assert.match(metricsEl.innerHTML, /GET \/ops\/dashboard-health/i);
  assert.match(metricsEl.innerHTML, /ops\.dashboard\.history/i);
  assert.match(metricsEl.innerHTML, /Refresh performance/i);
  assert.doesNotMatch(metricsEl.innerHTML, /token=|secret|rawSamples/i);

  copyButton.click();
  refreshButton.click();

  assert.equal(copied.length, 1);
  assert.equal(copied[0].key, "performance");
  assert.equal(copied[0].routes[0].label, "GET /ops/dashboard-health");
  assert.equal(copied[0].operations[0].label, "ops.dashboard.history");
  assert.equal(refreshed, 1);
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
          name: "page-fetch-lifecycle",
          exists: true,
          pathDisplay: "/data/page-fetch-lifecycle.v1.json",
          sizeBytes: 262144,
          modifiedAt: "2026-10-13T09:00:00Z",
          fingerprintKind: "sha256",
          fingerprint: "def456",
          sha256: "def456",
          topLevelKeys: ["sources"],
          summary: { status: "ok", candidateCount: 1 },
          pageFetchLifecycle: {
            runId: "run-1",
            coverage: "full",
            truncated: false,
            omittedCount: 0,
            sources: {
              gameprog: {
                source: "gameprog",
                state: "clean",
                coverage: "full",
                truncated: false,
                omittedCount: 0,
                receiptCount: 1,
                rollup: {
                  batchCount: 1, submitted: 3, finished: 3, failed: 0,
                  outstandingNow: 0, pendingAsyncCleanup: 0, state: "clean"
                }
              }
            }
          },
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
  assert.match(metricsEl.innerHTML, /2\/3 present/i);
  assert.match(metricsEl.innerHTML, /sheet-directory-discovery-audit\.json/i);
  assert.match(metricsEl.innerHTML, /Refresh artifacts/i);
  assert.doesNotMatch(metricsEl.innerHTML, /do-not-expose|rawSourceRows/i);
  // The view model carries the new fields end to end: the row keeps its fingerprint and the
  // per-source projection travels through the compact view model rather than being dropped.
  assert.match(metricsEl.innerHTML, /page-fetch-lifecycle/i);
  assert.match(metricsEl.innerHTML, /Page fetch lifecycle/i);
  assert.match(metricsEl.innerHTML, /gameprog/i);
  assert.match(metricsEl.innerHTML, /3 page fetches/i);

  copyButton.click();
  refreshButton.click();

  assert.equal(copied.length, 1);
  assert.equal(copied[0].key, "auditArtifacts");
  assert.equal(copied[0].artifacts.length, 3);
  assert.equal(copied[0].artifacts[0].summary.candidatesCount, 1);
  // The copy-diagnostics path must carry the new fields through compactAuditArtifact:
  // fingerprint identity and the per-source page-fetch-lifecycle projection survive, so a
  // future revert that drops them from the allowlist fails this assertion (discriminating).
  const copiedPfl = copied[0].artifacts[1];
  assert.equal(copiedPfl.name, "page-fetch-lifecycle");
  assert.equal(copiedPfl.fingerprintKind, "sha256");
  assert.equal(copiedPfl.fingerprint, "def456");
  assert.equal(copiedPfl.pageFetchLifecycle.sources.gameprog.state, "clean");
  assert.equal(copiedPfl.pageFetchLifecycle.sources.gameprog.rollup.outstandingNow, 0);
  assert.equal(refreshed, 1);
});

test("admin render: the probe lifecycle receipt reads as abandoned fetches and an unclosed client", () => {
  const metricsEl = makeEl();

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    discoveryAuditArtifacts: {
      ok: true,
      artifacts: [
        {
          name: "gamedevmap-receipt",
          exists: true,
          pathDisplay: "/data/gamedevmap-audit-receipt.json",
          sizeBytes: 412,
          modifiedAt: "2026-10-12T14:54:04Z",
          summary: {},
          probeLifecycle: {
            batches: 1,
            fetchesStarted: 17,
            fetchesBoundedOut: 2,
            fetchesOutstandingAtExit: 1,
            clientFetchesOutstandingAtExit: 1,
            clientsUnclosed: 1,
            deadlineExits: 0
          },
          warnings: []
        }
      ]
    }
  });

  assert.match(metricsEl.innerHTML, /Probe lifecycle/i);
  assert.match(metricsEl.innerHTML, /client left unclosed/i);
  assert.match(metricsEl.innerHTML, /17 probe fetches/i);
  assert.match(metricsEl.innerHTML, /2 abandoned by their own bound/i);
  assert.match(metricsEl.innerHTML, /1 inside the shared client/i);
  assert.match(metricsEl.innerHTML, /1 clients unclosed/i);
});

test("admin render: probe lifecycle KPI says so when no receipt is published, and when it is clean", () => {
  const pendingEl = makeEl();
  renderAdminOpsKpis(pendingEl, {}, "healthy", {});

  assert.match(pendingEl.innerHTML, /Probe lifecycle/i);
  assert.match(pendingEl.innerHTML, /no probe-wave receipt published/i);

  const cleanEl = makeEl();
  renderAdminOpsKpis(cleanEl, {
    probeLifecycle: {
      batches: 1,
      fetchesStarted: 17,
      fetchesBoundedOut: 0,
      fetchesOutstandingAtExit: 0,
      clientFetchesOutstandingAtExit: 0,
      clientsUnclosed: 0,
      deadlineExits: 0
    }
  }, "healthy", {});

  assert.match(cleanEl.innerHTML, /Probe lifecycle/i);
  assert.match(cleanEl.innerHTML, /admin-status-chip healthy">clean</i);
  assert.match(cleanEl.innerHTML, /0 clients unclosed/i);
  assert.doesNotMatch(cleanEl.innerHTML, /no probe-wave receipt published/i);
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
        expectedNegativeCount: 222,
        actionableDiagnosticCount: 92,
        highPriorityBuckets: [
          { key: "dedupe_skipped", count: 405, classification: "expected_skip" },
          { key: "gamedevmap_recovery_not_found", count: 158, classification: "expected_negative" }
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
  assert.match(metricsEl.innerHTML, /expected negatives 222/i);
  assert.match(metricsEl.innerHTML, /gamedevmap_recovery_not_found/i);
  assert.match(metricsEl.innerHTML, /expected negative/i);
  assert.doesNotMatch(metricsEl.innerHTML, /hidden\.invalid|raw should not render/i);

  copyButton.click();
  refreshButton.click();

  assert.equal(copied.length, 1);
  assert.equal(copied[0].key, "taskFailures");
  assert.equal(copied[0].fetch.partialWarningCount, 1);
  assert.equal(copied[0].discovery.expectedNegativeCount, 222);
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

test("admin render: the page-fetch receipt reads per source and marks a truncated reading", () => {
  const metricsEl = makeEl();

  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {},
    history: {},
    discoveryAuditArtifacts: {
      ok: true,
      artifacts: [
        {
          name: "page-fetch-lifecycle",
          exists: true,
          pathDisplay: "/data/page-fetch-lifecycle.v1.json",
          sizeBytes: 2048,
          modifiedAt: "2026-10-13T09:00:00Z",
          summary: {},
          pageFetchLifecycle: {
            runId: "run-42",
            coverage: "partial",
            truncated: true,
            omittedCount: 7,
            sources: {
              gameprog: {
                source: "gameprog",
                runId: "run-42",
                state: "pending",
                coverage: "partial",
                truncated: true,
                omittedCount: 7,
                receiptCount: 3,
                rollup: {
                  batchCount: 3,
                  submitted: 12,
                  finished: 9,
                  failed: 1,
                  outstandingNow: 2,
                  pendingAsyncCleanup: 1,
                  state: "pending"
                }
              },
              gamesmap: {
                source: "gamesmap",
                runId: "run-42",
                state: "clean",
                coverage: "full",
                truncated: false,
                omittedCount: 0,
                receiptCount: 1,
                rollup: {
                  batchCount: 1,
                  submitted: 4,
                  finished: 4,
                  failed: 0,
                  outstandingNow: 0,
                  pendingAsyncCleanup: 0,
                  state: "clean"
                }
              }
            }
          },
          warnings: []
        }
      ]
    }
  });

  const html = metricsEl.innerHTML;
  assert.match(html, /Page fetch lifecycle/i);
  assert.match(html, /gameprog/i);
  assert.match(html, /gamesmap/i);
  assert.match(html, /12 page fetches/i);
  assert.match(html, /2 still out when the batch left/i);
  // A truncated reading must never render as clean, and the omitted count is visible.
  assert.match(html, /reading truncated/i);
  assert.match(html, /7 omitted/i);
});

test("admin render: page-fetch KPI says so when no receipt is published", () => {
  const pendingEl = makeEl();
  renderAdminOpsKpis(pendingEl, {}, "healthy", {});

  assert.match(pendingEl.innerHTML, /Page fetch lifecycle/i);
  assert.match(pendingEl.innerHTML, /no page-fetch receipt published/i);

  const cleanEl = makeEl();
  renderAdminOpsKpis(
    cleanEl,
    {
      pageFetchLifecycle: {
        runId: "run-1",
        sources: {
          gameprog: {
            source: "gameprog",
            state: "clean",
            coverage: "full",
            truncated: false,
            omittedCount: 0,
            receiptCount: 1,
            rollup: { batchCount: 1, submitted: 3, finished: 3, failed: 0, outstandingNow: 0, pendingAsyncCleanup: 0, state: "clean" }
          }
        }
      }
    },
    "healthy",
    {}
  );

  assert.doesNotMatch(cleanEl.innerHTML, /no page-fetch receipt published/i);
  assert.match(cleanEl.innerHTML, /3 page fetches/i);
});
