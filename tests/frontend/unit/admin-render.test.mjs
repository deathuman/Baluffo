import test from "node:test";
import assert from "node:assert/strict";
import {
  renderAdminOpsAlerts,
  renderAdminOpsKpis,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsSchedule,
  renderSourcesTableHtml
} from "../../../frontend/admin/render.js";
import {
  deriveSourceApprovalStatus,
  deriveSourceStatus,
  getSourceDiscoveryJobsCount,
  getSourceJobsFoundCount
} from "../../../frontend/admin/domain.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("admin render: alerts and kpis render healthy/critical states", () => {
  const alertsEl = makeEl();
  renderAdminOpsAlerts(alertsEl, []);
  assert.equal(alertsEl.innerHTML, "");

  const kpisEl = makeEl();
  renderAdminOpsKpis(kpisEl, {
    sevenDayFetchSuccessRate: 0.91,
    failedSourceRatioLatest: 0.12,
    pendingApprovalsCount: 4,
    avgFetchDurationMs7d: 12345,
    lastSuccessfulFetchAge: "12m ago"
  }, "critical");
  assert.match(kpisEl.innerHTML, /admin-status-chip critical/);
  assert.match(kpisEl.innerHTML, /91\.0%/);
  assert.match(kpisEl.innerHTML, /12\.3s/);
});

test("admin render: non-dismissible alerts omit dismiss control", () => {
  const alertsEl = makeEl();
  renderAdminOpsAlerts(alertsEl, [
    { id: "fetch_never_run", severity: "warning", message: "No successful fetch yet.", dismissible: false }
  ]);

  assert.match(alertsEl.innerHTML, /No successful fetch yet/i);
  assert.doesNotMatch(alertsEl.innerHTML, /Dismiss/i);
});

test("admin render: fetcher metrics render failure buckets and examples", () => {
  const metricsEl = makeEl();
  renderAdminOpsFetcherMetrics(metricsEl, {
    latestRun: {
      durationMs: 240000,
      failedSources: 24,
      sourceCount: 90,
      sourceFailureRate: 24 / 90,
      duplicateRate: 0.06,
      outputYieldRate: 0.71,
      medianSourceDurationMs: 1200,
      p95SourceDurationMs: 45000,
      slowestSources: [{ name: "static_sources", durationMs: 31000 }],
      stageTop: [{ stage: "detailFetch", durationMs: 82000 }],
      highCostLowYieldSources: [{ name: "stormind", durationMs: 25000, keptCount: 0 }]
    },
    history: {
      windowRuns: 7,
      medianDurationMs: 180000,
      averageDurationMs: 210000
    }
  }, {
    topLevelFailedSources: 1,
    detailFailureCount: 1,
    buckets: [
      { key: "extract_zero", count: 1, examples: ["ashby_sources"] },
      { key: "provider_rate_limited", count: 1, examples: ["InnoGames (Personio)"] }
    ]
  });

  assert.match(metricsEl.innerHTML, /Top-level failed sources/i);
  assert.match(metricsEl.innerHTML, /admin-ops-fetcher-diagnostics/i);
  assert.doesNotMatch(metricsEl.innerHTML, /<details[^>]*admin-ops-fetcher-diagnostics[^>]*open/i);
  assert.match(metricsEl.innerHTML, /admin-ops-metrics-section-runtime/i);
  assert.match(metricsEl.innerHTML, /admin-ops-metrics-section-failures/i);
  assert.match(metricsEl.innerHTML, />Failures</i);
  assert.match(metricsEl.innerHTML, /Fetcher failure counts, buckets, and source examples/i);
  assert.match(metricsEl.innerHTML, /Grouped detail failures/i);
  assert.match(metricsEl.innerHTML, /Failure buckets/i);
  assert.match(metricsEl.innerHTML, /Extract Zero/i);
  assert.match(metricsEl.innerHTML, /Provider Rate Limited/i);
  assert.match(metricsEl.innerHTML, /InnoGames \(Personio\)/i);
  assert.match(metricsEl.innerHTML, /Latest Runtime/i);
  assert.match(metricsEl.innerHTML, /Slowest stages/i);
  assert.match(metricsEl.innerHTML, /High-cost low-yield/i);
});

test("admin render: signature patching skips redundant alerts/kpis/schedule rewrites", () => {
  const alertsEl = makeEl();
  alertsEl.dataset = {};
  const alerts = [{ id: "a1", severity: "warning", message: "x" }];
  renderAdminOpsAlerts(alertsEl, alerts);
  assert.ok(alertsEl.dataset.opsAlertsSig);
  alertsEl.innerHTML = `${alertsEl.innerHTML}<!--keep-->`;
  renderAdminOpsAlerts(alertsEl, alerts);
  assert.match(alertsEl.innerHTML, /<!--keep-->/);
  renderAdminOpsAlerts(alertsEl, [{ id: "a1", severity: "warning", message: "x", dismissible: false }]);
  assert.doesNotMatch(alertsEl.innerHTML, /<!--keep-->/);
  assert.doesNotMatch(alertsEl.innerHTML, /Dismiss/);

  const kpisEl = makeEl();
  kpisEl.dataset = {};
  const kpis = {
    sevenDayFetchSuccessRate: 0.9,
    failedSourceRatioLatest: 0.1,
    pendingApprovalsCount: 2,
    avgFetchDurationMs7d: 1000,
    lastSuccessfulFetchAge: "5m"
  };
  renderAdminOpsKpis(kpisEl, kpis, "healthy");
  assert.ok(kpisEl.dataset.opsKpisSig);
  kpisEl.innerHTML = `${kpisEl.innerHTML}<!--keep-->`;
  renderAdminOpsKpis(kpisEl, kpis, "healthy");
  assert.match(kpisEl.innerHTML, /<!--keep-->/);

  const scheduleEl = makeEl();
  scheduleEl.dataset = {};
  const schedule = { fetcher: { intervalHours: 6 }, discovery: { note: "manual_task" } };
  const latest = { kpis: { lastRunResult: { type: "fetch", status: "ok", finishedAt: "2026-03-08T08:00:00.000Z" } } };
  renderAdminOpsSchedule(scheduleEl, schedule, latest);
  assert.ok(scheduleEl.dataset.opsScheduleSig);
  scheduleEl.innerHTML = `${scheduleEl.innerHTML}<!--keep-->`;
  renderAdminOpsSchedule(scheduleEl, schedule, latest);
  assert.match(scheduleEl.innerHTML, /<!--keep-->/);
});

test("admin render: active excluded rows include a tooltip with the exclusion reason", () => {
  const excludedHtml = renderSourcesTableHtml(
    [
      {
        id: "source-1",
        name: "Excluded Source",
        adapter: "static",
        studio: "Studio A",
        status: "excluded",
        exclusionReason: "cache_within_freshness_window"
      }
    ],
    "active",
    row => Number(row.jobsFound || 0),
    row => row._lastStatus || row.status
  );

  assert.match(excludedHtml, /admin-status-chip warning" data-tooltip="Excluded: cache_within_freshness_window"/);
  assert.match(excludedHtml, />excluded<\/span>/i);

  const healthyHtml = renderSourcesTableHtml(
    [
      {
        id: "source-2",
        name: "Healthy Source",
        adapter: "static",
        studio: "Studio B",
        status: "ok"
      }
    ],
    "active",
    row => Number(row.jobsFound || 0),
    row => row._lastStatus || row.status
  );

  assert.doesNotMatch(healthyHtml, /Excluded:/);
  assert.doesNotMatch(healthyHtml, /Error:/);
});

test("admin render: pending and active warning statuses explain the warning on hover", () => {
  const pendingHtml = renderSourcesTableHtml(
    [
      {
        id: "source-warning-pending",
        name: "Pending Warning Source",
        adapter: "static",
        studio: "Studio Pending",
        status: "warning",
        warningReason: "latest probe returned partial metadata"
      }
    ],
    "pending",
    row => Number(row.jobsFound || 0),
    row => row._lastStatus || row.status
  );

  assert.match(
    pendingHtml,
    /admin-status-chip warning" data-tooltip="Warning: latest probe returned partial metadata">warning<\/span>/
  );

  const activeHtml = renderSourcesTableHtml(
    [
      {
        id: "source-warning-active",
        name: "Active Warning Source",
        adapter: "greenhouse",
        studio: "Studio Active",
        status: "warning",
        lastProbedAt: "2026-05-14T18:28:34.000Z"
      }
    ],
    "active",
    row => Number(row.jobsFound || 0),
    row => row._lastStatus || row.status
  );

  assert.match(
    activeHtml,
    /admin-status-chip warning" data-tooltip="Warning: source was probed, but no confirmed healthy check result is available yet\.">warning<\/span>/
  );
});

test("admin render: pending source table separates approval jobs from fetch health", () => {
  const html = renderSourcesTableHtml(
    [
      {
        id: "ashby:board_url:https://jobs.ashbyhq.com/scopely",
        name: "Scopely (Ashby)",
        adapter: "ashby",
        studio: "Scopely",
        jobsFound: 0,
        sampleCount: 0,
        _lastKeptCount: 179,
        _lastStatus: "ok"
      },
      {
        id: "static:listing_url:https://example.com/jobs",
        name: "Example (Manual Website)",
        adapter: "static",
        studio: "Example",
        jobsFound: 3,
        _lastStatus: "ok"
      }
    ],
    "pending",
    row => Number.isFinite(getSourceDiscoveryJobsCount(row)) ? getSourceDiscoveryJobsCount(row).toLocaleString() : "N/A",
    row => deriveSourceStatus(row),
    (row, mode) => deriveSourceApprovalStatus(row, mode)
  );

  assert.match(html, /<div class="admin-cell" data-label="Jobs">0<\/div>/);
  assert.match(html, /Blocked: 0 discovery jobs/);
  assert.match(html, /<div class="admin-cell" data-label="Jobs">3<\/div>/);
  assert.match(html, /Auto-approvable/);
});

test("admin render: active approval uses canonical stateChangedBy actor", () => {
  const html = renderSourcesTableHtml(
    [
      {
        id: "static:listing_url:https://careers.nintendo.com/jobs",
        name: "Nintendo (Manual Website)",
        adapter: "static",
        studio: "Nintendo",
        jobsFound: 12,
        approvedBy: "registry_migration_v2",
        stateChangedBy: "discovery_auto_approve"
      }
    ],
    "active",
    row => Number.isFinite(getSourceJobsFoundCount(row)) ? getSourceJobsFoundCount(row).toLocaleString() : "N/A",
    row => deriveSourceStatus(row),
    (row, mode) => deriveSourceApprovalStatus(row, mode)
  );

  assert.match(html, /Live: discovery_auto_approve/);
  assert.doesNotMatch(html, /Live: registry_migration_v2/);
});

test("admin render: source table virtual window caps rendered rows and adds spacers", () => {
  const rows = Array.from({ length: 120 }, (_, index) => ({
    id: `source-${index}`,
    name: `Source ${index}`,
    adapter: "static",
    studio: `Studio ${index}`,
    jobsFound: index,
    status: "ok"
  }));
  const html = renderSourcesTableHtml(
    rows,
    "active",
    row => Number(row.jobsFound || 0).toLocaleString(),
    row => row.status,
    () => ({ label: "Live", tone: "healthy" }),
    {
      virtual: true,
      startIndex: 40,
      endIndex: 71,
      rowHeightPx: 52,
      selectedSourceIds: new Set(["source-42"])
    }
  );

  const renderedRows = html.match(/admin-user-row admin-source-row/g) || [];
  assert.equal(renderedRows.length, 31);
  assert.match(html, /data-virtualized="true"/);
  assert.match(html, /data-window-start="40"/);
  assert.match(html, /height: 2080px/);
  assert.match(html, /Source 40/);
  assert.match(html, /Source 70/);
  assert.doesNotMatch(html, /Source 39/);
  assert.doesNotMatch(html, /Source 71/);
  assert.match(html, /data-source-id="source-42"[^>]* checked/);
});
