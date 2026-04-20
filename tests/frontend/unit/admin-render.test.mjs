import test from "node:test";
import assert from "node:assert/strict";
import {
  renderAdminOpsAlerts,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsTrends,
  renderAdminOpsHistory,
  renderSourcesTableHtml
} from "../../../frontend/admin/render.js";

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
  assert.match(alertsEl.innerHTML, /No active alerts/i);

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

test("admin render: schedule/trends/history render deterministic core text", () => {
  const scheduleEl = makeEl();
  renderAdminOpsSchedule(
    scheduleEl,
    {
      fetcher: { intervalHours: 6, nextRunAt: "2026-03-08T10:00:00.000Z" },
      discovery: { note: "manual_task" }
    },
    { kpis: { lastRunResult: { type: "fetch", status: "ok", finishedAt: "2026-03-08T08:00:00.000Z" } } }
  );
  assert.match(scheduleEl.innerHTML, /every 6h/i);
  assert.match(scheduleEl.innerHTML, /manual task/i);
  assert.match(scheduleEl.innerHTML, /fetch ok/i);

  const trendsEl = makeEl();
  renderAdminOpsTrends(trendsEl, [
    { type: "fetch", status: "ok", finishedAt: "2026-03-07T08:00:00.000Z", summary: { outputCount: 100, failedSources: 4 } },
    { type: "fetch", status: "ok", finishedAt: "2026-03-08T08:00:00.000Z", summary: { outputCount: 120, failedSources: 2 } }
  ]);
  assert.match(trendsEl.innerHTML, /admin-ops-trend-chart/);
  assert.match(trendsEl.innerHTML, /output.*\+20/i);
  assert.match(trendsEl.innerHTML, /failed sources.*-2/i);

  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        displayStatus: "running",
        elapsedMs: 5000,
        startedAt: "2026-03-08T11:00:00.000Z",
        summary: { outputCount: 42, failedSources: 1 }
      }
    ],
    visibleCompletedRows: [
      {
        type: "discovery",
        status: "error",
        durationMs: 950,
        finishedAt: "2026-03-08T09:00:00.000Z",
        summary: { queuedCandidateCount: 5, failedProbeCount: 2 }
      }
    ],
    olderCompletedRows: [
      {
        type: "sync",
        status: "ok",
        durationMs: 700,
        finishedAt: "2026-03-08T08:30:00.000Z",
        summary: { action: "pull", activeCount: 12, pendingCount: 4, rejectedCount: 1 }
      },
      {
        type: "fetch",
        status: "ok",
        durationMs: 2100,
        finishedAt: "2026-03-08T08:00:00.000Z",
        summary: { outputCount: 15, failedSources: 0 }
      }
    ]
  });
  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.match(historyEl.innerHTML, /Runs/);
  assert.match(historyEl.innerHTML, /Older runs \(2\)/);
  assert.match(historyEl.innerHTML, /running/);
  assert.match(historyEl.innerHTML, /critical/);
  assert.match(historyEl.innerHTML, />42</);
  assert.match(historyEl.innerHTML, /Queued \(new\): 5/);
  assert.match(historyEl.innerHTML, /Sync pull/i);
});

test("admin render: live discovery ops history keeps only the primary phase text", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "discovery",
        displayStatus: "running",
        isLive: true,
        elapsedMs: 5000,
        startedAt: "2026-03-08T11:00:00.000Z",
        taskProgress: {
          active: true,
          phaseKey: "probing_candidates",
          phaseLabel: "gamemap",
          mode: "determinate",
          ratio: 0.5,
          counts: {
            foundEndpoints: 12,
            probedCandidates: 5,
            probeTotal: 10,
            queuedCandidates: 3
          }
        },
        summary: { failedProbeCount: 1 }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /gamemap \(50%\)/i);
  assert.doesNotMatch(historyEl.innerHTML, /found/i);
  assert.doesNotMatch(historyEl.innerHTML, /endpoints/i);
  assert.doesNotMatch(historyEl.innerHTML, /probed/i);
});

test("admin render: live fetch ops history appends scrapy fallback badge", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        displayStatus: "running",
        isLive: true,
        elapsedMs: 5000,
        startedAt: "2026-03-08T11:00:00.000Z",
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "determinate",
          ratio: 0.5,
          counts: {
            resolvedSources: 550,
            sourceCount: 551
          }
        },
        workItems: [
          {
            id: "scrapy_static_sources",
            status: "running",
            progress: {
              active: true,
              phaseKey: "loading_source",
              phaseLabel: "Processing browser fallback queue",
              counts: {
                completedSources: 19,
                totalSources: 26
              }
            }
          }
        ],
        summary: { failedSources: 0 }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /Executing sources \(50%\) \| Browser fallback 19\/26/i);
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

  assert.match(excludedHtml, /admin-status-chip warning" title="Excluded: cache_within_freshness_window"/);
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
