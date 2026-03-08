import test from "node:test";
import assert from "node:assert/strict";
import {
  renderAdminOpsAlerts,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsTrends,
  renderAdminOpsHistory
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
        type: "fetch",
        status: "ok",
        durationMs: 2100,
        finishedAt: "2026-03-08T08:00:00.000Z",
        summary: { outputCount: 15, failedSources: 0 }
      }
    ]
  });
  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.match(historyEl.innerHTML, /Current Runs/);
  assert.match(historyEl.innerHTML, /Older runs \(2\)/);
  assert.match(historyEl.innerHTML, /running/);
  assert.match(historyEl.innerHTML, /critical/);
  assert.match(historyEl.innerHTML, />42</);
  assert.match(historyEl.innerHTML, /Queued \(new\): 5/);
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
