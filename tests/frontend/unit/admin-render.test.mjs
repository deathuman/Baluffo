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
    { type: "fetch", summary: { outputCount: 100, failedSources: 4 } },
    { type: "fetch", summary: { outputCount: 120, failedSources: 2 } }
  ]);
  assert.match(trendsEl.textContent, /output.*\+20/i);
  assert.match(trendsEl.textContent, /failed sources.*-2/i);

  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, [
    {
      type: "discovery",
      status: "error",
      durationMs: 950,
      finishedAt: "2026-03-08T09:00:00.000Z",
      summary: { queuedCandidateCount: 5, failedProbeCount: 2 }
    },
    {
      type: "fetch",
      status: "ok",
      durationMs: 2100,
      finishedAt: "2026-03-08T11:00:00.000Z",
      summary: { outputCount: 42, failedSources: 1 }
    }
  ]);
  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.match(historyEl.innerHTML, /critical/);
  assert.match(historyEl.innerHTML, />42</);
  assert.match(historyEl.innerHTML, />5</);
});
