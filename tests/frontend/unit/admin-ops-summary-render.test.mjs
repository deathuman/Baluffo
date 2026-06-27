import test from "node:test";
import assert from "node:assert/strict";

import {
  renderAdminOpsHistory,
  renderAdminOpsSchedule,
  renderAdminOpsTrends
} from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

test("admin render: schedule/trends/history render deterministic core text", () => {
  const scheduleEl = makeEl();
  renderAdminOpsSchedule(
    scheduleEl,
    {
      pipeline: {
        enabled: true,
        intervalHours: 24,
        pending: false,
        due: false,
        nextRunAt: "2026-03-09T10:00:00.000Z"
      },
      fetcher: { intervalHours: 6, nextRunAt: "2026-03-08T10:00:00.000Z" },
      discovery: { note: "manual_task" }
    },
    { kpis: { lastRunResult: { type: "fetch", status: "ok", finishedAt: "2026-03-08T08:00:00.000Z" } } }
  );
  assert.match(scheduleEl.innerHTML, /Pipeline/i);
  assert.match(scheduleEl.innerHTML, /data-action="save-pipeline-schedule"/i);
  assert.doesNotMatch(scheduleEl.innerHTML, /<strong>Fetcher<\/strong>/i);
  assert.doesNotMatch(scheduleEl.innerHTML, /<strong>Discovery<\/strong>/i);
  assert.doesNotMatch(scheduleEl.innerHTML, /<strong>Last Run<\/strong>/i);

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
        startedAt: "2026-03-08T08:58:00.000Z",
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
  assert.match(historyEl.innerHTML, /Current Runs/);
  assert.match(historyEl.innerHTML, /Recent Runs/);
  assert.match(historyEl.innerHTML, /admin-ops-history-recent/);
  assert.doesNotMatch(historyEl.innerHTML, /<details[^>]*admin-ops-history-recent[^>]*open/i);
  assert.match(historyEl.innerHTML, /Older runs \(2\)/);
  assert.match(historyEl.innerHTML, /admin-ops-history-older-scroll/);
  assert.match(historyEl.innerHTML, /running/);
  assert.match(historyEl.innerHTML, /critical/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.match(historyEl.innerHTML, />Fetch</);
  assert.match(historyEl.innerHTML, />Discovery</);
  assert.match(historyEl.innerHTML, />Sync</);
  assert.match(historyEl.innerHTML, />42</);
  assert.match(historyEl.innerHTML, /Review queue: 5/);
  assert.match(historyEl.innerHTML, /Sync pull/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button/i);
});

test("admin render: pipeline schedule state text covers key states", () => {
  const cases = [
    [{ enabled: false, intervalHours: 24 }, /disabled/i],
    [{ enabled: true, intervalHours: 24, pending: true, due: true }, /pending; waiting for idle/i],
    [{ enabled: true, intervalHours: 24, pending: false, due: true }, /due now/i],
    [
      {
        enabled: true,
        intervalHours: 11,
        pending: false,
        due: true,
        nextRunAt: "2026-03-09T10:00:00.000Z"
      },
      /every 11h, next/i
    ],
    [
      {
        enabled: true,
        intervalHours: 11,
        pending: false,
        due: true,
        nextRunAt: "2026-03-09T09:00:00.000Z",
        nextAfterCurrentCompletes: true,
        pipeline: { active: true, stage: "fetch" }
      },
      /every 11h, running now; next after this pipeline finishes/i
    ],
    [
      {
        enabled: true,
        intervalHours: 12,
        pending: false,
        due: false,
        nextRunAt: "2026-03-09T10:00:00.000Z"
      },
      /every 12h, next/i
    ],
    [
      {
        enabled: true,
        intervalHours: 11,
        pending: false,
        due: true,
        nextRunAt: "2099-03-09T10:00:00.000Z"
      },
      /every 11h, next/i
    ],
    [
      {
        enabled: true,
        intervalHours: 12,
        pending: false,
        due: false,
        nextRunAt: ""
      },
      /Pipeline<\/strong>: every 12h/i
    ],
    [
      {
        enabled: true,
        intervalHours: 12,
        pending: false,
        due: false,
        nextRunAt: "not-a-date"
      },
      /Pipeline<\/strong>: every 12h/i
    ],
    [
      {
        enabled: true,
        intervalHours: 12,
        pending: false,
        due: false,
        nextRunAt: "",
        nextAfterCurrentCompletes: true,
        pipeline: { active: true, stage: "fetch" }
      },
      /every 12h, running now; next after this pipeline finishes/i
    ]
  ];

  for (const [pipeline, expectedText] of cases) {
    const scheduleEl = makeEl();
    renderAdminOpsSchedule(scheduleEl, { pipeline }, {});
    assert.match(scheduleEl.innerHTML, expectedText);
    assert.doesNotMatch(scheduleEl.innerHTML, /next unknown/i);
  }
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
  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.doesNotMatch(historyEl.innerHTML, /role="progressbar"/i);
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
  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.doesNotMatch(historyEl.innerHTML, /aria-valuenow="50"/i);
});
