import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsHistory } from "../../../frontend/admin/render.js";
import { createRenderEl as makeEl } from "./helpers/dom-test-helpers.mjs";

test("admin ops history: completed rows omit inline detail disclosures", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [],
    visibleCompletedRows: [
      {
        type: "fetch",
        status: "warning",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "2026-03-08T10:04:00.000Z",
        durationMs: 240000,
        taskProgress: {
          active: false,
          phaseKey: "complete",
          phaseLabel: "Complete",
          mode: "determinate",
          ratio: 1,
          counts: {
            resolvedSources: 9,
            sourceCount: 9,
            completedSources: 9,
            failedSources: 1
          }
        },
        summary: {
          outputCount: 120,
          failedSources: 1,
          okWithWarningSources: 2
        }
      },
      {
        type: "sync",
        status: "error",
        finishedAt: "2026-03-08T09:30:00.000Z",
        durationMs: 1500,
        summary: {
          action: "push",
          activeCount: 7,
          pendingCount: 2,
          rejectedCount: 1,
          error: "remote rejected test payload"
        }
      }
    ],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.match(historyEl.innerHTML, /Progress \/ Summary/);
  assert.doesNotMatch(historyEl.innerHTML, /<details class="admin-ops-run-detail"/i);
  assert.doesNotMatch(historyEl.innerHTML, /Fetcher details/i);
  assert.doesNotMatch(historyEl.innerHTML, /Sync details/i);
  assert.match(historyEl.innerHTML, /Sync push/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button/i);
  assert.doesNotMatch(historyEl.innerHTML, /raw payload/i);
});

test("admin ops history: sync row without counts shows awaiting progress instead of zero tuple", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "sync",
        runId: "sync_starting",
        active: true,
        isLive: true,
        status: "running",
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: {
          action: "pull",
          automatic: true,
          reason: "startup"
        },
        taskProgress: {
          active: true,
          counts: {}
        }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /Sync pull \(awaiting progress\)/i);
  assert.doesNotMatch(historyEl.innerHTML, /0\/0\/0/);
  assert.doesNotMatch(historyEl.innerHTML, /active 0 \/ pending 0 \/ rejected 0/i);
});

test("admin ops history: lifecycle statuses drive terminal chip labels", () => {
  const historyEl = makeEl();

  renderAdminOpsHistory(historyEl, {
    currentRows: [],
    visibleCompletedRows: [
      {
        type: "fetch",
        status: "ok",
        lifecycleStatus: "succeeded",
        runId: "fetch_lifecycle_success_1",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "2026-03-08T10:02:00.000Z",
        summary: { outputCount: 12, failedSources: 0 }
      },
      {
        type: "pipeline",
        status: "error",
        lifecycleStatus: "orphaned",
        runId: "pipeline_orphan_1",
        startedAt: "2026-03-08T09:00:00.000Z",
        finishedAt: "2026-03-08T09:10:00.000Z",
        summary: { error: "owner_inactive_without_terminal_report" }
      },
      {
        type: "sync",
        status: "canceled",
        lifecycleStatus: "canceled",
        runId: "sync_cancel_1",
        startedAt: "2026-03-08T08:00:00.000Z",
        finishedAt: "2026-03-08T08:01:00.000Z",
        summary: { action: "pull" }
      }
    ],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, />succeeded</i);
  assert.match(historyEl.innerHTML, />orphaned</i);
  assert.match(historyEl.innerHTML, />canceled</i);
  assert.match(historyEl.innerHTML, /admin-status-chip critical/i);
  assert.match(historyEl.innerHTML, /admin-status-chip warning/i);
});

test("admin ops history: pipeline rows keep progress text visible and cap overflow rows", () => {
  const historyEl = makeEl();
  const nowMs = Date.now();
  const startedAt = new Date(nowMs - (3 * 60 * 1000)).toISOString();
  const heartbeatAt = new Date(nowMs - (30 * 1000)).toISOString();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "pipeline",
        active: true,
        startedAt,
        heartbeatAt,
        summary: {
          currentStep: 3,
          totalSteps: 7,
          baselineOutputCount: 120,
          finalOutputCount: 240
        },
        taskProgress: {
          active: true,
          phaseKey: "transforming_snapshot",
          phaseLabel: "Transforming snapshot",
          mode: "determinate",
          ratio: 0.5,
          counts: {
            currentStep: 3,
            totalSteps: 7,
            baselineOutputCount: 120,
            finalOutputCount: 240
          }
        }
      },
      ...Array.from({ length: 10 }, (_row, index) => ({
        type: "fetch",
        active: true,
        startedAt: new Date(nowMs - ((index + 4) * 60 * 1000)).toISOString(),
        heartbeatAt: new Date(nowMs - ((index + 4) * 60 * 1000) + 30_000).toISOString(),
        summary: { outputCount: index + 1, failedSources: 0 }
      }))
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /pipeline/i);
  assert.match(historyEl.innerHTML, /step 3\/7/i);
  assert.match(historyEl.innerHTML, /output 240 \(baseline 120\)/i);
  assert.match(historyEl.innerHTML, /Show all 11 runs/i);
  assert.match(historyEl.innerHTML, /admin-ops-expand-capped/);
});

test("admin ops history: pipeline no-progress scenarios show neutral fallback", () => {
  const cases = [
    {
      caseId: "live-without-progress-evidence",
      model: {
        currentRows: [{
          type: "pipeline",
          active: true,
          startedAt: new Date(Date.now() - 60_000).toISOString(),
          heartbeatAt: new Date().toISOString(),
          summary: {},
          taskProgress: { active: true, phaseKey: "", phaseLabel: "", counts: {} }
        }],
        visibleCompletedRows: [],
        olderCompletedRows: []
      },
      matches: [/Pipeline running/],
      nonMatches: [/step 0/i, /output 0 \(baseline 0\)/i]
    },
    {
      caseId: "completed-zero-progress",
      model: {
        currentRows: [],
        visibleCompletedRows: [{
          type: "pipeline",
          status: "ok",
          startedAt: "2026-03-08T10:00:00.000Z",
          finishedAt: "2026-03-08T10:02:00.000Z",
          taskProgress: { active: false, counts: { currentStep: 0, baselineOutputCount: 0, finalOutputCount: 0 } },
          summary: { currentStep: 0, baselineOutputCount: 0, finalOutputCount: 0 }
        }],
        olderCompletedRows: []
      },
      matches: [/Pipeline completed/],
      nonMatches: [/step 0/i, /output 0 \(baseline 0\)/i]
    }
  ];

  for (const { caseId, model, matches, nonMatches } of cases) {
    const historyEl = makeEl();
    renderAdminOpsHistory(historyEl, model);
    for (const pattern of matches) assert.match(historyEl.innerHTML, pattern, caseId);
    for (const pattern of nonMatches) assert.doesNotMatch(historyEl.innerHTML, pattern, caseId);
  }
});

// The Finished column is the one that used to be clamped: its track could not fit
// `9/21/2026, 9:58:38 AM`, so every value ellipsized. The compact form fits, and
// the full localized stamp rides along as a tooltip so nothing is lost.
test("admin ops history: the finished column stays compact and keeps the full stamp", () => {
  const nowMs = Date.now();
  const recent = new Date(nowMs - (60 * 60 * 1000));
  const previousYear = new Date("2019-09-21T14:53:00.000Z");

  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [],
    visibleCompletedRows: [
      { type: "fetch", status: "ok", runId: "recent_1", startedAt: recent.toISOString(), finishedAt: recent.toISOString(), summary: { outputCount: 1 } },
      { type: "fetch", status: "ok", runId: "old_1", startedAt: previousYear.toISOString(), finishedAt: previousYear.toISOString(), summary: { outputCount: 1 } }
    ],
    olderCompletedRows: []
  });

  const cells = Array.from(historyEl.innerHTML.matchAll(/<div class="admin-cell" data-tooltip="([^"]*)">([^<]*)<\/div>/g));
  const finished = cells.map((match) => ({ title: match[1], text: match[2] }));

  // The current-year stamp omits the year; an older one keeps it. The month is
  // `\w{3,4}` because the locale abbreviation is not always three letters
  // (September renders as "Sept").
  const recentCell = finished.find((cell) => /^\d{2} \w{3,4} \d{2}:\d{2}$/.test(cell.text));
  const oldCell = finished.find((cell) => /^21 \w{3,4} 2019 \d{2}:\d{2}$/.test(cell.text));
  assert.ok(recentCell, `a current-year stamp must render as "dd Mon HH:MM", got ${JSON.stringify(finished)}`);
  assert.ok(oldCell, `a prior-year stamp must carry its year, got ${JSON.stringify(finished)}`);
  assert.ok(recentCell.title.length > recentCell.text.length, "the full stamp must survive in the tooltip");
});
