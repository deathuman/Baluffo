import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsHistory } from "../../../frontend/admin/render.js";

function makeEl() {
  return {
    innerHTML: "",
    textContent: "",
    querySelectorAll: () => []
  };
}

function makeButton(attributeValue) {
  return {
    onclick: null,
    getAttribute(name) {
      return name === "data-ops-run-diagnostics-copy" ? attributeValue : "";
    },
    click() {
      if (typeof this.onclick === "function") this.onclick();
    }
  };
}

test("admin ops history: completed run details show warning, failure, and capped hints read-only", () => {
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
  assert.match(historyEl.innerHTML, /admin-ops-run-detail/);
  assert.match(historyEl.innerHTML, /Fetcher details/i);
  assert.match(historyEl.innerHTML, /completed with warnings/i);
  assert.match(historyEl.innerHTML, /2 source warnings?/i);
  assert.match(historyEl.innerHTML, /1 failed source/i);
  assert.match(historyEl.innerHTML, /9\/9 sources resolved/i);
  assert.match(historyEl.innerHTML, /Sync push/i);
  assert.match(historyEl.innerHTML, /remote rejected test payload/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button/i);
  assert.doesNotMatch(historyEl.innerHTML, /raw payload/i);
});

test("admin ops history: run diagnostics copy uses bounded payload without changing compact rows", () => {
  const runKey = "current||fetch_live_1|fetch|2026-03-08T10:00:00.000Z||0";
  const copyButton = makeButton(runKey);
  const historyEl = {
    innerHTML: "",
    textContent: "",
    querySelectorAll(selector) {
      return selector === "[data-ops-run-diagnostics-copy]" ? [copyButton] : [];
    }
  };
  const copied = [];

  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        runId: "fetch_live_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: {
          outputCount: 12,
          failedSources: 0,
          recommendedApiPayload: { hidden: true }
        },
        workItems: Array.from({ length: 8 }, (_row, index) => ({
          id: `source_${index}`,
          name: `Source ${index}`,
          status: index === 0 ? "running" : "pending",
          rawLargeThing: { hidden: true }
        }))
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey,
    onCopyRunDiagnostics: payload => copied.push(payload)
  });

  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.match(historyEl.innerHTML, /Selected Run Analysis/);
  assert.match(historyEl.innerHTML, /data-ops-run-diagnostics-copy=/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.doesNotMatch(historyEl.innerHTML, /role="progressbar"/i);

  copyButton.click();

  assert.equal(copied.length, 1);
  assert.equal(copied[0].kind, "admin_run_diagnostics");
  assert.equal(copied[0].rowArea, "current");
  assert.equal(copied[0].runId, "fetch_live_1");
  assert.equal(copied[0].workItemExamples.length, 5);
  const serialized = JSON.stringify(copied[0]);
  assert.doesNotMatch(serialized, /recommendedApiPayload|rawLargeThing/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button[^>]*>(?:Start|Stop|Retry|Clear|Cleanup|Lifecycle)/i);
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

test("admin ops history: selected run analysis renders bounded read-only evidence", () => {
  const historyEl = makeEl();
  const runKey = "current||fetch_selected_1|fetch|2026-03-08T10:00:00.000Z||0";

  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        runId: "fetch_selected_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: {
          outputCount: 42,
          failedSources: 1,
          slowestSources: Array.from({ length: 8 }, (_row, index) => ({
            sourceId: `slow_${index}`,
            durationMs: 1000 + index
          }))
        },
        workItems: Array.from({ length: 8 }, (_row, index) => ({
          id: `source_${index}`,
          name: `Source ${index}`,
          status: index === 0 ? "running" : index === 1 ? "failed" : "pending",
          error: index === 1 ? "source failed after timeout" : "",
          updatedAt: index === 1 ? "2026-03-08T10:03:00.000Z" : ""
        })),
        recentEvents: Array.from({ length: 8 }, (_row, index) => ({
          at: `2026-03-08T10:0${Math.min(index, 5)}:00.000Z`,
          level: "info",
          message: `Event ${index}`
        }))
      }
    ],
    visibleCompletedRows: [
      {
        type: "sync",
        status: "ok",
        runId: "sync_done_1",
        finishedAt: "2026-03-08T09:30:00.000Z",
        durationMs: 1500,
        summary: { action: "push", activeCount: 7, pendingCount: 2, rejectedCount: 1 }
      }
    ],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey
  });

  assert.match(historyEl.innerHTML, /Selected Run Analysis/);
  assert.match(historyEl.innerHTML, /admin-ops-history-row-selected/);
  assert.match(historyEl.innerHTML, /fetch_selected_1/);
  assert.match(historyEl.innerHTML, /slow_4/);
  assert.doesNotMatch(historyEl.innerHTML, /slow_5/);
  assert.match(historyEl.innerHTML, /source_4/);
  assert.doesNotMatch(historyEl.innerHTML, /source_5/);
  assert.match(historyEl.innerHTML, /Event 4/);
  assert.doesNotMatch(historyEl.innerHTML, /Event 5/);
  assert.match(historyEl.innerHTML, /Timeline/);
  assert.match(historyEl.innerHTML, /source order|3\/8\/2026|2026/);
  assert.match(historyEl.innerHTML, /admin-ops-run-detail/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.doesNotMatch(historyEl.innerHTML, /role="progressbar"/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button[^>]*>(?:Start|Stop|Retry|Clear|Cleanup|Lifecycle)/i);
});

test("admin ops history: selected run analysis stays hidden without selection", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        runId: "fetch_live_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { outputCount: 12, failedSources: 0 }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.doesNotMatch(historyEl.innerHTML, /Selected Run Analysis/);
  assert.doesNotMatch(historyEl.innerHTML, /Select a run row to inspect bounded run evidence/);
  assert.doesNotMatch(historyEl.innerHTML, /Timeline/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-history-row-selected/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.doesNotMatch(historyEl.innerHTML, /role="progressbar"/i);
});

test("admin ops history: active rows do not render stale finished timestamps", () => {
  const historyEl = makeEl();
  const runKey = "current||discovery_live_1|discovery|2026-03-08T10:00:00.000Z||0";

  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "discovery",
        runId: "discovery_live_1",
        active: true,
        isLive: true,
        displayStatus: "running",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "2026-04-09T12:00:00.000Z",
        summary: { queuedCandidateCount: 4, failedProbeCount: 0 },
        taskProgress: {
          active: true,
          phaseKey: "probing",
          phaseLabel: "Probing candidates",
          counts: { queuedCandidates: 4 }
        }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey
  });

  assert.match(historyEl.innerHTML, /running/i);
  assert.match(historyEl.innerHTML, /Selected Run Analysis/);
  assert.doesNotMatch(historyEl.innerHTML, /<strong>Finished<\/strong>/);
  assert.doesNotMatch(historyEl.innerHTML, /4\/9\/2026|Apr/i);
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

test("admin ops history: selected run analysis renders timeline empty state", () => {
  const historyEl = makeEl();
  const runKey = "completed||sync_no_timeline|sync||2026-03-08T09:30:00.000Z|0";

  renderAdminOpsHistory(historyEl, {
    currentRows: [],
    visibleCompletedRows: [
      {
        type: "sync",
        status: "ok",
        runId: "sync_no_timeline",
        finishedAt: "2026-03-08T09:30:00.000Z",
        durationMs: 1500,
        summary: { action: "push", activeCount: 7, pendingCount: 2, rejectedCount: 1 }
      }
    ],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey
  });

  assert.match(historyEl.innerHTML, /Selected Run Analysis/);
  assert.match(historyEl.innerHTML, /Timeline/);
  assert.match(historyEl.innerHTML, /No timeline evidence recorded for this run/);
  assert.match(historyEl.innerHTML, /admin-ops-run-detail/);
  assert.doesNotMatch(historyEl.innerHTML, /<button[^>]*>(?:Start|Stop|Retry|Clear|Cleanup|Lifecycle)/i);
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
