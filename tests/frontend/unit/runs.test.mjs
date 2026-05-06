import test from "node:test";
import assert from "node:assert/strict";

import {
  deriveAdminRunsModel,
  normalizeOpsRuns
} from "../../../frontend/admin/domain/runs.js";

test("normalizeOpsRuns and deriveAdminRunsModel keep shared run fields aligned", () => {
  const nowMs = Date.parse("2026-03-08T10:10:00.000Z");
  const liveRow = {
    type: "fetch",
    taskType: "fetch",
    runId: "fetch_live_1",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    finishedAt: "2026-03-08T10:02:00.000Z",
    status: "running",
    summary: {
      outputCount: 42,
      failedSources: 1
    },
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "determinate",
      ratio: 0.5,
      counts: {
        resolvedSources: 6,
        sourceCount: 12,
        outputCount: 42,
        failedSources: 1
      }
    }
  };
  const completedRow = {
    id: "sync_done_1",
    runId: "sync_done_1",
    type: "sync",
    status: "ok",
    startedAt: "2026-03-08T09:00:00.000Z",
    finishedAt: "2026-03-08T09:05:00.000Z",
    durationMs: 300000,
    summary: {
      action: "push",
      activeCount: 7,
      pendingCount: 2,
      rejectedCount: 1
    }
  };
  const normalized = normalizeOpsRuns([liveRow, completedRow], nowMs);
  const model = deriveAdminRunsModel({
    taskState: { tasks: [liveRow] },
    historyRuns: [completedRow]
  }, nowMs);
  const pick = row => ({
    runId: row.runId,
    type: row.type,
    displayStatus: row.displayStatus,
    isLive: row.isLive,
    startedAt: row.startedAt,
    finishedAt: String(row.finishedAt || ""),
    elapsedMs: row.elapsedMs
  });

  assert.deepEqual(model.currentRows.map(pick), normalized.currentRows.map(pick));
  assert.deepEqual(model.visibleCompletedRows.map(pick), normalized.visibleCompletedRows.map(pick));
  assert.equal(model.currentRows[0].displayStatus, "running");
  assert.equal(model.currentRows[0].finishedAt, "");
  assert.equal(model.currentRows[0].elapsedMs, 600000);
  assert.equal(model.currentRows[0].runId, "fetch_live_1");
  assert.equal(model.visibleCompletedRows[0].type, "sync");
});
