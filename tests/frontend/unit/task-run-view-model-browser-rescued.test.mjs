// A discovery run reports failed probes and the subset the browser stage re-read.
// The admin drawer can only show that split if the view model passes the count
// through, so this pins the key against the allowed-summary-key list.

import test from "node:test";
import assert from "node:assert/strict";

import { buildTaskRunAnalysis } from "../../../frontend/shared/task-run-view-model.js";

test("task run analysis carries the browser-rescued split next to the failure count", () => {
  const analysis = buildTaskRunAnalysis({
    type: "discovery",
    runId: "discovery_selected_rescued",
    status: "warning",
    finishedAt: "2026-03-08T10:00:00.000Z",
    summary: { failedProbeCount: 5, failuresResolvedByBrowser: 2, queuedCandidateCount: 1 }
  }, {
    rowArea: "completed",
    nowMs: Date.parse("2026-03-08T10:10:00.000Z")
  });

  assert.equal(analysis.summaryCounts.failedProbeCount, 5);
  assert.equal(analysis.summaryCounts.failuresResolvedByBrowser, 2);
});
