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
