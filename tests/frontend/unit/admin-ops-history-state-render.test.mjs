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

test("admin ops history: stalled and orphaned current runs render read-only state cards", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        active: true,
        startedAt: "2026-03-08T09:00:00.000Z",
        heartbeatAt: "2026-03-08T09:30:00.000Z",
        summary: { outputCount: 12, failedSources: 0 }
      },
      {
        type: "discovery",
        status: "started",
        startedAt: "2026-03-08T09:00:00.000Z",
        summary: { queuedCandidateCount: 4, failedProbeCount: 1 }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /admin-ops-run-card-stalled/);
  assert.match(historyEl.innerHTML, /admin-ops-run-card-orphaned/);
  assert.match(historyEl.innerHTML, /<span class="admin-status-chip warning">stalled<\/span>/);
  assert.match(historyEl.innerHTML, /<span class="admin-status-chip critical">orphaned<\/span>/);
  assert.match(historyEl.innerHTML, /No recent heartbeat/);
  assert.match(historyEl.innerHTML, /Task state has no active owner/);
  assert.doesNotMatch(historyEl.innerHTML, /<button/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button[^>]*>(?:Start|Stop|Retry|Clear)/i);
});
