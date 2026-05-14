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

test("admin ops history: stalled and orphaned current runs render read-only compact rows", () => {
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

  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.match(
    historyEl.innerHTML,
    /<span class="admin-status-chip warning" data-tooltip="Check bridge and task logs; verify whether the task heartbeat stopped\.">stalled<\/span>/
  );
  assert.match(
    historyEl.innerHTML,
    /<span class="admin-status-chip critical" data-tooltip="Refresh task state and check whether the owning process exited\.">orphaned<\/span>/
  );
  assert.equal((historyEl.innerHTML.match(/Check bridge and task logs/g) || []).length, 1);
  assert.equal((historyEl.innerHTML.match(/owning process exited/g) || []).length, 1);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-detail/);
  assert.match(historyEl.innerHTML, /No recent heartbeat/);
  assert.doesNotMatch(historyEl.innerHTML, /Task state has no active owner/);
  assert.doesNotMatch(historyEl.innerHTML, /<button/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button[^>]*>(?:Start|Stop|Retry|Clear)/i);
});

test("admin ops history: running current run renders compact row without remediation guidance", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        active: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { outputCount: 12, failedSources: 0 }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.match(historyEl.innerHTML, /running/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card-remediation/);
  assert.doesNotMatch(historyEl.innerHTML, /Check bridge and task logs/);
  assert.doesNotMatch(historyEl.innerHTML, /owning process exited/);
  assert.doesNotMatch(historyEl.innerHTML, /<button/i);
});

test("admin ops history: waiting state renders a loading indicator and approaching rows stay distinct", () => {
  const waitingHistory = makeEl();
  renderAdminOpsHistory(waitingHistory, {
    currentRows: [],
    visibleCompletedRows: [],
    olderCompletedRows: []
  }, {
    waitingForTaskState: true
  });

  assert.match(waitingHistory.innerHTML, /Waiting for task state/i);
  assert.doesNotMatch(waitingHistory.innerHTML, /No run history yet/i);

  const approachingHistory = makeEl();
  const heartbeatAt = new Date(Date.now() - (8 * 60 * 1000)).toISOString();
  renderAdminOpsHistory(approachingHistory, {
    currentRows: [
      {
        type: "fetch",
        active: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt,
        summary: { outputCount: 12, failedSources: 0 }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(approachingHistory.innerHTML, /admin-ops-history-row-approaching/);
  assert.match(approachingHistory.innerHTML, /Heartbeat aging/i);
  assert.match(approachingHistory.innerHTML, /admin-status-chip warning/);
});
