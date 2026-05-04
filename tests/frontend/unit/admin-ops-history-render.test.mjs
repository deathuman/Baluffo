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
    onCopyRunDiagnostics: payload => copied.push(payload)
  });

  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
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
