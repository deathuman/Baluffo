import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsHistory } from "../../../frontend/admin/render.js";

function makeAbortButtonsFromHtml(historyEl) {
  return Array.from(historyEl.innerHTML.matchAll(/data-ops-run-abort="([^"]+)"/g))
    .map(match => ({
      onclick: null,
      getAttribute(name) {
        return name === "data-ops-run-abort" ? match[1] : "";
      },
      click() {
        if (typeof this.onclick === "function") {
          this.onclick({
            preventDefault() {},
            stopPropagation() {}
          });
        }
      }
    }));
}

test("admin ops history: current abort actions are scoped to abortable live tasks", () => {
  let abortButtons = [];
  const aborted = [];
  const historyEl = {
    innerHTML: "",
    textContent: "",
    querySelectorAll(selector) {
      if (selector === "[data-ops-run-abort]") {
        abortButtons = makeAbortButtonsFromHtml(this);
        return abortButtons;
      }
      return [];
    }
  };

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
      },
      {
        type: "discovery",
        runId: "discovery_live_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:01:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { queuedCandidateCount: 4, failedProbeCount: 0 }
      },
      {
        type: "fetch",
        runId: "fetch_control_plane_1",
        active: true,
        isLive: true,
        controlPlaneSource: "pipeline-status",
        displayOnly: true,
        startedAt: "2026-03-08T10:01:30.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { controlPlane: true }
      },
      {
        type: "fetch",
        runId: "fetch_pipeline_child_1",
        active: true,
        isLive: true,
        parentTaskType: "pipeline",
        parentRunId: "pipeline_live_1",
        startedAt: "2026-03-08T10:01:45.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { pipelineRunId: "pipeline_live_1" }
      },
      {
        type: "pipeline",
        runId: "pipeline_live_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:02:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { currentStep: 2, totalSteps: 4 }
      },
      {
        type: "fetch",
        runId: "fetch_aborting_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:02:30.000Z",
        heartbeatAt: new Date().toISOString(),
        stage: "aborting",
        taskProgress: { phaseKey: "aborting", phaseLabel: "Aborting..." },
        summary: { abortRequestedAt: "2026-03-08T10:02:31.000Z" }
      },
      {
        type: "sync",
        runId: "sync_live_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:03:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { action: "push" }
      }
    ],
    visibleCompletedRows: [
      {
        type: "fetch",
        runId: "fetch_done_1",
        status: "ok",
        finishedAt: "2026-03-08T09:30:00.000Z",
        summary: { outputCount: 12, failedSources: 0 }
      }
    ],
    olderCompletedRows: []
  }, {
    onAbortRun: payload => aborted.push(payload)
  });

  assert.match(historyEl.innerHTML, /fetch_control_plane_1/);
  assert.match(historyEl.innerHTML, /fetch_pipeline_child_1/);
  assert.match(historyEl.innerHTML, /fetch_aborting_1/);
  assert.equal((historyEl.innerHTML.match(/data-ops-run-abort=/g) || []).length, 3);
  assert.match(historyEl.innerHTML, /admin-ops-run-abort-btn/);
  assert.equal(abortButtons.length, 3);

  abortButtons.forEach(button => button.click());

  assert.deepEqual(aborted.map(item => [item.taskType, item.runId]), [
    ["fetch", "fetch_live_1"],
    ["discovery", "discovery_live_1"],
    ["pipeline", "pipeline_live_1"]
  ]);
});
