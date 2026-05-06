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

test("admin ops history: current rows show live discovery and pipeline child progress", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "discovery",
        runId: "discovery_live_progress_1",
        active: true,
        isLive: true,
        displayStatus: "running",
        startedAt: "2026-03-08T10:00:00.000Z",
        summary: { queuedCandidateCount: 0, failedProbeCount: 0 },
        taskProgress: {
          active: true,
          phaseKey: "scanning_sources",
          phaseLabel: "Scanning GameDevMap directory",
          mode: "indeterminate",
          ratio: 0,
          counts: { stageIndex: 7, stageTotal: 11 }
        }
      },
      {
        type: "pipeline",
        runId: "pipeline_live_progress_1",
        active: true,
        isLive: true,
        displayStatus: "running",
        startedAt: "2026-03-08T10:00:00.000Z",
        summary: {
          activeChildTaskType: "discovery",
          activeChildRunId: "discovery_live_progress_1",
          activeChildPhaseLabel: "Scanning GameDevMap directory",
          activeChildDisplayLabel: "Discovery: Scanning GameDevMap directory"
        },
        taskProgress: {
          active: true,
          phaseKey: "discovery_child",
          phaseLabel: "Discovery: Scanning GameDevMap directory",
          mode: "determinate",
          ratio: 1 / 3,
          counts: {
            currentStep: 1,
            totalSteps: 3,
            baselineOutputCount: 0,
            finalOutputCount: 0
          }
        }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /Progress \/ Summary/);
  assert.match(historyEl.innerHTML, /Scanning GameDevMap directory/);
  assert.match(historyEl.innerHTML, /Discovery: Scanning GameDevMap directory/);
});
