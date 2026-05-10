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
          counts: {
            stageIndex: 7,
            stageTotal: 11,
            subtaskKey: "gamedevmap_active_audit",
            subtaskLabel: "GameDevMap active audit",
            activeAuditPhase: "homepage_fetch",
            activeAuditPhaseCompleted: 553,
            activeAuditPhaseTotal: 1000
          }
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
  assert.match(historyEl.innerHTML, /GameDevMap active dry run/);
  assert.match(historyEl.innerHTML, /homepage fetch 553\/1[,.]000 pages/);
  assert.match(historyEl.innerHTML, /stage 7\/11/);
  assert.match(historyEl.innerHTML, /Discovery: Scanning GameDevMap directory/);
});

test("admin ops history: current discovery rows show active audit URL progress", () => {
  const historyEl = makeEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "discovery",
        runId: "discovery_live_audit_1",
        active: true,
        isLive: true,
        displayStatus: "running",
        startedAt: "2026-03-08T10:00:00.000Z",
        summary: { queuedCandidateCount: 0, failedProbeCount: 0 },
        taskProgress: {
          active: true,
          phaseKey: "scanning_sources",
          phaseLabel: "Preparing GameDevMap active audit",
          mode: "determinate",
          ratio: 0.25,
          counts: {
            stageIndex: 7,
            stageTotal: 11,
            subtaskKey: "gamedevmap_active_audit",
            subtaskLabel: "GameDevMap active audit",
            activeAuditPhase: "batch_start",
            activeAuditCompletedUrls: 2000,
            activeAuditTotalUrls: 7524,
            activeAuditBatch: 2,
            activeAuditPhaseCompleted: 2000,
            activeAuditPhaseTotal: 7524
          }
        }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.match(historyEl.innerHTML, /Preparing GameDevMap active audit/);
  assert.match(historyEl.innerHTML, /GameDevMap active audit/);
  assert.match(historyEl.innerHTML, /batch 2/);
  assert.match(historyEl.innerHTML, /2[,.]000\/7[,.]524 URLs/);
  assert.match(historyEl.innerHTML, /batch start 2[,.]000\/7[,.]524/);
});
