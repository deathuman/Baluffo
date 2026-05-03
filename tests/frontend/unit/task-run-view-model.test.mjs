import test from "node:test";
import assert from "node:assert/strict";

import { buildTaskRunView } from "../../../frontend/shared/task-run-view-model.js";

const NOW = Date.parse("2026-03-08T10:10:00.000Z");

test("task run view model derives live fetch progress", () => {
  const view = buildTaskRunView({
    taskType: "fetch",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    heartbeatAt: "2026-03-08T10:09:30.000Z",
    summary: { outputCount: 42, failedSources: 1, sourceCount: 12 },
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
  }, { nowMs: NOW });

  assert.equal(view.taskType, "fetch");
  assert.equal(view.title, "Fetcher");
  assert.equal(view.status, "running");
  assert.equal(view.severity, "healthy");
  assert.equal(view.progressMode, "determinate");
  assert.equal(view.progressRatio, 0.5);
  assert.match(view.progressLabel, /Executing sources/i);
  assert.match(view.primaryLabel, /42 jobs/i);
  assert.match(view.failureSummary, /1 failed source/i);
});

test("task run view model derives discovery and sync summaries", () => {
  const discovery = buildTaskRunView({
    taskType: "discovery",
    active: true,
    startedAt: "2026-03-08T10:08:00.000Z",
    summary: { queuedCandidateCount: 7, failedProbeCount: 2 },
    taskProgress: {
      active: true,
      phaseKey: "probing_candidates",
      phaseLabel: "Probing candidates",
      mode: "determinate",
      ratio: 0.25,
      counts: {
        foundEndpoints: 12,
        probedCandidates: 3,
        probeTotal: 12,
        queuedCandidates: 7,
        failedProbes: 2
      }
    }
  }, { nowMs: NOW });
  const sync = buildTaskRunView({
    taskType: "sync",
    active: true,
    startedAt: "2026-03-08T10:09:00.000Z",
    summary: { action: "push", activeCount: 10, pendingCount: 3, rejectedCount: 1 }
  }, { nowMs: NOW });

  assert.equal(discovery.title, "Discovery");
  assert.match(discovery.primaryLabel, /7 queued/i);
  assert.match(discovery.failureSummary, /2 failed probes/i);
  assert.equal(sync.title, "Sync");
  assert.match(sync.primaryLabel, /Sync push/i);
  assert.match(sync.secondaryLabel, /active 10 \/ pending 3 \/ rejected 1/i);
});

test("task run view model derives terminal, stalled, and orphaned states", () => {
  assert.equal(buildTaskRunView({ type: "fetch", status: "ok", finishedAt: "2026-03-08T10:01:00.000Z" }, { nowMs: NOW }).status, "completed");
  assert.equal(buildTaskRunView({ type: "fetch", status: "warning", finishedAt: "2026-03-08T10:01:00.000Z" }, { nowMs: NOW }).status, "completed_with_warnings");
  assert.equal(buildTaskRunView({ type: "fetch", status: "error", finishedAt: "2026-03-08T10:01:00.000Z" }, { nowMs: NOW }).status, "failed");
  const stalled = buildTaskRunView({
    type: "fetch",
    active: true,
    startedAt: "2026-03-08T09:00:00.000Z",
    heartbeatAt: "2026-03-08T09:30:00.000Z"
  }, { nowMs: NOW });
  const orphaned = buildTaskRunView({
    type: "fetch",
    status: "started",
    startedAt: "2026-03-08T09:00:00.000Z"
  }, { nowMs: NOW });
  const running = buildTaskRunView({
    type: "fetch",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    heartbeatAt: "2026-03-08T10:09:30.000Z"
  }, { nowMs: NOW });
  assert.equal(stalled.status, "stalled");
  assert.equal(stalled.remediationHint, "Check bridge and task logs; verify whether the task heartbeat stopped.");
  assert.equal(orphaned.status, "orphaned");
  assert.equal(orphaned.remediationHint, "Refresh task state and check whether the owning process exited.");
  assert.equal(running.status, "running");
  assert.equal(running.remediationHint, "");
});

test("task run view model tolerates missing payloads", () => {
  const view = buildTaskRunView(null, { nowMs: NOW });
  assert.equal(view.taskType, "unknown");
  assert.equal(view.status, "waiting");
  assert.equal(view.title, "Task");
  assert.equal(view.progressLabel, "");
});
