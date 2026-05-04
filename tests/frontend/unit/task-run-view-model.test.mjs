import test from "node:test";
import assert from "node:assert/strict";

import {
  buildTaskRunAnalysis,
  buildTaskRunDiagnostics,
  buildTaskRunView
} from "../../../frontend/shared/task-run-view-model.js";

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

test("task run diagnostics normalizes live rows into bounded support payloads", () => {
  const payload = buildTaskRunDiagnostics({
    taskType: "fetch",
    runId: "fetch_live_1",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    heartbeatAt: "2026-03-08T10:09:30.000Z",
    summary: {
      outputCount: 42,
      failedSources: 1,
      sourceCount: 12,
      recommendedApiPayload: { shouldNotCopy: true }
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
    },
    workItems: Array.from({ length: 8 }, (_row, index) => ({
      id: `source_${index}`,
      name: `Source ${index}`,
      status: index === 0 ? "running" : "pending",
      largeRawPayload: { hidden: true }
    })),
    recentEvents: Array.from({ length: 8 }, (_row, index) => ({
      level: "info",
      message: `Event ${index}`,
      rawLargeThing: { hidden: true }
    })),
    latestRun: { hidden: true },
    dedupEvidence: { hidden: true }
  }, {
    rowArea: "current",
    nowMs: NOW,
    generatedAt: "2026-03-08T10:10:00.000Z"
  });

  assert.equal(payload.kind, "admin_run_diagnostics");
  assert.equal(payload.version, 1);
  assert.equal(payload.rowArea, "current");
  assert.equal(payload.taskType, "fetch");
  assert.equal(payload.runId, "fetch_live_1");
  assert.equal(payload.status, "running");
  assert.equal(payload.progressMode, "determinate");
  assert.equal(payload.progressRatio, 0.5);
  assert.equal(payload.summaryCounts.outputCount, 42);
  assert.equal(payload.workItemExamples.length, 5);
  assert.equal(payload.eventExamples.length, 5);
  const serialized = JSON.stringify(payload);
  assert.doesNotMatch(serialized, /latestRun|dedupEvidence|recommendedApiPayload|largeRawPayload|rawLargeThing/i);
});

test("task run diagnostics covers completed failures and missing rows safely", () => {
  const failed = buildTaskRunDiagnostics({
    type: "sync",
    status: "error",
    finishedAt: "2026-03-08T10:01:00.000Z",
    durationMs: 1500,
    summary: {
      action: "push",
      activeCount: 7,
      pendingCount: 2,
      rejectedCount: 1,
      error: "remote rejected payload"
    }
  }, {
    rowArea: "completed",
    nowMs: NOW,
    generatedAt: "2026-03-08T10:10:00.000Z"
  });
  const missing = buildTaskRunDiagnostics(null, {
    nowMs: NOW,
    generatedAt: "2026-03-08T10:10:00.000Z"
  });

  assert.equal(failed.status, "failed");
  assert.equal(failed.severity, "critical");
  assert.equal(failed.timing.durationLabel, "1.5s");
  assert.match(failed.failureSummary, /remote rejected payload/i);
  assert.equal(failed.summaryCounts.action, "push");
  assert.equal(missing.taskType, "unknown");
  assert.equal(missing.status, "waiting");
  assert.deepEqual(missing.workItemExamples, []);
});

test("task run analysis normalizes selected run evidence with capped examples", () => {
  const analysis = buildTaskRunAnalysis({
    type: "fetch",
    runId: "fetch_selected_1",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    heartbeatAt: "2026-03-08T10:09:30.000Z",
    summary: {
      outputCount: 42,
      failedSources: 1,
      slowestSources: Array.from({ length: 8 }, (_row, index) => ({
        sourceId: `slow_${index}`,
        durationMs: 1000 + index,
        rawPayload: { hidden: true }
      }))
    },
    workItems: Array.from({ length: 8 }, (_row, index) => ({
      id: `source_${index}`,
      name: `Source ${index}`,
      status: index === 0 ? "running" : "pending",
      rawLargeThing: { hidden: true }
    })),
    recentEvents: Array.from({ length: 8 }, (_row, index) => ({
      level: "info",
      message: `Event ${index}`,
      rawLargeThing: { hidden: true }
    }))
  }, {
    rowArea: "current",
    nowMs: NOW
  });

  assert.equal(analysis.kind, "admin_selected_run_analysis");
  assert.equal(analysis.rowArea, "current");
  assert.equal(analysis.runId, "fetch_selected_1");
  assert.equal(analysis.status, "running");
  assert.equal(analysis.summaryCounts.outputCount, 42);
  assert.equal(analysis.slowExamples.length, 5);
  assert.equal(analysis.workItemExamples.length, 5);
  assert.equal(analysis.eventExamples.length, 5);
  assert.equal(analysis.timelineEntries.length, 5);
  assert.equal(analysis.timelineEntries[0].source, "event");
  assert.match(analysis.timelineEntries[0].label, /Event 0/);
  const serialized = JSON.stringify(analysis);
  assert.doesNotMatch(serialized, /rawPayload|rawLargeThing/i);
});

test("task run analysis timeline uses progress and work items when event evidence is sparse", () => {
  const analysis = buildTaskRunAnalysis({
    type: "discovery",
    runId: "discovery_selected_1",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    summary: { queuedCandidateCount: 4, failedProbeCount: 1 },
    taskProgress: {
      active: true,
      phaseKey: "probing_candidates",
      phaseLabel: "Probing candidates",
      mode: "determinate",
      ratio: 0.5,
      updatedAt: "2026-03-08T10:05:00.000Z"
    },
    workItems: [
      {
        id: "probe_pending",
        name: "Pending probe",
        status: "pending"
      },
      {
        id: "probe_failed",
        name: "Failed probe",
        status: "failed",
        error: "timeout while probing candidate",
        updatedAt: "2026-03-08T10:04:00.000Z"
      },
      {
        id: "probe_running",
        name: "Running probe",
        status: "running"
      }
    ],
    recentEvents: []
  }, {
    rowArea: "current",
    nowMs: NOW
  });

  assert.equal(analysis.timelineEntries.length, 3);
  assert.deepEqual(
    analysis.timelineEntries.map(entry => entry.source),
    ["work item", "progress", "work item"]
  );
  assert.equal(analysis.timelineEntries[0].severity, "critical");
  assert.match(analysis.timelineEntries[0].detail, /timeout/);
  assert.equal(analysis.timelineEntries[2].timestamp, "");
  assert.match(analysis.timelineEntries[2].label, /Running probe/);
});
