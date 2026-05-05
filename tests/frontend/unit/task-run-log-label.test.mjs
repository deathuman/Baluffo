import test from "node:test";
import assert from "node:assert/strict";

import { buildTaskRunLogLabel } from "../../../frontend/shared/task-run-view-model.js";

const NOW = Date.parse("2026-03-08T10:10:00.000Z");

test("task run log label formats active fetch progress with browser fallback tail", () => {
  const log = buildTaskRunLogLabel({
    taskType: "fetch",
    active: true,
    startedAt: "2026-03-08T10:00:00.000Z",
    summary: { outputCount: 42, failedSources: 1, sourceCount: 12 },
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "determinate",
      ratio: 0.5,
      counts: { resolvedSources: 6, sourceCount: 12, outputCount: 42, failedSources: 1 }
    },
    workItems: [
      {
        id: "scrapy_static_sources",
        status: "running",
        progress: { counts: { completedSources: 19, totalSources: 26 } }
      }
    ]
  }, {
    taskType: "fetch",
    running: true,
    nowMs: NOW
  });

  assert.match(log.message, /^Fetcher:/);
  assert.match(log.message, /Executing sources/i);
  assert.match(log.message, /6\/12 sources resolved/i);
  assert.match(log.message, /Browser fallback 19\/26/i);
  assert.equal(log.levelHint, "warn");
});

test("task run log label formats active discovery progress", () => {
  const log = buildTaskRunLogLabel({
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
        generatedCandidates: 30,
        survivedDedupeCandidates: 18,
        probedCandidates: 3,
        probeTotal: 12,
        queuedCandidates: 7,
        failedProbes: 2
      }
    }
  }, {
    taskType: "discovery",
    running: true,
    nowMs: NOW,
    prefix: "Discovery active"
  });

  assert.match(log.message, /^Discovery active:/);
  assert.match(log.message, /Probing candidates/i);
  assert.match(log.message, /probed 3\/12/i);
  assert.match(log.message, /queued 7/i);
  assert.match(log.message, /failed 2/i);
  assert.equal(log.levelHint, "warn");
});

test("task run log label formats terminal fetch completion with failure warning", () => {
  const log = buildTaskRunLogLabel({
    taskType: "fetch",
    status: "ok",
    finishedAt: "2026-03-08T10:01:00.000Z",
    summary: { outputCount: 42, failedSources: 1, sourceCount: 12 },
    taskProgress: {
      active: false,
      phaseKey: "completed",
      phaseLabel: "Completed",
      mode: "determinate",
      ratio: 1,
      counts: { resolvedSources: 12, sourceCount: 12, outputCount: 42, failedSources: 1 }
    }
  }, {
    taskType: "fetch",
    running: false,
    nowMs: NOW,
    prefix: "Fetcher completed"
  });

  assert.match(log.message, /^Fetcher completed:/);
  assert.match(log.message, /Completed/i);
  assert.match(log.message, /12\/12 sources resolved/i);
  assert.match(log.message, /output 42/i);
  assert.equal(log.levelHint, "warn");
});

test("task run log label handles missing payloads", () => {
  const log = buildTaskRunLogLabel(null, { taskType: "fetch", nowMs: NOW });

  assert.equal(log.message, "Fetcher: no progress detail available.");
  assert.equal(log.levelHint, "info");
  assert.equal(log.view.status, "waiting");
});
