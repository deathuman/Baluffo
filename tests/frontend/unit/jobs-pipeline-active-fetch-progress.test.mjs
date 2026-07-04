import test from "node:test";
import assert from "node:assert/strict";

import {
  createButtonMock,
  createJobsPipelineController,
  createJobsPipelineUiState,
  installFakeTimers
} from "./helpers/jobs-pipeline-controller-helpers.mjs";

test("pollJobsPipelineStatus shows bounded active fetch progress while pipeline is active", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    uiState.active = true;
    uiState.runId = "pipeline_1";
    uiState.startedAt = "2026-03-12T12:00:00.000Z";

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            runId: "pipeline_1",
            stage: "fetch",
            startedAt: "2026-03-12T12:00:00.000Z"
          };
        }
        if (path === "/ops/task-state?view=summary") {
          return {
            tasks: [
              {
                taskType: "fetch",
                active: true,
                startedAt: "2026-03-12T12:00:10.000Z",
                summary: { outputCount: 48175, failedSources: 6 },
                taskProgress: {
                  active: true,
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  mode: "determinate",
                  ratio: 0.044,
                  counts: {
                    resolvedSources: 51,
                    sourceCount: 1154,
                    runningTasks: 6,
                    queuedTasks: 1097,
                    outputCount: 48175,
                    failedSources: 6,
                    completedSourcesPerMinute: 12,
                    estimatedRemainingMs: 3600000,
                    runningSourceNames: ["Studio A", "Studio B"],
                    runningSourceNamesTruncated: true
                  }
                }
              }
            ]
          };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: () => {},
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.active, true);
    assert.match(String(button.textContent || ""), /Fetching job listings/i);
    assert.match(String(button.textContent || ""), /51\/1,154 sources resolved/i);
    assert.match(String(button.textContent || ""), /rate 12\/min/i);
    assert.match(String(button.textContent || ""), /current Studio A, Studio B, \+more/i);
  } finally {
    restoreTimers();
  }
});

test("pollJobsPipelineStatus shows aggregate fetch tail ETA without extra routes", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    uiState.active = true;
    uiState.runId = "pipeline_aggregate";
    uiState.startedAt = "2026-03-12T12:00:00.000Z";
    const paths = [];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        paths.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            runId: "pipeline_aggregate",
            stage: "fetch",
            startedAt: "2026-03-12T12:00:00.000Z"
          };
        }
        if (path === "/ops/task-state?view=summary") {
          return {
            tasks: [
              {
                taskType: "fetch",
                active: true,
                startedAt: "2026-03-12T12:00:10.000Z",
                summary: { outputCount: 86151, failedSources: 308 },
                taskProgress: {
                  active: true,
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  mode: "determinate",
                  ratio: 333 / 334,
                  counts: {
                    etaBasis: "aggregate",
                    resolvedSources: 333,
                    sourceCount: 334,
                    runningTasks: 1,
                    queuedTasks: 0,
                    outputCount: 86151,
                    failedSources: 308,
                    completedSourcesPerMinute: 16,
                    estimatedRemainingMs: 1080000,
                    runningSourceNames: ["scrapy_static_sources"],
                    activeAggregateCompleted: 212,
                    activeAggregateTotal: 551,
                    activeAggregateRunning: 4,
                    activeAggregateQueued: 335,
                    activeAggregateError: 3,
                    activeAggregateRatePerMinute: 18,
                    activeAggregateEstimatedRemainingMs: 1080000
                  }
                }
              }
            ]
          };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: () => {},
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();

    assert.match(String(button.textContent || ""), /333\/334 sources resolved/i);
    assert.match(String(button.textContent || ""), /fallback 212\/551/i);
    assert.match(String(button.textContent || ""), /fallback rate 18\/min/i);
    assert.match(String(button.textContent || ""), /ETA 18m/i);
    assert.deepEqual(paths, [
      "/tasks/run-jobs-pipeline-status",
      "/ops/task-state?view=summary"
    ]);
  } finally {
    restoreTimers();
  }
});
