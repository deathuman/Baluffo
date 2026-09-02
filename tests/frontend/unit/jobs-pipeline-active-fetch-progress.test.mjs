import test from "node:test";
import assert from "node:assert/strict";

import {
  createButtonMock,
  createJobsPipelineController,
  createJobsPipelineUiState,
  getJobsPipelineProgressCaption,
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
    const caption = getJobsPipelineProgressCaption(button);
    assert.ok(caption, "sub-progress caption should be created");
    assert.equal(caption.hidden, false);
    assert.equal(caption.classList.contains("running"), true);
    assert.match(String(button.textContent || ""), /Fetching job listings/i);
    // ponytail: the compact caption keeps the phase + high-signal counts/ETA and
    // drops the verbose running-source detail, so the caption reads cleanly.
    assert.match(String(caption.textContent || ""), /51\/1,154 sources resolved/i);
    assert.match(String(caption.textContent || ""), /rate 12\/min/i);
    assert.match(String(caption.textContent || ""), /ETA 1h/i);
    assert.doesNotMatch(String(caption.textContent || ""), /current Studio A, Studio B/);
  } finally {
    restoreTimers();
  }
});

test("pollJobsPipelineStatus uses the live active child from the pipeline status payload for sub-progress", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    uiState.active = true;
    uiState.runId = "pipeline_livechild";
    uiState.startedAt = "2026-03-12T12:00:00.000Z";

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            runId: "pipeline_livechild",
            stage: "fetch",
            startedAt: "2026-03-12T12:00:00.000Z",
            activeChildren: [
              {
                runId: "fetch_live",
                taskType: "fetch",
                type: "fetch",
                active: true,
                startedAt: "2026-03-12T12:00:05.000Z",
                taskProgress: {
                  active: true,
                  phaseKey: "executing_sources",
                  phaseLabel: "Executing sources",
                  mode: "determinate",
                  ratio: 0.25,
                  counts: {
                    resolvedSources: 100,
                    sourceCount: 400,
                    runningTasks: 8,
                    queuedTasks: 300,
                    outputCount: 500,
                    failedSources: 2,
                    completedSourcesPerMinute: 10,
                    estimatedRemainingMs: 900000
                  }
                }
              }
            ]
          };
        }
        if (path === "/ops/task-state?view=summary") {
          return { tasks: [] };
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
    // live active child drives the fill (child determinate ratio) even with no
    // task-state blocking row present.
    assert.equal(button.dataset.progressMode, "determinate");
    assert.equal(button.dataset.progressFill, "25");
    const caption = getJobsPipelineProgressCaption(button);
    assert.ok(caption, "sub-progress caption should be created");
    assert.match(String(button.textContent || ""), /Fetching job listings/i);
    assert.match(String(caption.textContent || ""), /Executing sources/i);
    assert.match(String(caption.textContent || ""), /100\/400 sources resolved/i);
    assert.match(String(caption.textContent || ""), /ETA 15m/i);
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

    const aggregateCaption = getJobsPipelineProgressCaption(button);
    assert.ok(aggregateCaption, "sub-progress caption should be created");
    // ponytail: aggregate detail is compacted to the resolved count + fallback
    // rate + ETA; the per-phase aggregate completed/total detail is not surfaced
    // in the button caption.
    assert.match(String(aggregateCaption.textContent || ""), /333\/334 sources resolved/i);
    assert.match(String(aggregateCaption.textContent || ""), /fallback rate 18\/min/i);
    assert.match(String(aggregateCaption.textContent || ""), /ETA 18m/i);
    assert.doesNotMatch(String(aggregateCaption.textContent || ""), /fallback 212\/551/i);
    assert.deepEqual(paths, [
      "/tasks/run-jobs-pipeline-status",
      "/ops/task-state?view=summary"
    ]);
  } finally {
    restoreTimers();
  }
});
