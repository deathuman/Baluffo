import test from "node:test";
import assert from "node:assert/strict";

import { JOBS_UPDATE_COPY } from "../../../frontend/jobs/app/pipeline.js";
import {
  createButtonMock,
  createJobsPipelineController,
  createJobsPipelineUiState,
  installFakeTimers
} from "./helpers/jobs-pipeline-controller-helpers.mjs";

test("triggerJobsPipelineRun uses long timeout and starts normally", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const calls = [];
    const toasts = [];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async (path, options = {}) => {
        calls.push({ path, options });
        if (path === "/tasks/run-jobs-pipeline") {
          return {
            started: true,
            runId: "pipeline_started",
            stage: "starting",
            startedAt: "2026-06-02T23:16:56.000Z"
          };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [{ id: 1 }, { id: 2 }],
      showToast: (message, kind) => {
        toasts.push({ message, kind });
      },
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.triggerJobsPipelineRun();

    assert.equal(calls[0].path, "/tasks/run-jobs-pipeline");
    assert.equal(calls[0].options.timeoutMs, 18000);
    assert.deepEqual(calls[0].options.allowStatuses, [409]);
    assert.equal(calls[0].options.body.jobsPageLoadedCount, 2);
    assert.equal(uiState.active, true);
    assert.equal(uiState.runId, "pipeline_started");
    assert.deepEqual(toasts, [{ message: JOBS_UPDATE_COPY.startedToast, kind: "success" }]);
  } finally {
    restoreTimers();
  }
});

test("triggerJobsPipelineRun attaches after start timeout when status is active", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const calls = [];
    const toasts = [];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async (path, options = {}) => {
        calls.push({ path, options });
        if (path === "/tasks/run-jobs-pipeline") {
          throw new Error("Bridge request timed out");
        }
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            runId: "pipeline_after_timeout",
            stage: "discovery",
            startedAt: "2026-06-02T23:16:56.000Z",
            progress: { label: "Running discovery..." }
          };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => {
        toasts.push({ message, kind });
      },
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.triggerJobsPipelineRun();

    assert.deepEqual(calls.map(call => call.path), [
      "/tasks/run-jobs-pipeline",
      "/tasks/run-jobs-pipeline-status"
    ]);
    assert.equal(calls[0].options.timeoutMs, 18000);
    assert.equal(calls[1].options.timeoutMs, 18000);
    assert.equal(uiState.active, true);
    assert.equal(uiState.pendingStart, false);
    assert.equal(uiState.runId, "pipeline_after_timeout");
    assert.equal(toasts.some(toast => toast.kind === "error"), false);
    assert.deepEqual(toasts, [{ message: "Job update is running.", kind: "info" }]);
  } finally {
    restoreTimers();
  }
});

test("triggerJobsPipelineRun attaches to already-running response without generic failure", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const toasts = [];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline") {
          return {
            started: false,
            error: "Jobs pipeline already running",
            runId: "pipeline_existing",
            stage: "discovery"
          };
        }
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            runId: "pipeline_existing",
            stage: "discovery",
            startedAt: "2026-06-02T23:16:56.000Z"
          };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => {
        toasts.push({ message, kind });
      },
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.triggerJobsPipelineRun();

    assert.equal(uiState.active, true);
    assert.equal(uiState.runId, "pipeline_existing");
    assert.deepEqual(toasts, [{ message: "Job update already running.", kind: "info" }]);
  } finally {
    restoreTimers();
  }
});

test("triggerJobsPipelineRun keeps failure path when verification finds no active pipeline", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const toasts = [];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline") {
          throw new Error("Bridge request timed out");
        }
        if (path === "/tasks/run-jobs-pipeline-status") {
          return { active: false, stage: "idle" };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => {
        toasts.push({ message, kind });
      },
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.triggerJobsPipelineRun();

    assert.equal(uiState.active, false);
    assert.equal(button.classList.contains("log-error"), true);
    assert.deepEqual(toasts, [{ message: JOBS_UPDATE_COPY.startFailed, kind: "error" }]);
  } finally {
    restoreTimers();
  }
});

test("triggerJobsPipelineRun maps active sync block to background-task copy", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const toasts = [];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline") {
          return {
            started: false,
            error: "Another sync task is already running",
            stage: "blocked"
          };
        }
        if (path === "/tasks/run-jobs-pipeline-status") {
          return { active: false, stage: "idle" };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => {
        toasts.push({ message, kind });
      },
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.triggerJobsPipelineRun();

    assert.deepEqual(toasts, [
      { message: "Another background task is still running.", kind: "error" }
    ]);
  } finally {
    restoreTimers();
  }
});
