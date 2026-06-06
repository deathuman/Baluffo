import test from "node:test";
import assert from "node:assert/strict";
import { JOBS_UPDATE_COPY } from "../../../frontend/jobs/app/pipeline.js";
import {
  createButtonMock,
  createJobsPipelineController,
  createJobsPipelineUiState,
  installFakeTimers
} from "./helpers/jobs-pipeline-controller-helpers.mjs";
test("active discovery task exposes Abort update hover label data and aborts on click", async () => {
  const restoreTimers = installFakeTimers();
  const originalConfirm = globalThis.confirm;
  globalThis.confirm = () => true;
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const abortRequests = [];
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async (path, options = {}) => {
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") {
          return {
            tasks: [
              {
                taskType: "discovery",
                runId: "discovery_live_1",
                active: true,
                startedAt: "2026-03-12T12:00:00.000Z",
                taskProgress: { phaseLabel: "Running discovery..." }
              }
            ]
          };
        }
        if (path === "/tasks/abort") {
          abortRequests.push(options.body);
          return { ok: true, abortAccepted: true };
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
    assert.equal(uiState.active, false);
    assert.equal(button.disabled, false);
    assert.equal(button.dataset.abortable, "true");
    assert.equal(button.dataset.abortLabel, JOBS_UPDATE_COPY.abortLabel);
    assert.match(String(button.textContent || ""), /^Checking sources\.\.\./);
    button.dispatch("pointerenter", { pointerType: "mouse" });
    assert.match(String(button.textContent || ""), /^Checking sources\.\.\./);
    assert.equal(button.classList.contains("abort-reveal"), false);
    await controller.triggerJobsPipelineRun();
    assert.deepEqual(abortRequests, [
      {
        taskType: "discovery",
        runId: "discovery_live_1",
        reason: "jobs_page_abort_update"
      }
    ]);
    assert.equal(button.textContent, JOBS_UPDATE_COPY.abortingLabel);
  } finally {
    globalThis.confirm = originalConfirm;
    restoreTimers();
  }
});

test("active discovery task aborts from the main button even when hover reveal did not fire", async () => {
  const restoreTimers = installFakeTimers();
  const originalConfirm = globalThis.confirm;
  globalThis.confirm = () => true;
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const abortRequests = [];
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async (path, options = {}) => {
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") {
          return {
            tasks: [
              {
                taskType: "discovery",
                runId: "discovery_live_1",
                active: true,
                startedAt: "2026-03-12T12:00:00.000Z",
                taskProgress: { phaseLabel: "Running discovery..." }
              }
            ]
          };
        }
        if (path === "/tasks/abort") {
          abortRequests.push(options.body);
          return { ok: true, abortAccepted: true };
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
    assert.equal(button.dataset.abortable, "true");
    assert.equal(button.dataset.abortLabel, JOBS_UPDATE_COPY.abortLabel);
    await controller.triggerJobsPipelineRun();
    assert.deepEqual(abortRequests, [
      {
        taskType: "discovery",
        runId: "discovery_live_1",
        reason: "jobs_page_abort_update"
      }
    ]);
  } finally {
    globalThis.confirm = originalConfirm;
    restoreTimers();
  }
});

test("pipeline abort completion clears Aborting state and suppresses failure toast", async () => {
  const restoreTimers = installFakeTimers();
  const originalConfirm = globalThis.confirm;
  globalThis.confirm = () => true;
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const toasts = [];
    const abortRequests = [];
    const statuses = [
      {
        active: true,
        runId: "pipeline_1",
        stage: "discovery",
        startedAt: "2026-03-12T12:00:00.000Z"
      },
      {
        active: false,
        runId: "pipeline_1",
        stage: "canceled",
        terminalReason: "user_abort_requested",
        error: ""
      }
    ];
    const taskStates = [
      { tasks: [{ taskType: "pipeline", runId: "pipeline_1", active: true }] },
      { tasks: [] }
    ];
    let statusIndex = 0;
    let taskStateIndex = 0;

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async (path, options = {}) => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return statuses[Math.min(statusIndex++, statuses.length - 1)];
        }
        if (path === "/ops/task-state?view=summary") {
          return taskStates[Math.min(taskStateIndex++, taskStates.length - 1)];
        }
        if (path === "/tasks/abort") {
          abortRequests.push(options.body);
          return { ok: true, abortAccepted: true };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, type) => toasts.push({ message, type }),
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error) || String(payload?.stage || "") === "error",
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();
    await controller.triggerJobsPipelineRun();

    assert.equal(uiState.abortRequested, true);
    assert.equal(button.textContent, JOBS_UPDATE_COPY.abortingLabel);

    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.abortRequested, false);
    assert.equal(uiState.abortTask, null);
    assert.equal(button.textContent, JOBS_UPDATE_COPY.idleLabel);
    assert.deepEqual(abortRequests, [
      {
        taskType: "pipeline",
        runId: "pipeline_1",
        reason: "jobs_page_abort_update"
      }
    ]);
    assert.equal(toasts.some(toast => String(toast.message).includes("Job update failed")), false);
  } finally {
    globalThis.confirm = originalConfirm;
    restoreTimers();
  }
});

test("wrapped pipeline abort error is treated as a canceled user abort", async () => {
  const restoreTimers = installFakeTimers();
  const originalConfirm = globalThis.confirm;
  globalThis.confirm = () => true;
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const toasts = [];
    const statuses = [
      {
        active: true,
        runId: "pipeline_1",
        stage: "discovery",
        startedAt: "2026-03-12T12:00:00.000Z"
      },
      {
        active: false,
        runId: "pipeline_1",
        stage: "error",
        error: "discovery_wait: pipeline abort requested"
      }
    ];
    let statusIndex = 0;
    let taskStateIndex = 0;

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async (path) => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return statuses[Math.min(statusIndex++, statuses.length - 1)];
        }
        if (path === "/ops/task-state?view=summary") {
          taskStateIndex += 1;
          return taskStateIndex === 1
            ? { tasks: [{ taskType: "pipeline", runId: "pipeline_1", active: true }] }
            : { tasks: [] };
        }
        if (path === "/tasks/abort") return { ok: true, abortAccepted: true };
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, type) => toasts.push({ message, type }),
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error) || String(payload?.stage || "") === "error",
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();
    await controller.triggerJobsPipelineRun();
    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.abortRequested, false);
    assert.equal(button.textContent, JOBS_UPDATE_COPY.idleLabel);
    assert.equal(toasts.some(toast => String(toast.message).includes("Job update failed")), false);
  } finally {
    globalThis.confirm = originalConfirm;
    restoreTimers();
  }
});

test("pending abort blocks a new pipeline start even if abort task was cleared", async () => {
  const button = createButtonMock();
  const uiState = createJobsPipelineUiState();
  uiState.abortRequested = true;
  uiState.abortRequestedTask = { taskType: "fetch", runId: "fetch_1" };
  const calls = [];

  const controller = createJobsPipelineController({
    refs: { jobsPipelineRunBtn: button },
    jobsPipelineUiState: uiState,
    callJobsBridge: async (path) => {
      calls.push(path);
      return {};
    },
    getAllJobs: () => [],
    showToast: () => {},
    setRefreshJobsNeedsAttention: () => {},
    isErrorStage: payload => Boolean(payload?.error),
    pollDelayMs: 25,
    idlePollDelayMs: 50
  });

  await controller.triggerJobsPipelineRun();

  assert.deepEqual(calls, []);
});

test("failed abort request only shows an error after the target remains active", async () => {
  const restoreTimers = installFakeTimers();
  const originalConfirm = globalThis.confirm;
  globalThis.confirm = () => true;
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const toasts = [];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async (path) => {
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") {
          return {
            tasks: [
              {
                taskType: "discovery",
                runId: "discovery_live_1",
                active: true,
                startedAt: "2026-03-12T12:00:00.000Z"
              }
            ]
          };
        }
        if (path === "/tasks/abort") throw new Error("Bridge request timed out");
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, type) => toasts.push({ message, type }),
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();
    await controller.triggerJobsPipelineRun();

    assert.equal(toasts.some(toast => toast.type === "error"), false);
    uiState.abortRequestErrorAt = Date.now() - 6000;

    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.abortRequested, false);
    assert.equal(
      toasts.some(toast => String(toast.message).includes("Could not abort job update")),
      true
    );
  } finally {
    globalThis.confirm = originalConfirm;
    restoreTimers();
  }
});

test("abort hover data preserves the latest live progress label", () => {
  const button = createButtonMock();
  const uiState = createJobsPipelineUiState();
  const controller = createJobsPipelineController({
    refs: { jobsPipelineRunBtn: button },
    jobsPipelineUiState: uiState,
    callJobsBridge: async () => ({}),
    getAllJobs: () => [],
    showToast: () => {},
    setRefreshJobsNeedsAttention: () => {},
    isErrorStage: payload => Boolean(payload?.error),
    pollDelayMs: 25,
    idlePollDelayMs: 50
  });
  const abortTask = { taskType: "discovery", runId: "discovery_live_1", active: true };

  controller.updateJobsPipelineUi({
    running: true,
    disabled: true,
    buttonLabel: "Checking sources... 26s",
    pipelinePayload: { active: true, stage: "discovery" },
    abortTask
  });
  button.dispatch("pointerenter", { pointerType: "mouse" });

  assert.equal(button.textContent, "Checking sources... 26s");
  assert.equal(button.dataset.abortLabel, JOBS_UPDATE_COPY.abortLabel);

  controller.updateJobsPipelineUi({
    running: true,
    disabled: true,
    buttonLabel: "Checking sources... 45s",
    pipelinePayload: { active: true, stage: "discovery" },
    abortTask
  });

  assert.equal(button.textContent, "Checking sources... 45s");
  assert.equal(button.dataset.abortable, "true");
  assert.equal(button.dataset.abortLabel, JOBS_UPDATE_COPY.abortLabel);
  assert.equal(button.classList.contains("abort-reveal"), false);
});
