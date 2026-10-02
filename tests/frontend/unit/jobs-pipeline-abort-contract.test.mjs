import test from "node:test";
import assert from "node:assert/strict";
import { JOBS_UPDATE_COPY } from "../../../frontend/jobs/app/pipeline.js";
import {
  createButtonMock,
  createJobsPipelineController,
  createJobsPipelineUiState,
  installFakeTimers
} from "./helpers/jobs-pipeline-controller-helpers.mjs";

// Two contract cases for the Jobs abort control, split out of
// jobs-pipeline-abort-hover.test.mjs because that file is at its line budget.
//
// Both drive the same controller against a single active task; only the task type and
// the confirm answer differ, so the harness is shared.

const makeTask = (taskType, runId, phaseLabel) => ({
  taskType,
  runId,
  active: true,
  startedAt: "2026-03-12T12:00:00.000Z",
  taskProgress: { phaseLabel }
});

function abortControllerFor(taskType, runId, phaseLabel, abortRequests) {
  const button = createButtonMock();
  const uiState = createJobsPipelineUiState();
  const controller = createJobsPipelineController({
    refs: { jobsPipelineRunBtn: button },
    jobsPipelineUiState: uiState,
    callJobsBridge: async (path, options = {}) => {
      if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
      if (path === "/ops/task-state?view=summary") {
        return { tasks: [makeTask(taskType, runId, phaseLabel)] };
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
  return { button, uiState, controller };
}

test("declining the confirmation aborts nothing", async () => {
  // Every pre-existing case stubs confirm to `true`, so nothing proved that a user
  // saying "no" leaves the run alone. requestJobsPipelineAbort guards on the return
  // value in pipeline-controller.js; this pins that the guard is honoured.
  const restoreTimers = installFakeTimers();
  const originalConfirm = globalThis.confirm;
  const abortRequests = [];
  let confirmCalls = 0;
  globalThis.confirm = () => {
    confirmCalls += 1;
    return false;
  };
  try {
    const { button, uiState, controller } = abortControllerFor(
      "pipeline",
      "pipeline_live_1",
      "Running pipeline...",
      abortRequests
    );

    await controller.pollJobsPipelineStatus();
    await controller.triggerJobsPipelineRun();

    assert.equal(confirmCalls, 1, "abort must ask before posting");
    assert.deepEqual(abortRequests, [], "declining must not POST /tasks/abort");
    assert.notEqual(
      button.textContent,
      JOBS_UPDATE_COPY.abortingLabel,
      "must not show an aborting state"
    );
    assert.equal(uiState.abortRequested, false, "must not record abort intent locally");
  } finally {
    globalThis.confirm = originalConfirm;
    restoreTimers();
  }
});

test("a sync-only task is not offered abort", async () => {
  // The Jobs side of the abortable-type contract. isAbortableTask shares the backend
  // allow-list ({fetch, discovery, pipeline}) and the Admin view scopes abort inline
  // with the same three types, so sync genuinely cannot be aborted -- but only the
  // Admin side was pinned, so widening either allow-list would have gone unnoticed.
  const restoreTimers = installFakeTimers();
  const originalConfirm = globalThis.confirm;
  const abortRequests = [];
  globalThis.confirm = () => true;
  try {
    const { button, uiState, controller } = abortControllerFor(
      "sync",
      "sync_live_1",
      "Updating local jobs",
      abortRequests
    );

    await controller.pollJobsPipelineStatus();

    assert.equal(button.dataset.abortable, "false", "sync rows must not be abortable");
    assert.equal(uiState.abortTask, null, "sync must not become an abort target");

    await controller.triggerJobsPipelineRun();
    assert.deepEqual(abortRequests, [], "sync is rejected by the route; do not even try");
  } finally {
    globalThis.confirm = originalConfirm;
    restoreTimers();
  }
});
