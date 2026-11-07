import test from "node:test";
import assert from "node:assert/strict";

import { createJobsPipelineController } from "../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { JOBS_UPDATE_COPY } from "../../../frontend/jobs/app/pipeline.js";
import { createJobsPipelineUiState } from "../../../frontend/jobs/app/runtime/state.js";
import { installFakeTimers } from "./helpers/jobs-pipeline-controller-helpers.mjs";

function styleMock() {
  return { setProperty() {}, removeProperty() {} };
}

function elementMock(tagName = "span") {
  return {
    tagName: tagName.toUpperCase(),
    dataset: {},
    style: styleMock(),
    textContent: "",
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
}

function buttonMock() {
  return {
    dataset: {},
    style: styleMock(),
    disabled: false,
    children: [],
    classList: { toggle() {}, contains() { return false; } },
    ownerDocument: { createElement: elementMock },
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    },
    get textContent() {
      return this.children.map(child => String(child?.textContent || "")).join("");
    },
    set textContent(value) {
      this.children = [{ textContent: String(value || "") }];
    },
    replaceChildren(...nodes) {
      this.children = nodes;
    },
    querySelector(selector) {
      const match = /^\[data-ui="([^"]+)"\]$/.exec(String(selector || ""));
      return match
        ? this.children.find(child => String(child?.dataset?.ui || "") === match[1]) || null
        : null;
    }
  };
}

async function runPipelineCompletion(payload) {
  const restoreTimers = installFakeTimers();
  try {
    const button = buttonMock();
    const uiState = createJobsPipelineUiState();
    Object.assign(uiState, { active: true, runId: "pipeline_1", startedAt: "2026-03-12T12:00:00Z" });
    const toasts = [];
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") return payload;
        if (path === "/ops/task-state?view=summary") return { tasks: [] };
        if (path === "/ops/dashboard-health?view=summary") return { alerts: [] };
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => toasts.push({ message, kind }),
      isErrorStage: candidate => Boolean(candidate?.error)
        || String(candidate?.stage || "") === "error"
    });

    await controller.pollJobsPipelineStatus();
    return { toasts, uiState, button };
  } finally {
    restoreTimers();
  }
}

test("pipeline sync warning completes the update with non-blocking Jobs copy", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = buttonMock();
    const uiState = createJobsPipelineUiState();
    Object.assign(uiState, { active: true, runId: "pipeline_1", startedAt: "2026-03-12T12:00:00Z" });
    const toasts = [];
    const refreshCalls = [];
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: false,
            runId: "pipeline_1",
            stage: "completed_with_warnings",
            completedWithWarnings: true,
            syncWarning: { kind: "recoverable_remote_conflict" },
            updatesFound: true,
            refreshRecommended: true
          };
        }
        if (path === "/ops/task-state") return { tasks: [] };
        if (path === "/ops/dashboard-health?view=summary") return { alerts: [] };
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => toasts.push({ message, kind }),
      refreshJobsAfterPipelineCompletion: async payload => {
        refreshCalls.push(payload);
      },
      isErrorStage: payload => Boolean(payload?.error) || String(payload?.stage || "") === "error"
    });

    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.active, false);
    assert.equal(button.disabled, false);
    assert.deepEqual(toasts, [{
      message: "Job update completed. Loading updated listings. Source sync needs attention.",
      kind: "warn"
    }]);
    assert.equal(refreshCalls.length, 1);
  } finally {
    restoreTimers();
  }
});

test("pipeline fetch warning includes its failed-source count without claiming source sync", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = buttonMock();
    const uiState = createJobsPipelineUiState();
    Object.assign(uiState, { active: true, runId: "pipeline_1", startedAt: "2026-03-12T12:00:00Z" });
    const toasts = [];
    const refreshCalls = [];
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: false,
            runId: "pipeline_1",
            stage: "completed_with_warnings",
            completedWithWarnings: true,
            syncWarning: {},
            warnings: [
              {
                kind: "fetch_sources_failed",
                stage: "fetch",
                failedSources: 3,
                message: "3 of 100 sources failed during fetch."
              }
            ],
            updatesFound: true,
            refreshRecommended: true
          };
        }
        if (path === "/ops/task-state") return { tasks: [] };
        if (path === "/ops/dashboard-health?view=summary") return { alerts: [] };
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => toasts.push({ message, kind }),
      refreshJobsAfterPipelineCompletion: async payload => {
        refreshCalls.push(payload);
      },
      isErrorStage: payload => Boolean(payload?.error) || String(payload?.stage || "") === "error"
    });

    await controller.pollJobsPipelineStatus();

    assert.deepEqual(toasts, [{
      message: `${JOBS_UPDATE_COPY.completedWithWarnings} 3 failed sources.`,
      kind: "warn"
    }]);
    assert.equal(refreshCalls.length, 1);
  } finally {
    restoreTimers();
  }
});

test("pipeline fetch warning uses the singular noun for one failed source", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = buttonMock();
    const uiState = createJobsPipelineUiState();
    Object.assign(uiState, { active: true, runId: "pipeline_1", startedAt: "2026-03-12T12:00:00Z" });
    const toasts = [];
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: false,
            runId: "pipeline_1",
            stage: "completed_with_warnings",
            completedWithWarnings: true,
            syncWarning: {},
            warnings: [
              {
                kind: "fetch_sources_failed",
                stage: "fetch",
                failedSources: 1,
                message: "1 of 100 sources failed during fetch."
              }
            ],
            updatesFound: true,
            refreshRecommended: true
          };
        }
        if (path === "/ops/task-state") return { tasks: [] };
        if (path === "/ops/dashboard-health?view=summary") return { alerts: [] };
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => toasts.push({ message, kind }),
      isErrorStage: payload => Boolean(payload?.error) || String(payload?.stage || "") === "error"
    });

    await controller.pollJobsPipelineStatus();

    assert.deepEqual(toasts, [{
      message: `${JOBS_UPDATE_COPY.completedWithWarnings} 1 failed source.`,
      kind: "warn"
    }]);
  } finally {
    restoreTimers();
  }
});

test("pipeline error completion never shows the completed-with-warnings toast", async () => {
  const { toasts } = await runPipelineCompletion({
    active: false,
    runId: "pipeline_1",
    stage: "error",
    error: "fetch report failed",
    completedWithWarnings: true,
    syncWarning: { kind: "recoverable_remote_conflict" },
    warnings: [
      {
        kind: "fetch_sources_failed",
        stage: "fetch",
        failedSources: 3,
        message: "3 of 100 sources failed during fetch."
      }
    ],
    updatesFound: true,
    refreshRecommended: true
  });

  assert.deepEqual(toasts, [{ message: "Job update failed: fetch report failed", kind: "error" }]);
  assert.equal(
    toasts.some(toast => String(toast.message).includes(JOBS_UPDATE_COPY.completedWithWarnings)),
    false
  );
});

test("pipeline canceled completion never shows the completed-with-warnings toast", async () => {
  const { toasts } = await runPipelineCompletion({
    active: false,
    runId: "pipeline_1",
    stage: "canceled",
    terminalReason: "user_abort_requested",
    completedWithWarnings: true,
    syncWarning: { kind: "recoverable_remote_conflict" },
    warnings: [
      {
        kind: "fetch_sources_failed",
        stage: "fetch",
        failedSources: 3,
        message: "3 of 100 sources failed during fetch."
      }
    ],
    updatesFound: true,
    refreshRecommended: true
  });

  assert.deepEqual(toasts, []);
});

test("pipeline completion-shaped error and cancellation payloads suppress the warning toast", async () => {
  const warningFields = {
    active: false,
    runId: "pipeline_1",
    stage: "completed_with_warnings",
    completedWithWarnings: true,
    syncWarning: { kind: "recoverable_remote_conflict" },
    warnings: [
      {
        kind: "fetch_sources_failed",
        stage: "fetch",
        failedSources: 3,
        message: "3 of 100 sources failed during fetch."
      }
    ],
    updatesFound: true,
    refreshRecommended: true
  };

  const errored = await runPipelineCompletion({
    ...warningFields,
    error: "fetch report failed"
  });
  assert.equal(errored.uiState.active, false);
  assert.deepEqual(errored.toasts, [
    { message: "Job update failed: fetch report failed", kind: "error" }
  ]);

  const canceled = await runPipelineCompletion({
    ...warningFields,
    terminalReason: "user_abort_requested"
  });
  assert.equal(canceled.uiState.active, false);
  assert.deepEqual(canceled.toasts, []);
});
