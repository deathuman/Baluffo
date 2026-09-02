import test from "node:test";
import assert from "node:assert/strict";

import { createJobsPipelineController } from "../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { createJobsPipelineUiState } from "../../../frontend/jobs/app/runtime/state.js";

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

function installFakeTimers() {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  globalThis.setTimeout = callback => ({ callback });
  globalThis.clearTimeout = () => {};
  return () => {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  };
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
