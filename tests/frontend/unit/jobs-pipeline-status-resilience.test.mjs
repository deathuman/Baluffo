import test from "node:test";
import assert from "node:assert/strict";

import { createJobsPipelineController } from "../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { createJobsPipelineUiState } from "../../../frontend/jobs/app/runtime/state.js";

function createButtonMock() {
  const children = [];
  const button = {
    dataset: {},
    disabled: false,
    style: {
      setProperty(name, value) { this[name] = value; },
      removeProperty(name) { delete this[name]; }
    },
    classList: {
      toggle() {},
      contains() { return false; }
    },
    ownerDocument: {
      createElement(tagName) {
        return {
          tagName: String(tagName || "").toUpperCase(),
          dataset: {},
          style: { setProperty() {}, removeProperty() {} },
          textContent: ""
        };
      }
    },
    get textContent() {
      return children.length ? children.map(child => String(child?.textContent || "")).join("") : "";
    },
    set textContent(value) {
      children.length = 0;
      children.push({ textContent: String(value || "") });
    },
    replaceChildren(...nodes) {
      children.length = 0;
      children.push(...nodes);
    },
    querySelector(selector) {
      const match = /^\[data-ui="([^"]+)"\]$/.exec(String(selector || ""));
      return match ? children.find(child => String(child?.dataset?.ui || "") === match[1]) || null : null;
    },
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
  return button;
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

test("pollJobsPipelineStatus preserves recent active pipeline state across transient status timeout", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    let statusCalls = 0;

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          statusCalls += 1;
          if (statusCalls === 1) {
            return {
              active: true,
              runId: "pipeline_active_1",
              startedAt: "2026-06-06T09:00:00.000Z",
              stage: "fetch",
              progress: { label: "Fetching job listings" }
            };
          }
          throw new Error("Bridge request timed out");
        }
        if (path === "/ops/task-state?view=summary") {
          throw new Error("task details delayed");
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
    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.active, true);
    assert.equal(uiState.bridgeOnline, true);
    assert.equal(uiState.runId, "pipeline_active_1");
    assert.deepEqual(uiState.abortTask, { taskType: "pipeline", runId: "pipeline_active_1" });
    assert.equal(button.disabled, false);
    assert.match(String(button.textContent || ""), /^Fetching Job Listings\.\.\./i);
    assert.match(button.dataset.tooltip, /did not respond in time/i);
  } finally {
    restoreTimers();
  }
});

test("idle first-run tooltip refresh does not repaint button after pipeline becomes active", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const dashboard = {};
    dashboard.promise = new Promise(resolve => {
      dashboard.resolve = resolve;
    });

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") return { tasks: [] };
        if (path === "/ops/dashboard-health?view=summary") return dashboard.promise;
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
    uiState.active = true;
    uiState.runId = "pipeline_started_after_idle_poll";
    button.textContent = "Fetching Job Listings...";
    dashboard.resolve({ alerts: [{ id: "fetch_never_run" }] });
    await Promise.resolve();
    await Promise.resolve();

    assert.match(String(button.textContent || ""), /^Fetching Job Listings/);
  } finally {
    restoreTimers();
  }
});
